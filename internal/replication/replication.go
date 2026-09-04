// Package replication follows Synapse's replication stream over Redis.
//
// It is SUBSCRIBE-only, and that is a safety property rather than a
// simplification. A real federation sender publishes two things, and both would
// perturb the live cluster we are shadowing:
//
//   - REPLICATE on connect, which makes every other worker on the bus broadcast
//     its stream positions on our account.
//   - FEDERATION_ACK after each transaction, which tells the main process it may
//     clear its in-memory queues up to min() across sender instances. An ack
//     from a worker that has not actually delivered anything could drop EDUs a
//     real sender had not yet taken.
//
// So this connection cannot publish: newSubscriber takes a client whose only
// use is Subscribe, and there is no code path here that calls Publish. See
// docs/shadow-safety.md.
//
// The consequence we accept is that we never receive the POSITION broadcast a
// REPLICATE would have triggered. Positions are seeded from the database at
// startup instead and corrected as RDATA arrives; a seeded position is a lower
// bound, which for a shadow means at worst re-examining events we already
// examined.
package replication

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Stream names carried on the replication channel. A federation sender consumes
// the first four; the rest are here so an unrecognised stream can be reported
// as "not ours" rather than "unknown".
const (
	// StreamEvents is a poke only. The row says an event exists; the sender
	// re-reads the events table for the content
	// (replication/tcp/client.py:190 -> notifier -> notify_new_events).
	StreamEvents = "events"
	// StreamToDevice rows are [entity], where entity is a user id or a remote
	// server name. Only the server names concern a sender.
	StreamToDevice = "to_device"
	// StreamDeviceLists rows are [user_id, is_signature, hosts_calculated].
	// The destinations are not in the row; they come from
	// device_lists_outbound_pokes for that stream id.
	StreamDeviceLists = "device_lists"
	// StreamFederation carries EDUs relayed from the main process. Out of
	// scope for phase 1, but recognised so its arrival is not a surprise.
	StreamFederation = "federation"

	StreamReceipts           = "receipts"
	StreamTyping             = "typing"
	StreamPresence           = "presence"
	StreamPresenceFederation = "presence_federation"
	StreamCaches             = "caches"
)

// Command names on the wire (replication/tcp/commands.py).
const (
	cmdRDATA          = "RDATA"
	cmdPOSITION       = "POSITION"
	cmdRemoteServerUp = "REMOTE_SERVER_UP"
)

// batchToken is the literal token Synapse sends for a row that is part of a
// batch. The next numeric token flushes the buffer plus itself as one batch
// (commands.py:114).
const batchToken = "batch"

// Row is one replication row, already split into its parts.
type Row struct {
	Stream string
	// Instance is the worker that produced the row.
	Instance string
	Position int64
	// JSON is the raw positional array, parsed by the handler that knows the
	// stream's shape.
	JSON string
}

// Handler receives replication events.
//
// Rows arrive in order, batched: OnRows is called once per batch with every row
// that shares a position, which is how Synapse groups them and what lets a
// handler treat a burst of device pokes as one unit of work.
type Handler interface {
	// OnRows is called for each batch of rows on a stream.
	OnRows(stream string, position int64, rows []Row)
	// OnPosition is called when a stream jumps to a new position without rows,
	// which happens when the writer skipped ahead over things we do not care
	// about.
	OnPosition(stream, instance string, position int64)
	// OnRemoteServerUp says a destination that was backing off has been seen
	// to work by somebody else. A real sender wakes that destination
	// immediately; a shadow records the same intent.
	OnRemoteServerUp(server string)
}

// Config describes how to reach Redis.
type Config struct {
	Enabled bool
	// Address is a unix socket path, or host:port.
	Address string
	// Channel MUST equal Synapse's server_name: the channel is named after it
	// and there is no prefix setting (replication/tcp/redis.py:399).
	// Subscribing to the wrong channel raises no error and delivers nothing, so
	// the worker would simply never wake up and would look merely idle.
	Channel  string
	Password string
	DB       int
	// InstanceName is ours, used to drop our own echo. We never publish, so
	// nothing should ever match -- a row that does means somebody else is
	// running under our name, which is worth saying out loud.
	InstanceName string
}

// Subscriber follows the replication channel.
type Subscriber struct {
	cfg     Config
	log     zerolog.Logger
	handler Handler

	mu sync.RWMutex
	// live is false whenever the subscription is not known to be healthy.
	// Positions are only trustworthy while it is true.
	live      bool
	positions map[string]int64

	// pending buffers the rows of a batch until the row carrying the batch's
	// position arrives. Keyed by stream, because two streams can be mid-batch
	// at once.
	pending map[string][]Row

	onConnect func()
	onDrop    func()
}

// New builds a Subscriber.
func New(cfg Config, log zerolog.Logger, handler Handler) *Subscriber {
	return &Subscriber{
		cfg:       cfg,
		log:       log,
		handler:   handler,
		positions: map[string]int64{},
		pending:   map[string][]Row{},
	}
}

// SetOnConnect and SetOnDrop register callbacks for the edges of the
// subscription's health, so the caller can seed or invalidate without this
// package knowing about the store.
func (s *Subscriber) SetOnConnect(f func()) { s.onConnect = f }

// SetOnDrop registers a callback for the moment the subscription is lost.
func (s *Subscriber) SetOnDrop(f func()) { s.onDrop = f }

// Seed sets a starting position for a stream, from the database.
//
// A seeded position is a lower bound; the first RDATA for that stream replaces
// it with the truth. This is how we get a starting point without publishing
// REPLICATE.
func (s *Subscriber) Seed(positions map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for stream, pos := range positions {
		if pos > s.positions[stream] {
			s.positions[stream] = pos
		}
	}
}

// Position returns the last seen position of a stream.
func (s *Subscriber) Position(stream string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[stream]
}

// Positions returns a copy of every known stream position.
func (s *Subscriber) Positions() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.positions))
	for k, v := range s.positions {
		out[k] = v
	}
	return out
}

// Live reports whether the subscription is currently healthy.
func (s *Subscriber) Live() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live
}

// Run follows the channel until the context is cancelled, reconnecting with
// backoff.
func (s *Subscriber) Run(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info().Msg("replication disabled; the worker will not see new events")
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		subscribed, err := s.session(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.Warn().Err(err).Dur("retry_in", backoff).Msg("replication connection lost")
		}
		s.setLive(false)
		// A session that got as far as subscribing was a working connection, so
		// the next failure starts its backoff from the bottom again. Without
		// this the delay only ever grows, and a worker that has been up long
		// enough to see a handful of unrelated blips waits the full thirty
		// seconds before every subsequent reconnect.
		if subscribed {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Subscriber) session(ctx context.Context) (subscribed bool, err error) {
	opts := &redis.Options{
		Addr:     s.cfg.Address,
		Password: s.cfg.Password,
		DB:       s.cfg.DB,
	}
	// A path rather than host:port means a unix socket, which is how this
	// deployment reaches KeyDB.
	if strings.HasPrefix(s.cfg.Address, "/") {
		opts.Network = "unix"
	}
	client := redis.NewClient(opts)
	defer client.Close()

	// The only call made on this client. Nothing in this package publishes;
	// see the package comment.
	sub := client.Subscribe(ctx, s.cfg.Channel)
	defer sub.Close()

	if _, err := sub.Receive(ctx); err != nil {
		return false, err
	}
	subscribed = true
	s.setLive(true)
	s.log.Info().
		Str("channel", s.cfg.Channel).
		Str("address", s.cfg.Address).
		Msg("subscribed to the replication channel (subscribe-only; we never publish)")
	if s.onConnect != nil {
		s.onConnect()
	}

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return subscribed, ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return subscribed, nil
			}
			s.handleLine(msg.Payload)
		}
	}
}

func (s *Subscriber) setLive(live bool) {
	s.mu.Lock()
	was := s.live
	s.live = live
	if !live {
		// A dropped subscription means the buffered half of a batch will never
		// be completed by the row that names its position. Keeping it would
		// merge it into the next connection's first batch.
		s.pending = map[string][]Row{}
	}
	s.mu.Unlock()

	if was && !live && s.onDrop != nil {
		s.onDrop()
	}
}

// handleLine parses one published message.
//
// The format is "NAME rest-of-line", one line, no newlines
// (replication/tcp/redis.py:263).
func (s *Subscriber) handleLine(line string) {
	name, rest, ok := strings.Cut(line, " ")
	if !ok {
		// Commands with no arguments exist (REPLICATE); none of them concern us.
		return
	}
	switch name {
	case cmdRDATA:
		s.handleRDATA(rest)
	case cmdPOSITION:
		s.handlePOSITION(rest)
	case cmdRemoteServerUp:
		if s.handler != nil {
			s.handler.OnRemoteServerUp(strings.TrimSpace(rest))
		}
	}
}

// handleRDATA parses "RDATA <stream> <instance> <token|batch> <row_json>".
func (s *Subscriber) handleRDATA(rest string) {
	stream, rest, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}
	instance, rest, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}
	token, rowJSON, ok := strings.Cut(rest, " ")
	if !ok {
		return
	}

	// Synapse suppresses its own echo by instance name; so do we. We never
	// publish, so a match means another worker is running under our name.
	if instance == s.cfg.InstanceName {
		s.log.Warn().
			Str("instance", instance).
			Msg("saw a replication row from our own instance name; another worker is using it")
		return
	}

	row := Row{Stream: stream, Instance: instance, JSON: rowJSON}

	if token == batchToken {
		s.mu.Lock()
		s.pending[stream] = append(s.pending[stream], row)
		s.mu.Unlock()
		return
	}

	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		s.log.Warn().Str("stream", stream).Str("token", token).Msg("unparseable RDATA token")
		return
	}
	row.Position = pos

	// The numeric token flushes the buffered rows plus itself as one batch.
	s.mu.Lock()
	batch := append(s.pending[stream], row)
	delete(s.pending, stream)
	for i := range batch {
		batch[i].Position = pos
	}
	if pos > s.positions[stream] {
		s.positions[stream] = pos
	}
	s.mu.Unlock()

	if s.handler != nil {
		s.handler.OnRows(stream, pos, batch)
	}
}

// handlePOSITION parses "POSITION <stream> <instance> <prev_token> <new_token>".
//
// Synapse ignores RDATA for a stream until a POSITION for it has arrived on the
// same connection. We deliberately do not: we never send REPLICATE, so no
// POSITION is broadcast on our account, and waiting for one would mean waiting
// for an unrelated worker to trigger it. Our positions are seeded from the
// database instead, so acting on RDATA immediately is safe -- the seed is a
// lower bound and the row is newer than it.
func (s *Subscriber) handlePOSITION(rest string) {
	fields := strings.Fields(rest)
	if len(fields) < 4 {
		return
	}
	stream, instance := fields[0], fields[1]
	pos, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return
	}
	if instance == s.cfg.InstanceName {
		return
	}

	s.mu.Lock()
	// A POSITION can jump the stream forward over rows nobody sent us, which
	// is exactly what it is for.
	if pos > s.positions[stream] {
		s.positions[stream] = pos
	}
	s.mu.Unlock()

	if s.handler != nil {
		s.handler.OnPosition(stream, instance, pos)
	}
}
