// Package sender is the PDU pipeline: replication poke to queued transaction.
//
// It reproduces Synapse's _process_event_queue_loop
// (federation/sender/__init__.py:525). The shape is deliberately the same,
// including the parts that look like they could be simplified:
//
//   - The replication stream is a POKE. The row says an event exists; the
//     content comes from the database. Synapse works this way because the row
//     does not carry enough to send, and copying that means our decisions are
//     made from the same source as the real sender's.
//   - Events are grouped by room, rooms processed concurrently, events within a
//     room strictly in order. Order within a room is a correctness property --
//     a remote receiving a message before the join that authorises it will
//     reject it.
package sender

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
)

// Store is the database access the pipeline needs.
type Store interface {
	GetAllNewEventIDsStream(ctx context.Context, from, upto int64, limit int) ([]store.NewEvent, int64, error)
	GetEvents(ctx context.Context, ids []string) ([]store.Event, error)
	MaxStreamOrdering(ctx context.Context) (int64, error)
	GetDestinationRetryTimings(ctx context.Context, destinations []string) (map[string]store.RetryTimings, error)
}

// Cursors persists our position and our routing decisions.
type Cursors interface {
	Get(ctx context.Context, name string) (int64, bool, error)
	Set(ctx context.Context, name string, pos int64) error
	// RecordRoutes writes our routing decisions in the shape of Synapse's
	// destination_rooms, which is what the comparison joins against.
	RecordRoutes(ctx context.Context, routes []state.RoutedRoom) error
}

// Observer is told what the pipeline decided, so the shadow comparison and the
// metrics can be built on it without the pipeline knowing about either.
type Observer interface {
	// OnEventSkipped reports an event that will not be federated.
	OnEventSkipped(eventID string, reason destinations.SkipReason)
	// OnEventRouted reports an event's resolved destinations, before and after
	// the shard filter, and how the answer was reached. fallback is empty when
	// the destinations came from the state before the event, i.e. exactly as
	// Synapse would have computed them.
	OnEventRouted(eventID string, all, ours []string, fallback destinations.FallbackReason)
	// OnBatch reports one pass of the pickup loop.
	OnBatch(events, routed int, from, to int64, took time.Duration)
	// OnStage reports how long one stage of the pipeline took, so a change in
	// throughput can be attributed rather than merely noticed.
	OnStage(stage string, took time.Duration)
	// OnEventLag reports the age of an event when routing finished, which is
	// the number directly comparable with Synapse's event_processing_lag.
	OnEventLag(receivedTS int64, at time.Time)
}

// Config builds a Sender.
type Config struct {
	Store    Store
	Cursors  Cursors
	Resolver *destinations.Resolver
	Queues   *queue.Manager
	Observer Observer
	Log      zerolog.Logger

	// ServerName is ours, for the is-it-ours check on event senders.
	ServerName string
	// ShouldHandle answers whether a destination belongs to our shard.
	ShouldHandle func(destination string) bool
	// BatchLimit is the pickup query's limit, Synapse's 100.
	BatchLimit int
	// MaxRoomConcurrency bounds how many rooms are processed at once. Synapse
	// gathers all of them; a bound here keeps one very wide batch from
	// launching thousands of database queries at the same instant.
	MaxRoomConcurrency int
}

// Sender runs the PDU pipeline.
type Sender struct {
	cfg Config
	log zerolog.Logger

	mu sync.Mutex
	// lastPoked is the highest stream ordering we have been told exists. It is
	// the loop's upper bound, exactly like Synapse's _last_poked_id.
	lastPoked int64
	// processing is true while the loop runs, so a poke arriving mid-pass does
	// not start a second one.
	processing bool

	// wake carries pokes to the loop. Buffered with one slot: more than one
	// pending wakeup says nothing a single one does not, since the loop always
	// reads the newest position when it starts.
	wake chan struct{}
}

// New builds a Sender.
func New(cfg Config) *Sender {
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 100
	}
	if cfg.MaxRoomConcurrency <= 0 {
		cfg.MaxRoomConcurrency = 64
	}
	return &Sender{
		cfg:  cfg,
		log:  cfg.Log,
		wake: make(chan struct{}, 1),
	}
}

// NotifyNewEvents records that events exist up to maxStreamOrdering and wakes
// the loop.
//
// Synapse's notify_new_events (federation/sender/__init__.py:505). It takes
// only the stream position, not the rows: the rows are a poke.
func (s *Sender) NotifyNewEvents(maxStreamOrdering int64) {
	s.mu.Lock()
	if maxStreamOrdering > s.lastPoked {
		s.lastPoked = maxStreamOrdering
	}
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
		// A wakeup is already pending; a second says nothing new.
	}
}

// Run processes events until the context is cancelled.
func (s *Sender) Run(ctx context.Context) error {
	if err := s.seed(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
			if err := s.processAll(ctx); err != nil && ctx.Err() == nil {
				// A failed pass is not fatal: the cursor was not advanced, so
				// the next poke retries the same events. Logging and carrying
				// on is what Synapse does, and stopping here would turn a
				// transient database blip into a worker that never recovers.
				s.log.Error().Err(err).Msg("event pickup pass failed; will retry on the next poke")
			}
		}
	}
}

// seed establishes the starting position.
//
// Our own cursor if we have one, otherwise the position of the sender we
// shadow, which is what makes the two comparable from the first event rather
// than after a replay of the whole database.
func (s *Sender) seed(ctx context.Context) error {
	max, err := s.cfg.Store.MaxStreamOrdering(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if max > s.lastPoked {
		s.lastPoked = max
	}
	s.mu.Unlock()

	pos, ok, err := s.cfg.Cursors.Get(ctx, state.CursorEvents)
	if err != nil {
		return err
	}
	s.log.Info().
		Int64("cursor", pos).Bool("resuming", ok).Int64("max_stream_ordering", max).
		Msg("seeded the event pickup loop")

	// Nudge the loop so a restart with a backlog starts working immediately
	// rather than waiting for the next event on a quiet server.
	s.NotifyNewEvents(max)
	return nil
}

// processAll drains the backlog, one batch at a time.
func (s *Sender) processAll(ctx context.Context) error {
	s.mu.Lock()
	if s.processing {
		s.mu.Unlock()
		return nil
	}
	s.processing = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.processing = false
		s.mu.Unlock()
	}()

	for ctx.Err() == nil {
		more, err := s.processBatch(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
	return ctx.Err()
}

// processBatch is one pass of Synapse's loop. It reports whether there is more
// to do.
func (s *Sender) processBatch(ctx context.Context) (bool, error) {
	started := time.Now()

	from, ok, err := s.cfg.Cursors.Get(ctx, state.CursorEvents)
	if err != nil {
		return false, err
	}
	if !ok {
		// Never run before. Start from the current tip rather than replaying
		// the whole database: a shadow that spent its first day re-deciding
		// history would be comparing against a real sender that is not doing
		// anything of the kind.
		s.mu.Lock()
		from = s.lastPoked
		s.mu.Unlock()
		if err := s.cfg.Cursors.Set(ctx, state.CursorEvents, from); err != nil {
			return false, err
		}
		s.log.Info().Int64("from", from).Msg("no saved cursor; starting from the current tip")
	}

	s.mu.Lock()
	upto := s.lastPoked
	s.mu.Unlock()

	if from >= upto {
		return false, nil
	}

	rows, next, err := s.cfg.Store.GetAllNewEventIDsStream(ctx, from, upto, s.cfg.BatchLimit)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		// Nothing in the window, but the window itself may have been a gap in
		// stream orderings. Advancing past it is what stops the loop spinning.
		if next > from {
			if err := s.cfg.Cursors.Set(ctx, state.CursorEvents, next); err != nil {
				return false, err
			}
		}
		return next < upto, nil
	}

	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.EventID)
	}
	events, err := s.cfg.Store.GetEvents(ctx, ids)
	if err != nil {
		return false, err
	}

	routed, err := s.handleEvents(ctx, events)
	if err != nil {
		return false, err
	}

	if err := s.cfg.Cursors.Set(ctx, state.CursorEvents, next); err != nil {
		return false, err
	}
	if s.cfg.Observer != nil {
		s.cfg.Observer.OnBatch(len(events), routed, from, next, time.Since(started))
	}
	return next < upto, nil
}

// handleEvents groups by room and processes rooms concurrently.
//
// Within a room the events are handled strictly in order. That is not an
// optimisation to preserve: a remote server receiving a message before the join
// that authorises it will reject the message.
func (s *Sender) handleEvents(ctx context.Context, events []store.Event) (int, error) {
	byRoom := map[string][]store.Event{}
	order := make([]string, 0, len(events))
	for _, e := range events {
		if _, seen := byRoom[e.RoomID]; !seen {
			order = append(order, e.RoomID)
		}
		byRoom[e.RoomID] = append(byRoom[e.RoomID], e)
	}

	var routed struct {
		sync.Mutex
		n int
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.cfg.MaxRoomConcurrency)
	for _, roomID := range order {
		roomEvents := byRoom[roomID]
		g.Go(func() error {
			n := 0
			for _, e := range roomEvents {
				sent, err := s.handleEvent(gctx, e)
				if err != nil {
					return err
				}
				if sent {
					n++
				}
			}
			routed.Lock()
			routed.n += n
			routed.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return routed.n, nil
}
