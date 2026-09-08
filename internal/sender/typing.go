package sender

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Synapse's typing timings, from handlers/typing.py:64.
//
// A remote server expires a typing indicator after FederationTimeout, so a user
// who keeps typing must be re-announced before that. FederationPingInterval is
// deliberately shorter, and the gap is the margin for a slow transaction.
const (
	FederationPingInterval = 40 * time.Second
	FederationTimeout      = time.Minute
)

// Typing turns the typing replication stream into federation EDUs.
//
// The mechanism is not the obvious one, and getting it wrong is a feature that
// is silently absent rather than broken. Typing is NOT relayed to the sender as
// a ready-made EDU the way it first appears: handlers/typing.py:188 builds the
// EDU, but the object it calls is only non-nil when should_send_federation() is
// true (typing.py:89), i.e. only on an instance that IS a federation sender. On
// every other instance -- including the one a user's PUT .../typing lands on --
// self.federation is None and _push_remote returns immediately.
//
// So nothing is put on the federation stream at all. Instead every instance
// feeds typing rows to FollowerTypingHandler.process_replication_rows
// (replication/tcp/client.py:148), and on the sender that handler diffs the
// room's typing set against the previous one and builds the EDUs ITSELF
// (typing.py:231). Being the federation sender means doing that work, not
// receiving its output.
//
// A row is the WHOLE typing set for one room, not a delta: "alice stopped
// typing" arrives as a row listing everyone except alice. The diff against the
// previous set is the only place the start/stop distinction exists.
type Typing struct {
	hosts  RoomHostsSource
	queues *queue.Manager
	log    zerolog.Logger

	serverName   string
	shouldHandle func(destination string) bool
	// due reports whether a destination's backoff allows an attempt. Synapse
	// filters typing destinations the same way before building the EDU
	// (typing.py:180); a typing notification for a server that is down is worth
	// nothing by the time it comes back.
	due func(destination string) bool

	// work carries pushes to the single consumer in Run.
	//
	// Typing arrives as a stream of tokens whose ORDER is the whole meaning: a
	// row is the room's complete typing set, so the diff against the previous
	// one is where start and stop come from. Handing each batch to its own
	// goroutine loses that order -- the batch for token 19259 can run after the
	// one for 19255, which reads as the writer having gone backwards and
	// throws away every room's state. Observed doing exactly that, several
	// times a minute.
	//
	// So the diff runs inline on the replication goroutine, where it is a few
	// map operations and cannot stall anything, and only the pushes -- which
	// read room membership from the database -- are handed off. One consumer,
	// so they stay in the order the diff produced them.
	work chan typingWork

	mu sync.Mutex
	// serial is the last typing stream token seen, for the backwards check.
	serial int64
	// rooms is the current typing set per room, which is what makes a diff
	// possible.
	rooms map[string]map[string]bool
	// lastPoke is when each member was last announced, for the keep-alive.
	lastPoke map[member]time.Time
}

// member is one user typing in one room -- Synapse's RoomMember, which is also
// the EDU's clobber key (typing.py:194).
type member struct {
	roomID string
	userID string
}

// RoomHostsSource answers which servers are in a room right now. Synapse's
// get_current_hosts_in_room, and current state rather than state at an event:
// typing is about now, and there is no event to resolve against.
type RoomHostsSource interface {
	CurrentJoinedHosts(ctx context.Context, roomID string) ([]string, error)
}

// TypingConfig builds a Typing.
type TypingConfig struct {
	Hosts        RoomHostsSource
	Queues       *queue.Manager
	Log          zerolog.Logger
	ServerName   string
	ShouldHandle func(destination string) bool
	Due          func(destination string) bool
}

// NewTyping builds the typing router.
func NewTyping(cfg TypingConfig) *Typing {
	return &Typing{
		hosts: cfg.Hosts, queues: cfg.Queues, log: cfg.Log,
		serverName: cfg.ServerName, shouldHandle: cfg.ShouldHandle, due: cfg.Due,
		rooms: map[string]map[string]bool{}, lastPoke: map[member]time.Time{},
		work: make(chan typingWork, typingWorkBuffer),
	}
}

// typingWorkBuffer is how many pushes may be waiting on the consumer.
//
// Typing is low volume -- 0.37 rows per second on this deployment -- so this is
// generous. It is bounded rather than unbounded because the consumer talks to
// the database: if that stalls, an unbounded queue turns one slow query into
// unbounded memory.
const typingWorkBuffer = 1024

// typingWork is one member's typing state, waiting to be sent.
type typingWork struct {
	m      member
	typing bool
}

// TypingRow is one row of the typing stream: the complete set of users typing
// in one room (replication/tcp/streams/_base.py:413).
type TypingRow struct {
	RoomID  string
	UserIDs []string
}

// HandleRows processes a batch of typing rows.
//
// Called SYNCHRONOUSLY from the replication goroutine, and that is required
// rather than incidental: the diff is only meaningful in token order. It does
// no I/O -- the database work is queued for Run.
func (t *Typing) HandleRows(token int64, rows []TypingRow) {
	starts, stops := t.diff(token, rows)
	for _, m := range starts {
		t.submit(typingWork{m: m, typing: true})
	}
	for _, m := range stops {
		t.submit(typingWork{m: m, typing: false})
	}
}

// Run sends the queued typing updates, one at a time and in order.
func (t *Typing) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case w := <-t.work:
			pushCtx, cancel := context.WithTimeout(ctx, typingPushTimeout)
			t.push(pushCtx, w.m, w.typing)
			cancel()
		}
	}
}

// typingPushTimeout bounds one push's database work.
const typingPushTimeout = 30 * time.Second

// submit queues a push, dropping it if the consumer has fallen far behind.
//
// Dropped rather than blocked: blocking here would stall the replication
// subscriber, which would hold up every other stream to preserve a typing
// notification -- the least important thing this worker sends.
func (t *Typing) submit(w typingWork) {
	select {
	case t.work <- w:
	default:
		t.log.Warn().Str("room", w.m.roomID).Str("user", w.m.userID).
			Msg("typing queue is full; dropping an update")
	}
}

// diff updates the remembered sets and returns who started and stopped.
//
// Held apart from push so the state transition is one short critical section
// and the database work is not done under the lock.
func (t *Typing) diff(token int64, rows []TypingRow) (starts, stops []member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// The writer went backwards, which means it restarted. Synapse clears
	// everything here rather than trying to reconcile (typing.py:206): the
	// remembered sets are now of unknown age, and a stale "alice is typing"
	// that never gets its matching stop would leave an indicator on forever.
	if token < t.serial {
		t.log.Info().Int64("from", t.serial).Int64("to", token).
			Msg("typing stream went backwards; forgetting who was typing")
		t.rooms = map[string]map[string]bool{}
		t.lastPoke = map[member]time.Time{}
	}
	t.serial = token

	for _, row := range rows {
		prev := t.rooms[row.RoomID]
		now := make(map[string]bool, len(row.UserIDs))
		for _, u := range row.UserIDs {
			now[u] = true
		}
		t.rooms[row.RoomID] = now
		if len(now) == 0 {
			// Nothing is typing here any more; do not keep the empty set.
			delete(t.rooms, row.RoomID)
		}

		for u := range now {
			if !prev[u] && t.isMine(u) {
				starts = append(starts, member{row.RoomID, u})
			}
		}
		for u := range prev {
			if !now[u] && t.isMine(u) {
				m := member{row.RoomID, u}
				stops = append(stops, m)
				delete(t.lastPoke, m)
			}
		}
	}

	// Map iteration is random and these become EDUs; a stable order keeps a
	// two-user room from producing a different transaction on every run, which
	// matters for comparing against Synapse.
	sortMembers(starts)
	sortMembers(stops)
	return starts, stops
}

// KeepAlive re-announces everyone still typing whose last announcement is
// older than FederationPingInterval.
//
// Without it a remote server expires the indicator after FederationTimeout and
// a user who is still typing appears to have stopped -- Synapse re-pokes on a
// wheel timer for exactly this reason (typing.py:143).
func (t *Typing) KeepAlive() {
	now := time.Now()

	t.mu.Lock()
	var due []member
	for room, users := range t.rooms {
		for user := range users {
			if !t.isMine(user) {
				continue
			}
			m := member{room, user}
			if last, ok := t.lastPoke[m]; !ok || now.Sub(last) >= FederationPingInterval {
				due = append(due, m)
			}
		}
	}
	sortMembers(due)
	t.mu.Unlock()

	// Through the same consumer as the diffs, so a keep-alive cannot overtake a
	// stop and leave an indicator switched on.
	for _, m := range due {
		t.submit(typingWork{m: m, typing: true})
	}
}

// push builds the EDU and queues it for every server in the room.
func (t *Typing) push(ctx context.Context, m member, typing bool) {
	hosts, err := t.hosts.CurrentJoinedHosts(ctx, m.roomID)
	if err != nil {
		t.log.Error().Err(err).Str("room", m.roomID).Msg("failed to resolve typing destinations")
		return
	}

	content, err := json.Marshal(map[string]any{
		"room_id": m.roomID,
		"user_id": m.userID,
		"typing":  typing,
	})
	if err != nil {
		t.log.Error().Err(err).Msg("failed to encode a typing EDU")
		return
	}

	// The clobber key is (room, user), Synapse's RoomMember (typing.py:194).
	// Keyed rather than appended because "alice is typing" then "alice has
	// stopped" are two statements about one fact: a destination that was
	// briefly unreachable must receive only the second, or the indicator
	// switches on after she has finished.
	key := m.roomID + "|" + m.userID

	sent := 0
	for _, host := range hosts {
		if host == t.serverName || !t.shouldHandle(host) {
			continue
		}
		// Synapse drops destinations that are backing off before it builds the
		// EDU (typing.py:180). Queueing one anyway would hold a notification
		// that is worthless by the time the server returns.
		if t.due != nil && !t.due(host) {
			continue
		}
		q := t.queues.Get(host)
		q.EnqueueKeyedEDU(key, txn.EDU{Type: txn.EDUTypeTyping, Content: content})
		t.queues.Wake(q)
		sent++
	}

	if typing && sent > 0 {
		t.mu.Lock()
		t.lastPoke[m] = time.Now()
		t.mu.Unlock()
	}

	t.log.Debug().
		Str("room", m.roomID).Str("user", m.userID).Bool("typing", typing).
		Int("destinations", sent).Msg("typing routed")
}

func (t *Typing) isMine(userID string) bool {
	for i := len(userID) - 1; i >= 0; i-- {
		if userID[i] == ':' {
			return userID[i+1:] == t.serverName
		}
	}
	return false
}

func sortMembers(m []member) {
	sort.Slice(m, func(i, j int) bool {
		if m[i].roomID != m[j].roomID {
			return m[i].roomID < m[j].roomID
		}
		return m[i].userID < m[j].userID
	})
}
