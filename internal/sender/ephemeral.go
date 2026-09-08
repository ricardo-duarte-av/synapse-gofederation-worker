package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// receiptImmediateThreshold is Synapse's cutoff for sending a read receipt at
// once rather than batching it (federation/sender/__init__.py:849).
//
// Under ten destinations, or an event less than a minute old, everyone gets it
// immediately; otherwise only the event sender's server does and the rest wait
// for the next transaction. The reasoning is that a receipt for something just
// said is worth a transaction of its own, while a receipt for yesterday's
// message is not worth waking a thousand servers for.
const (
	receiptImmediateDestinations = 10
	receiptImmediateAgeMS        = 60_000
)

// EphemeralStore is the database access the ephemeral EDU paths need.
type EphemeralStore interface {
	CurrentJoinedHosts(ctx context.Context, roomID string) ([]string, error)
	GetPresenceStates(ctx context.Context, userIDs []string) (map[string]store.PresenceState, error)
}

// Ephemeral routes receipts, typing and presence.
//
// Grouped together because they share a shape that the durable EDUs do not:
// none of them is backed by a table this worker consumes, so none of them can
// be replayed. If a destination is unreachable the update is simply lost, and
// that is correct -- a read receipt that arrives an hour late is worse than one
// that never arrives, and catch-up deliberately carries no EDUs at all
// (per_destination_queue.py:596).
type Ephemeral struct {
	store  EphemeralStore
	queues *queue.Manager
	log    zerolog.Logger

	serverName   string
	shouldHandle func(destination string) bool
	// dueWithin reports whether a destination is worth building an EDU for:
	// due now, or due within the hour. Synapse asks the same question before
	// every ephemeral send (federation/sender/__init__.py:915,994) and the
	// point is to not do the work at all for a server that is down.
	dueWithin func(destination string) bool
}

// EphemeralConfig builds an Ephemeral.
type EphemeralConfig struct {
	Store        EphemeralStore
	Queues       *queue.Manager
	Log          zerolog.Logger
	ServerName   string
	ShouldHandle func(destination string) bool
	// DueWithin reports whether a destination is due now or within the hour.
	// Nil means every destination is treated as due, which is what a test or a
	// worker with no backoff wants.
	DueWithin func(destination string) bool
}

// NewEphemeral builds the ephemeral EDU router.
func NewEphemeral(cfg EphemeralConfig) *Ephemeral {
	return &Ephemeral{
		store: cfg.Store, queues: cfg.Queues, log: cfg.Log,
		serverName: cfg.ServerName, shouldHandle: cfg.ShouldHandle,
		dueWithin: cfg.DueWithin,
	}
}

// HandleReceipt routes one read receipt to the servers in its room.
func (e *Ephemeral) HandleReceipt(ctx context.Context, r ReceiptUpdate) error {
	// Only OUR users' receipts are ours to send, and private read receipts are
	// never federated at all (federation/sender/__init__.py:849) -- they exist
	// so a user can track their own reading without telling the room.
	if !e.isMine(r.UserID) || r.ReceiptType == "m.read.private" {
		// Logged, because "we filtered it" and "we failed to route it" look
		// identical from a metrics counter, and the device path taught that a
		// silent EDU route is one nobody can debug.
		e.log.Debug().
			Str("user", r.UserID).Str("type", r.ReceiptType).
			Bool("ours", e.isMine(r.UserID)).
			Msg("receipt not ours to send")
		return nil
	}

	hosts, err := e.store.CurrentJoinedHosts(ctx, r.RoomID)
	if err != nil {
		return err
	}

	content, err := json.Marshal(map[string]any{
		"event_ids": []string{r.EventID},
		"data":      r.Data,
	})
	if err != nil {
		return fmt.Errorf("sender: encoding receipt: %w", err)
	}

	sent := 0
	for _, host := range e.ourDestinations(hosts) {
		// Synapse asks the same before queueing a receipt
		// (federation/sender/__init__.py:915). A read receipt for a server that
		// has been down for a week is worth nothing by the time it returns.
		if !e.due(host) {
			continue
		}
		q := e.queues.Get(host)
		q.EnqueueReceipt(r.RoomID, r.ReceiptType, r.UserID, r.ThreadID, content)
		e.queues.Wake(q)
		sent++
	}
	e.log.Debug().Str("room", r.RoomID).Str("user", r.UserID).
		Int("destinations", sent).Msg("receipt routed")
	return nil
}

// due reports whether a destination is worth building an EDU for. A nil hook
// means yes, which is what a worker with no backoff and every test wants.
func (e *Ephemeral) due(destination string) bool {
	return e.dueWithin == nil || e.dueWithin(destination)
}

// ReceiptUpdate is one read receipt from the receipts stream.
type ReceiptUpdate struct {
	RoomID      string
	ReceiptType string
	UserID      string
	EventID     string
	ThreadID    string
	Data        json.RawMessage
}

// HandleTyping routes a typing notification.
//
// Keyed, so a later update for the same room and user replaces a pending
// earlier one. "alice is typing" then "alice has stopped" are two statements
// about one fact, and delivering both to a destination that was briefly
// unreachable would show an indicator that switches on after she finished.
func (e *Ephemeral) HandleTyping(destination, key string, content json.RawMessage) {
	if !e.shouldHandle(destination) || destination == e.serverName {
		return
	}
	q := e.queues.Get(destination)
	q.EnqueueKeyedEDU(key, txn.EDU{Type: txn.EDUTypeTyping, Content: content})
	e.queues.Wake(q)
}

// HandleEDU routes an arbitrary EDU handed to us on the federation stream.
func (e *Ephemeral) HandleEDU(destination, eduType string, content json.RawMessage) {
	if !e.shouldHandle(destination) || destination == e.serverName {
		return
	}
	q := e.queues.Get(destination)
	q.EnqueueEDU(txn.EDU{Type: eduType, Content: content})
	e.queues.Wake(q)
}

// HandlePresence routes presence updates to one destination.
//
// The states are read now rather than taken from the replication row: presence
// changes faster than it can be delivered, and a state captured when the row
// was written is often already wrong. Stale presence is worse than none -- it
// says somebody is online who has left.
func (e *Ephemeral) HandlePresence(ctx context.Context, destination string, userIDs []string) error {
	if !e.shouldHandle(destination) || destination == e.serverName || len(userIDs) == 0 {
		return nil
	}
	// Before the database read, not after the EDU is built. Synapse filters
	// here too (federation/sender/__init__.py:986), and the cost of not doing
	// so is not theoretical: about half this homeserver's ten thousand
	// destinations are backing off, so half of all presence was being read from
	// the database, encoded, queued, and then thrown away by the long-outage
	// rule -- measured at 8.98 dropped EDUs per second against 8.67 sent.
	if !e.due(destination) {
		return nil
	}
	states, err := e.store.GetPresenceStates(ctx, userIDs)
	if err != nil {
		return err
	}
	if len(states) == 0 {
		return nil
	}

	// Merged into the destination's pending m.presence EDU rather than sent as
	// an EDU of its own. One EDU carries fifty users
	// (per_destination_queue.py:79), and this worker was emitting exactly 1.00
	// states per EDU -- a transaction, an HTTP request and a signature
	// verification on somebody else's server, for each single user.
	//
	// The encoding is deferred to transaction time. last_active_ago is a
	// duration from NOW, so rendering it here and sending later would
	// understate how long ago the user was actually active.
	q := e.queues.Get(destination)
	queued := 0
	for _, userID := range userIDs {
		st, ok := states[userID]
		if !ok {
			continue
		}
		q.EnqueuePresence(queue.PresenceEntry{
			UserID: userID,
			Encode: func(nowMS int64) json.RawMessage {
				body, err := json.Marshal(formatPresenceAt(st, nowMS))
				if err != nil {
					// Cannot happen for this shape, and an EDU is not worth
					// failing a transaction over. An empty object is dropped by
					// the receiver rather than corrupting the rest.
					return json.RawMessage(`{}`)
				}
				return body
			},
		})
		queued++
	}
	if queued == 0 {
		return nil
	}
	e.queues.Wake(q)
	return nil
}

// formatPresence is Synapse's format_user_presence_state (presence.py:1935).
//
// Every field but presence is conditional, and the conditions are the point:
// last_active_ago is omitted when there is no timestamp rather than sent as
// zero, which would claim the user was active at the epoch, and
// currently_active is meaningless unless the user is online.
func formatPresence(s store.PresenceState) map[string]any {
	return formatPresenceAt(s, nowMS())
}

// formatPresenceAt is formatPresence with the clock passed in, so the EDU can be
// rendered when the transaction is built rather than when the state was read.
func formatPresenceAt(s store.PresenceState, nowMS int64) map[string]any {
	out := map[string]any{
		"presence": s.State,
		"user_id":  s.UserID,
	}
	if s.LastActiveTS > 0 {
		out["last_active_ago"] = nowMS - s.LastActiveTS
	}
	if s.StatusMsg != "" {
		out["status_msg"] = s.StatusMsg
	}
	if s.State == "online" {
		out["currently_active"] = s.CurrentlyActive
	}
	return out
}

// ourDestinations filters room hosts to the ones this worker sends to.
func (e *Ephemeral) ourDestinations(hosts []string) []string {
	out := make([]string, 0, len(hosts))
	for _, h := range hosts {
		if h == e.serverName || !e.shouldHandle(h) {
			continue
		}
		out = append(out, h)
	}
	return out
}

func (e *Ephemeral) isMine(id string) bool {
	return destinations.DomainOf(id) == e.serverName
}

func nowMS() int64 { return time.Now().UnixMilli() }
