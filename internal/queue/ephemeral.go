package queue

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Ephemeral EDU limits, from Synapse.
const (
	// maxReceiptEDUs is how many m.receipt EDUs one transaction carries
	// (per_destination_queue.py:652).
	maxReceiptEDUs = 5
	// maxPresenceStates is how many users one m.presence EDU describes
	// (per_destination_queue.py:79). Presence is the highest-volume EDU there
	// is, and without a bound one transaction to a busy server could carry
	// thousands of states.
	maxPresenceStates = 50
)

// EnqueueKeyedEDU adds a unit that REPLACES any pending unit with the same key.
//
// Typing is the reason this exists. "alice is typing" followed by "alice has
// stopped" are two updates about one fact, and a destination that was
// unreachable in between should receive only the second -- delivering both
// would show a typing indicator that flickers on after the user has finished.
// Synapse keys these by (edu_type, key) and clobbers
// (per_destination_queue.py:152).
func (d *Destination) EnqueueKeyedEDU(key string, e txn.EDU) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.pendingEDUs {
		if d.pendingEDUs[i].Key == key && d.pendingEDUs[i].Unit.Type == e.Type {
			d.pendingEDUs[i].Unit = e
			d.newData = true
			return
		}
	}
	d.pendingEDUs = append(d.pendingEDUs, EDU{Unit: e, Key: key})
	d.newData = true
}

// EnqueueReceipt merges a read receipt into the pending receipt EDUs.
//
// Receipts are not clobbered like typing and not appended like to-device: they
// are MERGED into a nested map of room -> receipt type -> user, because one EDU
// can carry every outstanding receipt for a destination and sending one EDU per
// receipt would waste a transaction slot on each.
//
// A new EDU is started only when the (room, type, user) slot is already taken
// by a receipt for a DIFFERENT thread (per_destination_queue.py:241). Two
// receipts from one user in one room for different threads are both real and
// neither may overwrite the other; the same user re-reading the same thread is
// one fact, and the newer wins.
func (d *Destination) EnqueueReceipt(roomID, receiptType, userID, threadID string, content json.RawMessage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := range d.pendingEDUs {
		e := &d.pendingEDUs[i]
		if e.Unit.Type != txn.EDUTypeReceipt || e.Receipts == nil {
			continue
		}
		byType, ok := e.Receipts[roomID]
		if !ok {
			byType = map[string]map[string]receiptEntry{}
			e.Receipts[roomID] = byType
		}
		byUser, ok := byType[receiptType]
		if !ok {
			byUser = map[string]receiptEntry{}
			byType[receiptType] = byUser
		}
		existing, taken := byUser[userID]
		if !taken || existing.threadID == threadID {
			byUser[userID] = receiptEntry{threadID: threadID, content: content}
			d.newData = true
			return
		}
		// Slot taken by another thread's receipt: try the next EDU.
	}

	d.pendingEDUs = append(d.pendingEDUs, EDU{
		Unit: txn.EDU{Type: txn.EDUTypeReceipt},
		Receipts: map[string]map[string]map[string]receiptEntry{
			roomID: {receiptType: {userID: {threadID: threadID, content: content}}},
		},
	})
	d.newData = true
}

// PresenceEntry is one user's presence, waiting to be sent.
//
// Encode is deferred rather than the content being handed over ready-made,
// because the wire format carries last_active_ago -- a duration from NOW, not a
// timestamp. Encoding on arrival and sending later would understate how long
// ago the user was active by however long the EDU waited, which is the one
// field in a presence update that silently rots. Synapse defers for the same
// reason, formatting from the stored state at transaction time
// (presence.py:1935 via per_destination_queue).
type PresenceEntry struct {
	UserID string
	Encode func(nowMS int64) json.RawMessage
}

// EnqueuePresence merges one user's presence into the pending m.presence EDU.
//
// A newer state for a user REPLACES the older one rather than being appended:
// presence is that user's current state, so two updates about one person are
// one fact and only the latest is worth sending. Synapse keeps _pending_presence
// as a dict keyed by user for exactly this.
//
// A new EDU is started only when the pending one already holds
// maxPresenceStates users and this user is not among them.
func (d *Destination) EnqueuePresence(e PresenceEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := range d.pendingEDUs {
		p := &d.pendingEDUs[i]
		if p.Unit.Type != txn.EDUTypePresence || p.Presence == nil {
			continue
		}
		if _, replacing := p.Presence[e.UserID]; !replacing && len(p.Presence) >= maxPresenceStates {
			// Full, and this is a new user. Try the next EDU.
			continue
		}
		p.Presence[e.UserID] = e
		d.newData = true
		return
	}

	d.pendingEDUs = append(d.pendingEDUs, EDU{
		Unit:     txn.EDU{Type: txn.EDUTypePresence},
		Presence: map[string]PresenceEntry{e.UserID: e},
	})
	d.newData = true
}

// receiptEntry is one user's receipt, with the thread it belongs to.
type receiptEntry struct {
	threadID string
	content  json.RawMessage
}

// materialise turns a merged receipt EDU into its wire content.
//
// Deferred until the transaction is built rather than done on arrival, because
// receipts keep merging into the same EDU while it waits: encoding on arrival
// would produce a body that is stale by the time it is sent.
func (e *EDU) materialise() (txn.EDU, error) {
	if e.Presence != nil {
		return e.materialisePresence()
	}
	if e.Receipts == nil {
		return e.Unit, nil
	}
	body := map[string]map[string]map[string]json.RawMessage{}
	for room, byType := range e.Receipts {
		body[room] = map[string]map[string]json.RawMessage{}
		for receiptType, byUser := range byType {
			body[room][receiptType] = map[string]json.RawMessage{}
			for user, entry := range byUser {
				body[room][receiptType][user] = entry.content
			}
		}
	}
	content, err := json.Marshal(body)
	if err != nil {
		return txn.EDU{}, err
	}
	return txn.EDU{Type: txn.EDUTypeReceipt, Content: content}, nil
}

// materialisePresence renders the merged presence EDU.
//
// The users are sorted, which matters here and not for receipts: presence goes
// on the wire as an ARRAY, so map iteration order would make two runs over the
// same state produce different bytes -- and comparing our transactions with
// Synapse's byte for byte is the point of the capture rig.
func (e *EDU) materialisePresence() (txn.EDU, error) {
	users := make([]string, 0, len(e.Presence))
	for u := range e.Presence {
		users = append(users, u)
	}
	sort.Strings(users)

	now := time.Now().UnixMilli()
	push := make([]json.RawMessage, 0, len(users))
	for _, u := range users {
		push = append(push, e.Presence[u].Encode(now))
	}
	content, err := json.Marshal(map[string]any{"push": push})
	if err != nil {
		return txn.EDU{}, err
	}
	return txn.EDU{Type: txn.EDUTypePresence, Content: content}, nil
}
