package queue

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Typing is two statements about one fact. A destination that was briefly
// unreachable must receive only the latest -- delivering both would show an
// indicator that switches on after the user finished typing.
func TestKeyedEDUReplacesRatherThanAccumulates(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	d.EnqueueKeyedEDU("m.typing|!room|@alice",
		txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"typing":true}`)})
	d.EnqueueKeyedEDU("m.typing|!room|@alice",
		txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"typing":false}`)})

	edus := d.PeekEDUs()
	if len(edus) != 1 {
		t.Fatalf("%d EDUs queued, want the later to replace the earlier", len(edus))
	}
	if string(edus[0].Content) != `{"typing":false}` {
		t.Errorf("content = %s, want the latest update", edus[0].Content)
	}
}

// Different keys are different facts and must not clobber each other: typing in
// one room says nothing about typing in another.
func TestDifferentKeysCoexist(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})
	d.EnqueueKeyedEDU("m.typing|!a|@alice",
		txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"room_id":"!a"}`)})
	d.EnqueueKeyedEDU("m.typing|!b|@alice",
		txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"room_id":"!b"}`)})

	if got := len(d.PeekEDUs()); got != 2 {
		t.Errorf("%d EDUs, want one per room", got)
	}
}

// Receipts merge into one EDU rather than one EDU each: a transaction carries
// at most a handful of EDUs, and spending one per receipt would crowd out
// everything else.
func TestReceiptsMergeIntoOneEDU(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	d.EnqueueReceipt("!room", "m.read", "@alice:x", "", json.RawMessage(`{"event_ids":["$1"]}`))
	d.EnqueueReceipt("!room", "m.read", "@bob:x", "", json.RawMessage(`{"event_ids":["$2"]}`))
	d.EnqueueReceipt("!other", "m.read", "@alice:x", "", json.RawMessage(`{"event_ids":["$3"]}`))

	edus := d.PeekEDUs()
	if len(edus) != 1 {
		t.Fatalf("%d receipt EDUs, want them merged into one", len(edus))
	}

	var body map[string]map[string]map[string]json.RawMessage
	if err := json.Unmarshal(edus[0].Content, &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 2 {
		t.Errorf("%d rooms in the EDU, want 2", len(body))
	}
	if len(body["!room"]["m.read"]) != 2 {
		t.Errorf("%d users in !room, want alice and bob", len(body["!room"]["m.read"]))
	}
}

// The same user re-reading the same thread is one fact and the newer wins.
func TestSameThreadReceiptIsReplaced(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})
	d.EnqueueReceipt("!room", "m.read", "@alice:x", "", json.RawMessage(`{"event_ids":["$old"]}`))
	d.EnqueueReceipt("!room", "m.read", "@alice:x", "", json.RawMessage(`{"event_ids":["$new"]}`))

	edus := d.PeekEDUs()
	if len(edus) != 1 {
		t.Fatalf("%d EDUs, want one", len(edus))
	}
	var body map[string]map[string]map[string]json.RawMessage
	if err := json.Unmarshal(edus[0].Content, &body); err != nil {
		t.Fatal(err)
	}
	if got := string(body["!room"]["m.read"]["@alice:x"]); got != `{"event_ids":["$new"]}` {
		t.Errorf("kept %s, want the newer receipt", got)
	}
}

// Two receipts from one user in one room for DIFFERENT threads are both real,
// and neither may overwrite the other -- so the second starts a new EDU.
func TestDifferentThreadReceiptsStartANewEDU(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})
	d.EnqueueReceipt("!room", "m.read", "@alice:x", "$threadA",
		json.RawMessage(`{"event_ids":["$a"]}`))
	d.EnqueueReceipt("!room", "m.read", "@alice:x", "$threadB",
		json.RawMessage(`{"event_ids":["$b"]}`))

	edus := d.PeekEDUs()
	if len(edus) != 2 {
		t.Fatalf("%d EDUs, want a second for the other thread rather than an overwrite", len(edus))
	}

	seen := map[string]bool{}
	for _, e := range edus {
		var body map[string]map[string]map[string]json.RawMessage
		if err := json.Unmarshal(e.Content, &body); err != nil {
			t.Fatal(err)
		}
		seen[string(body["!room"]["m.read"]["@alice:x"])] = true
	}
	for _, want := range []string{`{"event_ids":["$a"]}`, `{"event_ids":["$b"]}`} {
		if !seen[want] {
			t.Errorf("receipt %s was lost", want)
		}
	}
}

// A merged receipt EDU is encoded when the transaction is built, not when the
// first receipt arrives -- otherwise the body would be stale by the time it is
// sent.
func TestReceiptContentIsEncodedAtSendTime(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{})

	d.EnqueueReceipt("!room", "m.read", "@alice:x", "", json.RawMessage(`{"event_ids":["$1"]}`))
	// Arrives after the first, and must appear in the same EDU.
	d.EnqueueReceipt("!room", "m.read", "@bob:x", "", json.RawMessage(`{"event_ids":["$2"]}`))

	d.Attempt(t.Context())
	waitFor(t, "the transaction", func() bool { return len(s.sent()) == 1 })

	var sentBody struct {
		EDUs []struct {
			Content map[string]map[string]map[string]json.RawMessage `json:"content"`
		} `json:"edus"`
	}
	if err := json.Unmarshal([]byte(s.requests[0].Body), &sentBody); err != nil {
		t.Fatal(err)
	}
	if len(sentBody.EDUs) != 1 {
		t.Fatalf("%d EDUs on the wire", len(sentBody.EDUs))
	}
	if got := len(sentBody.EDUs[0].Content["!room"]["m.read"]); got != 2 {
		t.Errorf("%d users in the sent EDU, want both", got)
	}
}

// One m.presence EDU carries up to fifty users, and this worker was emitting
// exactly 1.00 states per EDU -- a transaction, an HTTP request and a signature
// verification on somebody else's server for each single user.
func TestPresenceMergesIntoOneEDU(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	for _, u := range []string{"@a:x", "@b:x", "@c:x"} {
		d.EnqueuePresence(presenceEntry(u, 0))
	}
	if _, edus := d.Pending(); edus != 1 {
		t.Fatalf("%d EDUs queued, want one carrying all three users", edus)
	}

	unit, err := d.pendingEDUs[0].materialise()
	if err != nil {
		t.Fatal(err)
	}
	push := gjson.GetBytes(unit.Content, "push").Array()
	if len(push) != 3 {
		t.Fatalf("push has %d entries, want 3: %s", len(push), unit.Content)
	}
	// Sorted, because presence goes on the wire as an ARRAY: map order would
	// make two runs over the same state produce different bytes, and comparing
	// against Synapse byte for byte is the point of the capture rig.
	got := []string{push[0].Get("user_id").String(), push[1].Get("user_id").String(),
		push[2].Get("user_id").String()}
	if got[0] != "@a:x" || got[1] != "@b:x" || got[2] != "@c:x" {
		t.Errorf("push order = %v, want sorted by user", got)
	}
}

// Presence is a user's CURRENT state, so two updates about one person are one
// fact and only the latest is worth sending. Synapse keeps _pending_presence as
// a dict keyed by user for the same reason.
func TestPresenceReplacesPerUser(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	d.EnqueuePresence(presenceEntry("@a:x", 1))
	d.EnqueuePresence(presenceEntry("@a:x", 2))

	if _, edus := d.Pending(); edus != 1 {
		t.Fatalf("%d EDUs queued, want one", edus)
	}
	unit, err := d.pendingEDUs[0].materialise()
	if err != nil {
		t.Fatal(err)
	}
	push := gjson.GetBytes(unit.Content, "push").Array()
	if len(push) != 1 {
		t.Fatalf("push has %d entries, want the newer state to have replaced the older", len(push))
	}
	if v := push[0].Get("v").Int(); v != 2 {
		t.Errorf("kept version %d, want the newer 2", v)
	}
}

// Fifty per EDU is Synapse's bound (per_destination_queue.py:79). Without it a
// transaction to a busy server could carry thousands of states.
func TestPresenceStartsANewEDUWhenFull(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	for i := 0; i < maxPresenceStates+5; i++ {
		d.EnqueuePresence(presenceEntry(fmt.Sprintf("@u%03d:x", i), 0))
	}
	_, edus := d.Pending()
	if edus != 2 {
		t.Fatalf("%d EDUs queued, want 2 once the first held %d", edus, maxPresenceStates)
	}
	first, err := d.pendingEDUs[0].materialise()
	if err != nil {
		t.Fatal(err)
	}
	if n := len(gjson.GetBytes(first.Content, "push").Array()); n != maxPresenceStates {
		t.Errorf("first EDU carries %d states, want %d", n, maxPresenceStates)
	}
}

// last_active_ago is a duration from NOW, so it must be rendered when the
// transaction is built. Encoding on arrival and sending later would understate
// how long ago the user was actually active by however long the EDU waited.
func TestPresenceEncodesAtTransactionTime(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})

	var encodedAt int64
	d.EnqueuePresence(PresenceEntry{
		UserID: "@a:x",
		Encode: func(nowMS int64) json.RawMessage {
			encodedAt = nowMS
			return json.RawMessage(`{"user_id":"@a:x"}`)
		},
	})
	if encodedAt != 0 {
		t.Fatal("presence was encoded on arrival, not at transaction time")
	}

	before := time.Now().UnixMilli()
	if _, err := d.pendingEDUs[0].materialise(); err != nil {
		t.Fatal(err)
	}
	if encodedAt < before {
		t.Errorf("encoded at %d, before materialise was called at %d", encodedAt, before)
	}
}

func presenceEntry(userID string, version int) PresenceEntry {
	return PresenceEntry{
		UserID: userID,
		Encode: func(int64) json.RawMessage {
			return json.RawMessage(fmt.Sprintf(`{"user_id":%q,"v":%d}`, userID, version))
		},
	}
}
