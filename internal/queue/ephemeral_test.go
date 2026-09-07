package queue

import (
	"encoding/json"
	"testing"

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
