package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

var testKeyLine = "ed25519 testkey " + base64.RawStdEncoding.EncodeToString(make([]byte, 32))

// recordingSink captures what would have been sent and can be made to fail or
// to block.
type recordingSink struct {
	mu       sync.Mutex
	requests []*txn.Request
	// concurrent tracks how many sends are in flight at once, which is what
	// the "one transaction per destination" rule is about.
	inFlight    atomic.Int32
	maxInFlight atomic.Int32

	fail  atomic.Bool
	block chan struct{}
}

func (r *recordingSink) Mode() string { return "test" }

func (r *recordingSink) Send(ctx context.Context, req *txn.Request) (sink.Result, error) {
	n := r.inFlight.Add(1)
	for {
		max := r.maxInFlight.Load()
		if n <= max || r.maxInFlight.CompareAndSwap(max, n) {
			break
		}
	}
	defer r.inFlight.Add(-1)

	if r.block != nil {
		select {
		case <-r.block:
		case <-ctx.Done():
			return sink.Result{}, ctx.Err()
		}
	}
	if r.fail.Load() {
		return sink.Result{Delivered: false}, nil
	}

	r.mu.Lock()
	r.requests = append(r.requests, req)
	r.mu.Unlock()
	return sink.Result{Delivered: true}, nil
}

func (r *recordingSink) sent() []*txn.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*txn.Request, len(r.requests))
	copy(out, r.requests)
	return out
}

func testDestination(t *testing.T, s sink.Sink, limits Limits) *Destination {
	t.Helper()
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	return NewDestination(Config{
		Name: "b.example", Limits: limits, Signer: signer,
		IDs: txn.NewIDGenerator(txn.DefaultIDPrefix), Sink: s, Log: zerolog.New(io.Discard),
	})
}

func pdu(id string, order int64) PDU {
	return PDU{
		EventID:        id,
		StreamOrdering: order,
		JSON:           json.RawMessage(`{"event_id":"` + id + `","type":"m.room.message"}`),
	}
}

// waitFor polls until cond holds or the deadline passes, so the concurrency
// tests do not depend on a sleep being long enough.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSendsQueuedPDUs(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{})
	d.EnqueuePDU(pdu("$a", 1))
	d.EnqueuePDU(pdu("$b", 2))
	d.Attempt(context.Background())

	waitFor(t, "the transaction", func() bool { return len(s.sent()) == 1 })

	if p, e := d.Pending(); p != 0 || e != 0 {
		t.Errorf("after a successful send, %d pdus and %d edus are still queued", p, e)
	}
	if got := d.LastSuccessfulStreamOrdering(); got != 2 {
		t.Errorf("LastSuccessfulStreamOrdering = %d, want the highest sent, 2", got)
	}
}

// Synapse's hardcoded 50 (per_destination_queue.py:838) and
// MAX_EDUS_PER_TRANSACTION (api/constants.py:56). Exceeding either produces a
// transaction remote servers may reject and, while shadowing, one that cannot
// be compared with the real sender's.
func TestTransactionLimits(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{MaxPDUs: 3, MaxEDUs: 2})
	for i := 0; i < 7; i++ {
		d.EnqueuePDU(pdu("$e", int64(i+1)))
	}
	for i := 0; i < 5; i++ {
		d.EnqueueEDU(txn.EDU{Type: "m.receipt", Content: json.RawMessage(`{}`)})
	}
	d.Attempt(context.Background())

	waitFor(t, "everything to drain", func() bool {
		p, e := d.Pending()
		return p == 0 && e == 0
	})

	for i, req := range s.sent() {
		var body struct {
			PDUs []json.RawMessage `json:"pdus"`
			EDUs []json.RawMessage `json:"edus"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.PDUs) > 3 {
			t.Errorf("transaction %d has %d pdus, over the limit of 3", i, len(body.PDUs))
		}
		if len(body.EDUs) > 2 {
			t.Errorf("transaction %d has %d edus, over the limit of 2", i, len(body.EDUs))
		}
	}
}

// Nothing is dequeued until the send has succeeded. Synapse gets this from
// __aexit__ returning early on an exception (per_destination_queue.py:851); if
// it were wrong here, a failed transaction would silently lose its events.
func TestFailedSendKeepsUnitsQueued(t *testing.T) {
	s := &recordingSink{}
	s.fail.Store(true)
	d := testDestination(t, s, Limits{})
	d.EnqueuePDU(pdu("$a", 1))
	d.EnqueuePDU(pdu("$b", 2))
	d.Attempt(context.Background())

	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	if p, _ := d.Pending(); p != 2 {
		t.Errorf("%d pdus queued after a failure, want both still there", p)
	}
	if got := d.LastSuccessfulStreamOrdering(); got != 0 {
		t.Errorf("the cursor advanced to %d on a failed send", got)
	}

	// And a retry delivers them.
	s.fail.Store(false)
	d.Attempt(context.Background())
	waitFor(t, "the retry", func() bool {
		p, _ := d.Pending()
		return p == 0
	})
}

// Exactly one in-flight transaction per destination, ever. Two would put PDUs
// on the wire out of order.
func TestOneTransactionAtATime(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	d := testDestination(t, s, Limits{})
	d.EnqueuePDU(pdu("$a", 1))

	ctx := context.Background()
	d.Attempt(ctx)
	waitFor(t, "the first send to start", func() bool { return s.inFlight.Load() == 1 })

	// Hammer it while one is in flight.
	for i := 0; i < 20; i++ {
		d.EnqueuePDU(pdu("$more", int64(i+2)))
		d.Attempt(ctx)
	}
	close(s.block)

	waitFor(t, "everything to drain", func() bool {
		p, _ := d.Pending()
		return p == 0 && !d.isRunning()
	})
	if got := s.maxInFlight.Load(); got > 1 {
		t.Errorf("%d transactions were in flight at once, want at most 1", got)
	}
}

// Something enqueued WHILE a transaction is in flight must be noticed. The
// newData flag is cleared at the top of the loop for exactly this; clearing it
// after the send would swallow these.
func TestDataArrivingDuringASendIsNotLost(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	d := testDestination(t, s, Limits{})
	d.EnqueuePDU(pdu("$first", 1))
	d.Attempt(context.Background())
	waitFor(t, "the send to start", func() bool { return s.inFlight.Load() == 1 })

	// Enqueued mid-flight, with no further Attempt: the running loop must
	// pick it up by itself.
	d.EnqueuePDU(pdu("$second", 2))
	close(s.block)

	waitFor(t, "the second pdu", func() bool { return d.LastSuccessfulStreamOrdering() == 2 })
	if p, _ := d.Pending(); p != 0 {
		t.Errorf("%d pdus left queued", p)
	}
}

func TestOnSuccessReportsTheHighestStreamOrdering(t *testing.T) {
	var got struct {
		sync.Mutex
		dest  string
		order int64
	}
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: &recordingSink{}, Log: zerolog.New(io.Discard),
		OnSuccess: func(dest string, order int64) {
			got.Lock()
			got.dest, got.order = dest, order
			got.Unlock()
		},
	})
	d.EnqueuePDU(pdu("$a", 41))
	d.EnqueuePDU(pdu("$b", 42))
	d.Attempt(context.Background())

	waitFor(t, "the callback", func() bool {
		got.Lock()
		defer got.Unlock()
		return got.order == 42
	})
	got.Lock()
	defer got.Unlock()
	if got.dest != "b.example" {
		t.Errorf("callback destination = %q", got.dest)
	}
}

// EDU-only transactions are real: a receipt or a device list update with no
// events to carry it.
func TestEDUOnlyTransaction(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{})
	d.EnqueueEDU(txn.EDU{Type: "m.device_list_update", Content: json.RawMessage(`{"user_id":"@u:a.example"}`)})
	d.Attempt(context.Background())

	waitFor(t, "the transaction", func() bool { return len(s.sent()) == 1 })
	if got := d.LastSuccessfulStreamOrdering(); got != 0 {
		t.Errorf("an EDU-only transaction advanced the PDU cursor to %d", got)
	}
}

// The cursor only ever moves forward.
func TestSetLastSuccessfulStreamOrderingNeverGoesBackwards(t *testing.T) {
	d := testDestination(t, &recordingSink{}, Limits{})
	d.SetLastSuccessfulStreamOrdering(100)
	d.SetLastSuccessfulStreamOrdering(50)
	if got := d.LastSuccessfulStreamOrdering(); got != 100 {
		t.Errorf("cursor moved backwards to %d", got)
	}
}

// Bookkeeping must happen ONLY for a transaction that was actually delivered.
//
// The rows a mark authorises deleting are to-device messages and device pokes.
// Deleting them for a transaction that failed destroys undelivered messages
// with nothing to recover them from -- there is no second copy, and the
// receiving server never knew they existed.
func TestEDUMarksCommitOnlyOnDelivery(t *testing.T) {
	s := &recordingSink{}
	s.fail.Store(true)

	var committed struct {
		sync.Mutex
		calls    int
		toDevice int64
	}
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		OnEDUsSent: func(_ string, toDevice, _ int64) {
			committed.Lock()
			committed.calls++
			committed.toDevice = toDevice
			committed.Unlock()
		},
	})

	d.EnqueueMarkedEDU(EDU{
		Unit:         txn.EDU{Type: "m.direct_to_device", Content: []byte(`{"message_id":"m1"}`)},
		ToDeviceUpTo: 500,
	})
	d.Attempt(context.Background())
	waitFor(t, "the failing attempt to finish", func() bool { return !d.isRunning() })

	committed.Lock()
	calls := committed.calls
	committed.Unlock()
	if calls != 0 {
		t.Fatal("bookkeeping ran for a transaction that was never delivered; " +
			"those rows would have been deleted undelivered")
	}
	if _, edus := d.Pending(); edus != 1 {
		t.Errorf("%d EDUs queued after a failure, want it still there", edus)
	}

	// And on success it commits, with the mark it carried.
	s.fail.Store(false)
	d.Attempt(context.Background())
	waitFor(t, "the retry", func() bool {
		committed.Lock()
		defer committed.Unlock()
		return committed.calls == 1
	})
	committed.Lock()
	defer committed.Unlock()
	if committed.toDevice != 500 {
		t.Errorf("committed up to %d, want 500", committed.toDevice)
	}
}

// Only the highest mark in the delivered batch is committed, and marks from
// units left behind by the 100-EDU limit are not.
func TestOnlyDeliveredMarksAreCommitted(t *testing.T) {
	s := &recordingSink{}
	var got struct {
		sync.Mutex
		toDevice int64
	}
	signer, _ := txn.NewSigner("a.example", testKeyLine)
	d := NewDestination(Config{
		Name: "b.example", Limits: Limits{MaxEDUs: 2}, Signer: signer,
		IDs: txn.NewIDGenerator(txn.DefaultIDPrefix), Sink: s, Log: zerolog.New(io.Discard),
		OnEDUsSent: func(_ string, toDevice, _ int64) {
			got.Lock()
			if toDevice > got.toDevice {
				got.toDevice = toDevice
			}
			got.Unlock()
		},
	})

	// Three units; the first transaction can take only two. The mark on the
	// third must not be committed by that transaction.
	d.EnqueueMarkedEDU(EDU{Unit: txn.EDU{Type: "m.direct_to_device", Content: []byte(`{}`)}})
	d.EnqueueMarkedEDU(EDU{Unit: txn.EDU{Type: "m.direct_to_device", Content: []byte(`{}`)},
		ToDeviceUpTo: 10})
	d.EnqueueMarkedEDU(EDU{Unit: txn.EDU{Type: "m.direct_to_device", Content: []byte(`{}`)},
		ToDeviceUpTo: 99})

	d.Attempt(context.Background())
	waitFor(t, "everything to drain", func() bool {
		_, e := d.Pending()
		return e == 0
	})

	got.Lock()
	defer got.Unlock()
	if got.toDevice != 99 {
		t.Errorf("highest committed mark = %d, want 99 once all were delivered", got.toDevice)
	}
}
