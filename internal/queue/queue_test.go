package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

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
	// err, when set, is returned instead of succeeding. Used to inject a rate
	// limit, which the queue must classify differently from a failure.
	err error
}

// fakeRateLimit stands in for what the sink returns on a 429. The queue asks
// sink.IsRateLimited about it, so it has to satisfy that check.
type fakeRateLimit struct{ after time.Duration }

func (e *fakeRateLimit) Error() string             { return "429 Too Many Requests" }
func (e *fakeRateLimit) RetryAfter() time.Duration { return e.after }

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
	if r.err != nil {
		return sink.Result{}, r.err
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

// The gate Synapse checks at the top of its transmission loop.
//
// Without it the backoff only limits how often a dead server is ENQUEUED for,
// not how often it is dialled -- so a busy room keeps a permanently gone
// server under continuous connection attempts, which is wasted work here and
// unsolicited traffic at the other end.
func TestBackingOffDestinationIsNotDialled(t *testing.T) {
	s := &recordingSink{}
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	allowed := false
	d := NewDestination(Config{
		Name: "dead.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		Due: func(string) bool { return allowed },
	})

	for i := 0; i < 20; i++ {
		d.EnqueuePDU(pdu("$e", int64(i+1)))
		d.Attempt(context.Background())
	}
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	if got := len(s.sent()); got != 0 {
		t.Fatalf("%d transactions were sent to a destination that is backing off", got)
	}
	// The work is not lost -- it is what the next attempt and catch-up send.
	if p, _ := d.Pending(); p != 20 {
		t.Errorf("%d pdus queued, want all 20 still there", p)
	}

	// And once it is due again, the queued work goes.
	allowed = true
	d.Attempt(context.Background())
	waitFor(t, "the drain once due", func() bool {
		p, _ := d.Pending()
		return p == 0
	})
}

// The two mutable EDU shapes are rewritten in place while a transaction is
// being built: a keyed EDU is clobbered by the next update for the same key,
// and a receipt EDU has receipts merged into its nested maps. Building the
// transaction outside the lock from a slice that still aliased the queue meant
// reading those as they were rewritten -- a data race for the keyed case, and a
// concurrent map read and write for receipts, which is a fatal runtime error
// rather than merely a wrong answer.
//
// Only meaningful under -race, which CI runs. It caught the real thing.
func TestConcurrentEnqueueDuringSend(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	d := m.Get("b.example")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Keyed EDUs for one key: every update after the first clobbers in place.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			typing := i%2 == 0
			d.EnqueueKeyedEDU("!r|@alice:a.example", txn.EDU{
				Type:    txn.EDUTypeTyping,
				Content: []byte(`{"room_id":"!r","user_id":"@alice:a.example","typing":` + boolText(typing) + `}`),
			})
			m.Wake(d)
		}
	}()

	// Receipts for one room: these merge into the maps materialise walks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			d.EnqueueReceipt("!r:a.example", "m.read",
				"@bob:a.example", "", []byte(`{"ts":1}`))
			m.Wake(d)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if len(s.sent()) == 0 {
		t.Error("nothing was sent; the test exercised nothing")
	}
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// A destination in a real outage must not hold its ephemeral EDUs forever.
// Observed in production: 5,265 backing-off destinations between them held
// 11,537 queued EDUs, having nearly doubled in five minutes, because presence
// and receipts for a dead server accumulate for as long as it stays dead.
// Synapse drops them at this exact point (per_destination_queue.py:423).
func TestLongOutageDropsEphemeralEDUs(t *testing.T) {
	s := &recordingSink{}
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	var dropped int
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		Due:           func(string) bool { return false },
		LongBackoff:   func(string) bool { return true },
		OnEDUsDropped: func(_ string, n int) { dropped = n },
	})

	// Two we can afford to lose, and two that are a cursor into a table.
	d.EnqueueKeyedEDU("!r|@a:x", txn.EDU{Type: txn.EDUTypeTyping, Content: []byte(`{}`)})
	d.EnqueueEDU(txn.EDU{Type: txn.EDUTypeReceipt, Content: []byte(`{}`)})
	d.EnqueueMarkedEDU(EDU{
		Unit: txn.EDU{Type: "m.direct_to_device", Content: []byte(`{}`)}, ToDeviceUpTo: 7,
	})
	d.EnqueueMarkedEDU(EDU{
		Unit: txn.EDU{Type: "m.device_list_update", Content: []byte(`{}`)}, DeviceListUpTo: 9,
	})
	d.EnqueuePDU(pdu("$a", 1))

	d.Attempt(context.Background())
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	if dropped != 2 {
		t.Errorf("dropped = %d, want the 2 ephemeral EDUs", dropped)
	}
	pdus, edus := d.Pending()
	if edus != 2 {
		t.Errorf("%d EDUs left, want the 2 carrying a durable mark", edus)
	}
	// The PDUs go through catch-up instead, which is Synapse's
	// _start_catching_up: they are recoverable from destination_rooms.
	if pdus != 0 {
		t.Errorf("%d PDUs left, want them handed to catch-up", pdus)
	}
	if !d.CatchingUp() {
		t.Error("the destination was not put into catch-up")
	}
}

// A short backoff is a blip. Dropping there would throw away receipts for a
// server that is about to answer.
func TestShortBackoffKeepsEphemeralEDUs(t *testing.T) {
	s := &recordingSink{}
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		Due:         func(string) bool { return false },
		LongBackoff: func(string) bool { return false },
	})
	d.EnqueueEDU(txn.EDU{Type: txn.EDUTypeReceipt, Content: []byte(`{}`)})
	d.EnqueuePDU(pdu("$a", 1))

	d.Attempt(context.Background())
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	pdus, edus := d.Pending()
	if edus != 1 || pdus != 1 {
		t.Errorf("pending = %d pdus %d edus, want everything kept on a short backoff", pdus, edus)
	}
}

// One outage, one log line. Every enqueue wakes the loop, so a busy room drops
// one EDU at a time; logging each would print thousands of identical lines
// about one dead server. Observed in production doing exactly that.
func TestLongOutageLogsOncePerOutage(t *testing.T) {
	var lines int
	// Info level, as the worker runs: the debug "not attempting" line is
	// per-attempt by design and is not what this is about.
	w := zerolog.New(funcWriter(func(p []byte) (int, error) {
		lines++
		return len(p), nil
	})).Level(zerolog.InfoLevel)
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	due := false
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: &recordingSink{}, Log: w,
		Due:         func(string) bool { return due },
		LongBackoff: func(string) bool { return true },
	})

	for i := 0; i < 5; i++ {
		d.EnqueueEDU(txn.EDU{Type: txn.EDUTypeReceipt, Content: []byte(`{}`)})
		d.Attempt(context.Background())
		waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })
	}
	if lines != 1 {
		t.Errorf("%d log lines for one outage, want 1", lines)
	}

	// A new outage after the destination has been reachable again gets its own
	// line, or a server that flaps for a week goes unreported.
	due = true
	d.Attempt(context.Background())
	waitFor(t, "the successful pass", func() bool { return !d.isRunning() })
	before := lines

	due = false
	d.EnqueueEDU(txn.EDU{Type: txn.EDUTypeReceipt, Content: []byte(`{}`)})
	d.Attempt(context.Background())
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })
	if lines != before+1 {
		t.Errorf("a second outage produced %d new lines, want 1", lines-before)
	}
}

type funcWriter func(p []byte) (int, error)

func (f funcWriter) Write(p []byte) (int, error) { return f(p) }

// batchDestination builds a destination that holds presence.
func batchDestination(t *testing.T, s sink.Sink, b BatchConfig) *Destination {
	t.Helper()
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard), Batch: b,
	})
	d.wake = func() { d.Attempt(context.Background()) }
	return d
}

func presenceOf(userID string) EDU {
	return EDU{
		Unit: txn.EDU{Type: txn.EDUTypePresence},
		Presence: map[string]PresenceEntry{userID: {
			UserID: userID,
			Encode: func(int64) json.RawMessage {
				return json.RawMessage(`{"user_id":"` + userID + `"}`)
			},
		}},
	}
}

// Presence alone waits, so that a second state has a chance to arrive and merge
// into the same EDU. Without the hold the queue drains on every enqueue and
// nothing ever accumulates -- which is exactly what happened in production:
// 516 states in 516 EDUs, a ratio of 1.000.
func TestPresenceIsHeldToAccumulate(t *testing.T) {
	s := &recordingSink{}
	d := batchDestination(t, s, BatchConfig{MaxStates: 50, MaxWait: time.Minute})

	d.EnqueuePresence(presenceOf("@a:x").Presence["@a:x"])
	d.Attempt(context.Background())
	waitFor(t, "the loop to settle", func() bool { return !d.isRunning() })

	if n := len(s.sent()); n != 0 {
		t.Errorf("sent %d transactions, want presence held back", n)
	}
	if _, edus := d.Pending(); edus != 1 {
		t.Errorf("%d EDUs pending, want the held one", edus)
	}
}

// A PDU is what a user is waiting for. It flushes immediately and takes any
// held presence with it, which is where a good part of the batching comes from
// at no latency cost.
func TestPDUFlushesHeldPresence(t *testing.T) {
	s := &recordingSink{}
	d := batchDestination(t, s, BatchConfig{MaxStates: 50, MaxWait: time.Minute})

	d.EnqueuePresence(presenceOf("@a:x").Presence["@a:x"])
	d.Attempt(context.Background())
	waitFor(t, "the hold", func() bool { return !d.isRunning() })

	d.EnqueuePDU(pdu("$e", 1))
	d.Attempt(context.Background())
	waitFor(t, "the flush", func() bool {
		p, e := d.Pending()
		return p == 0 && e == 0
	})
	if n := len(s.sent()); n != 1 {
		t.Fatalf("sent %d transactions, want one carrying both", n)
	}
}

// Typing and receipts are excluded from batching by design: typing is about
// this instant and a remote expires it after a minute, and a late read receipt
// defeats its purpose. Neither needs its own code path -- being in the queue is
// enough, because anything that is not presence flushes the whole transaction.
func TestTypingAndReceiptsAreNotHeld(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enqueue func(*Destination)
	}{
		{"typing", func(d *Destination) {
			d.EnqueueKeyedEDU("!r|@a:x", txn.EDU{
				Type: txn.EDUTypeTyping, Content: json.RawMessage(`{}`)})
		}},
		{"receipt", func(d *Destination) {
			d.EnqueueReceipt("!r:x", "m.read", "@a:x", "", json.RawMessage(`{"ts":1}`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &recordingSink{}
			d := batchDestination(t, s, BatchConfig{MaxStates: 50, MaxWait: time.Minute})

			d.EnqueuePresence(presenceOf("@held:x").Presence["@held:x"])
			d.Attempt(context.Background())
			waitFor(t, "the hold", func() bool { return !d.isRunning() })

			tc.enqueue(d)
			d.Attempt(context.Background())
			waitFor(t, "the flush", func() bool {
				_, e := d.Pending()
				return e == 0
			})
			if n := len(s.sent()); n != 1 {
				t.Errorf("sent %d transactions, want %s to have flushed at once", n, tc.name)
			}
		})
	}
}

// Enough states flush without waiting for the deadline.
func TestPresenceFlushesAtTheStateLimit(t *testing.T) {
	s := &recordingSink{}
	d := batchDestination(t, s, BatchConfig{MaxStates: 3, MaxWait: time.Hour})

	for _, u := range []string{"@a:x", "@b:x", "@c:x"} {
		d.EnqueuePresence(presenceOf(u).Presence[u])
		d.Attempt(context.Background())
	}
	waitFor(t, "the flush at the limit", func() bool {
		_, e := d.Pending()
		return e == 0
	})
	if n := len(s.sent()); n != 1 {
		t.Fatalf("sent %d transactions, want one holding all three", n)
	}
	body := s.sent()[0].Body
	if n := len(gjson.GetBytes(body, "edus").Array()); n != 1 {
		t.Errorf("%d EDUs in the transaction, want the three states merged into one", n)
	}
	if n := len(gjson.GetBytes(body, "edus.0.content.push").Array()); n != 3 {
		t.Errorf("%d states in the EDU, want 3", n)
	}
}

// The deadline must fire on its own. A destination holding presence with
// nothing further arriving would otherwise wait forever: the loop only runs
// when something wakes it.
func TestHeldPresenceIsSentWhenTheDeadlinePasses(t *testing.T) {
	s := &recordingSink{}
	d := batchDestination(t, s, BatchConfig{MaxStates: 50, MaxWait: 40 * time.Millisecond})

	d.EnqueuePresence(presenceOf("@a:x").Presence["@a:x"])
	d.Attempt(context.Background())

	waitFor(t, "the deadline to flush it", func() bool { return len(s.sent()) == 1 })
	if _, e := d.Pending(); e != 0 {
		t.Errorf("%d EDUs still pending after the deadline", e)
	}
}

// Zero disables it, which must mean "send immediately" and not "hold forever".
func TestBatchingDisabledSendsImmediately(t *testing.T) {
	s := &recordingSink{}
	d := batchDestination(t, s, BatchConfig{})

	d.EnqueuePresence(presenceOf("@a:x").Presence["@a:x"])
	d.Attempt(context.Background())
	waitFor(t, "the send", func() bool { return len(s.sent()) == 1 })
}

// Exactly one transaction per destination at a time, INCLUDING catch-up.
//
// Catch-up runs on its own goroutine and used to call send directly, so it went
// out alongside the live transmission loop. That puts PDUs on the wire out of
// order and makes a remote answer 429 "still processing another transaction
// from this origin" -- which was then recorded as a delivery failure, so this
// worker grew a live server's backoff because it was talking over itself.
func TestCatchUpDoesNotSendAlongsideTheLoop(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	d := testDestination(t, s, Limits{})

	// Occupy the destination with a live send.
	d.EnqueuePDU(pdu("$live", 1))
	d.Attempt(context.Background())
	waitFor(t, "the live send to start", func() bool { return s.inFlight.Load() == 1 })

	// Catch-up must refuse rather than send alongside.
	//
	// Bounded, because the failure mode without the claim is not a wrong
	// answer: SendCatchUp calls straight through to the blocked sink and hangs.
	// A test that hangs in CI is worse than one that fails, so the deadline
	// turns it back into a failure.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := d.SendCatchUp(ctx, pdu("$catchup", 2))
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("SendCatchUp = %v, want ErrBusy while the loop is sending "+
			"(a context error here means it sent alongside and blocked)", err)
	}
	if got := s.maxInFlight.Load(); got > 1 {
		t.Errorf("%d transactions in flight at once, want at most 1", got)
	}

	close(s.block)
	waitFor(t, "the loop to finish", func() bool { return !d.isRunning() })

	// And once the loop is done, catch-up may proceed.
	if err := d.SendCatchUp(context.Background(), pdu("$catchup", 2)); err != nil {
		t.Errorf("SendCatchUp after the loop finished = %v", err)
	}
}

// ErrBusy must not be reported as a delivery failure, or this worker grows a
// destination's backoff because of its own scheduling -- capped at a year on
// this deployment.
func TestBusyCatchUpIsNotAFailure(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	var outcomes []bool
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		OnOutcome: func(_ string, ok bool) { outcomes = append(outcomes, ok) },
	})

	d.EnqueuePDU(pdu("$live", 1))
	d.Attempt(context.Background())
	waitFor(t, "the live send to start", func() bool { return s.inFlight.Load() == 1 })

	busyCtx, busyCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer busyCancel()
	if err := d.SendCatchUp(busyCtx, pdu("$catchup", 2)); !errors.Is(err, ErrBusy) {
		t.Fatalf("SendCatchUp = %v, want ErrBusy", err)
	}
	for _, ok := range outcomes {
		if !ok {
			t.Error("a busy destination was reported as a delivery failure")
		}
	}
	close(s.block)
	waitFor(t, "the loop to finish", func() bool { return !d.isRunning() })
}

// Work queued while catch-up holds the claim must still be sent. Wake gives up
// when the destination is claimed and relies on the running LOOP to go round
// again -- and catch-up is not that loop.
func TestWorkQueuedDuringCatchUpIsNotStranded(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{})
	woken := make(chan struct{}, 1)
	d.wake = func() {
		select {
		case woken <- struct{}{}:
		default:
		}
	}

	// Claim it as catch-up does, queue live work, then release.
	if !d.claim() {
		t.Fatal("could not claim an idle destination")
	}
	d.EnqueuePDU(pdu("$live", 1))
	d.TryStart() // what Wake does: fails, sets newData
	d.releaseClaim()

	select {
	case <-woken:
	default:
		t.Error("work queued during catch-up did not re-wake the destination")
	}
}

// A 429 must reach OnRateLimited, not OnOutcome. The distinction is the whole
// policy: the host is up and answering, so throttle ourselves rather than write
// the destination off.
func TestRateLimitIsNotReportedAsAFailure(t *testing.T) {
	s := &recordingSink{}
	s.err = &fakeRateLimit{after: 1500 * time.Millisecond}

	var outcomes []bool
	var limitedAfter time.Duration
	var limitedCount int
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		OnOutcome:     func(_ string, ok bool) { outcomes = append(outcomes, ok) },
		OnRateLimited: func(_ string, after time.Duration) { limitedCount++; limitedAfter = after },
	})

	d.EnqueuePDU(pdu("$a", 1))
	d.Attempt(context.Background())
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	if limitedCount == 0 {
		t.Error("a 429 was not reported as rate limiting")
	}
	if limitedAfter != 1500*time.Millisecond {
		t.Errorf("after = %v, want the remote's hint carried through", limitedAfter)
	}
	for _, ok := range outcomes {
		if !ok {
			t.Error("a 429 was ALSO reported as a delivery failure; it would back off the host")
		}
	}
	// And nothing was dequeued, so the transaction is retried later.
	if p, _ := d.Pending(); p != 1 {
		t.Errorf("%d PDUs pending, want the transaction still queued", p)
	}
}

// Everything that is not a 429 still reports a failure, so a host with problems
// is still backed off.
func TestOrdinaryFailuresStillBackOff(t *testing.T) {
	s := &recordingSink{}
	s.fail.Store(true)

	var sawFailure bool
	var limited int
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDestination(Config{
		Name: "b.example", Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink: s, Log: zerolog.New(io.Discard),
		OnOutcome:     func(_ string, ok bool) { sawFailure = sawFailure || !ok },
		OnRateLimited: func(string, time.Duration) { limited++ },
	})

	d.EnqueuePDU(pdu("$a", 1))
	d.Attempt(context.Background())
	waitFor(t, "the loop to give up", func() bool { return !d.isRunning() })

	if !sawFailure {
		t.Error("an ordinary failure was not reported as one")
	}
	if limited != 0 {
		t.Error("an ordinary failure was reported as rate limiting")
	}
}
