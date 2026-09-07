package queue

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

func testManager(t *testing.T, s *recordingSink, maxConcurrent int) *Manager {
	t.Helper()
	signer, err := txn.NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	return NewManager(ManagerConfig{
		Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix), Sink: s,
		Log: zerolog.New(io.Discard), MaxConcurrent: maxConcurrent,
	})
}

// Two queues for one destination would mean two concurrent transactions to it,
// so Get must be safe to race.
func TestGetIsIdempotentUnderConcurrency(t *testing.T) {
	m := testManager(t, &recordingSink{}, 10)

	var wg sync.WaitGroup
	results := make([]*Destination, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = m.Get("b.example")
		}(i)
	}
	wg.Wait()

	for i, d := range results {
		if d != results[0] {
			t.Fatalf("call %d got a different Destination for the same name", i)
		}
	}
	if m.Count() != 1 {
		t.Errorf("Count = %d, want 1", m.Count())
	}
}

func TestManagerFansOutToManyDestinations(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 100)

	const n = 200
	for i := 0; i < n; i++ {
		d := m.Get(destName(i))
		d.EnqueuePDU(pdu("$e", 1))
		m.Wake(d)
	}

	waitFor(t, "every destination to drain", func() bool {
		_, pdus, edus := m.PendingTotals()
		return pdus == 0 && edus == 0
	})
	if m.Count() != n {
		t.Errorf("Count = %d, want %d", m.Count(), n)
	}
	if got := len(s.sent()); got != n {
		t.Errorf("%d transactions were sent, want %d", got, n)
	}
}

// The bound is what stops a slow remote parking every goroutine on the same
// socket timeout. Without it, "one goroutine per destination" is a way to fall
// over rather than a design.
func TestWakeRespectsTheConcurrencyBound(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	m := testManager(t, s, 3)

	for i := 0; i < 50; i++ {
		d := m.Get(destName(i))
		d.EnqueuePDU(pdu("$e", 1))
		m.Wake(d)
	}

	// Let the permitted ones pile up against the block, then check nobody
	// slipped past.
	waitFor(t, "the bound to be reached", func() bool { return s.inFlight.Load() == 3 })
	if got := s.maxInFlight.Load(); got > 3 {
		t.Fatalf("%d sends were in flight, over the bound of 3", got)
	}

	close(s.block)
	waitFor(t, "everything to drain", func() bool {
		_, pdus, _ := m.PendingTotals()
		return pdus == 0
	})
	if got := s.maxInFlight.Load(); got > 3 {
		t.Errorf("%d sends were in flight at the peak, over the bound of 3", got)
	}
}

// Wake must not block its caller: fanning one event out to a thousand
// destinations cannot wait on whichever is slowest.
func TestWakeDoesNotBlockTheCaller(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	defer close(s.block)
	m := testManager(t, s, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			d := m.Get(destName(i))
			d.EnqueuePDU(pdu("$e", 1))
			m.Wake(d)
		}
	}()

	select {
	case <-done:
	case <-timeoutAfterSeconds(5):
		t.Fatal("Wake blocked the caller while a send was stuck")
	}
}

// A cancelled base context must not leave a destination marked as sending, or
// it would never send again -- a deadlock of one server, which on a homeserver
// talking to thousands would go unnoticed.
func TestCancelledWakeReleasesTheClaim(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	defer close(s.block)
	m := testManager(t, s, 1)

	// Fill the single slot with a send that will not finish.
	blocker := m.Get("blocker.example")
	blocker.EnqueuePDU(pdu("$e", 1))
	m.Wake(blocker)
	waitFor(t, "the slot to be taken", func() bool { return s.inFlight.Load() == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	d := m.Get("b.example")
	d.EnqueuePDU(pdu("$e", 1))
	m.Wake(d)
	cancel()

	waitFor(t, "the claim to be released", func() bool { return !d.isRunning() })
}

func TestPendingTotals(t *testing.T) {
	m := testManager(t, &recordingSink{}, 10)
	a, b := m.Get("a.example"), m.Get("b.example")
	a.EnqueuePDU(pdu("$1", 1))
	a.EnqueuePDU(pdu("$2", 2))
	b.EnqueueEDU(txn.EDU{Type: "m.receipt"})
	m.Get("idle.example")

	dests, pdus, edus := m.PendingTotals()
	if dests != 2 || pdus != 2 || edus != 1 {
		t.Errorf("PendingTotals = (%d, %d, %d), want (2, 2, 1)", dests, pdus, edus)
	}
	if m.Count() != 3 {
		t.Errorf("Count = %d, want 3 including the idle one", m.Count())
	}
}

func destName(i int) string {
	return string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + ".example"
}

// The regression test for the bug that a live send found.
//
// Wake used to take the caller's context and run the transmission loop under
// it. The caller is a batch -- an errgroup whose context is cancelled the
// moment the batch finishes -- so a loop spawned near the end of one raced that
// cancellation and, when it lost, returned without sending. Nothing logged it,
// because returning on a cancelled context is exactly what the loop is meant to
// do; the only symptom was a message that never arrived.
//
// Here the caller's context is cancelled IMMEDIATELY after Wake, as a finished
// batch's would be. The transaction must still be sent.
func TestSendSurvivesTheCallersContextEnding(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 8)
	m.Start(context.Background())

	// Exactly the shape of processBatch: an errgroup whose context dies with
	// the batch.
	batchCtx, cancelBatch := context.WithCancel(context.Background())
	d := m.Get("b.example")
	d.EnqueuePDU(pdu("$a", 1))
	m.Wake(d)
	// The batch is over.
	cancelBatch()
	_ = batchCtx

	waitFor(t, "the transaction to be sent anyway", func() bool { return len(s.sent()) == 1 })
	if p, _ := d.Pending(); p != 0 {
		t.Errorf("%d pdus left queued after the batch ended", p)
	}
}

// And the same under repetition, since the original failure was a race that
// sometimes went the right way.
func TestSendSurvivesTheCallersContextEndingRepeatedly(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 16)
	m.Start(context.Background())

	const n = 50
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		d := m.Get(destName(i))
		d.EnqueuePDU(pdu("$e", int64(i+1)))
		m.Wake(d)
		cancel()
		_ = ctx
	}

	waitFor(t, "every destination to drain", func() bool {
		_, pdus, _ := m.PendingTotals()
		return pdus == 0
	})
	if got := len(s.sent()); got != n {
		t.Errorf("%d transactions sent, want %d; the caller's context is still "+
			"reaching the transmission loop", got, n)
	}
}
