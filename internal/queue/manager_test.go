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
		Signer: signer, IDs: txn.NewIDGenerator(), Sink: s,
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
	ctx := context.Background()

	const n = 200
	for i := 0; i < n; i++ {
		d := m.Get(destName(i))
		d.EnqueuePDU(pdu("$e", 1))
		m.Wake(ctx, d)
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
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		d := m.Get(destName(i))
		d.EnqueuePDU(pdu("$e", 1))
		m.Wake(ctx, d)
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
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			d := m.Get(destName(i))
			d.EnqueuePDU(pdu("$e", 1))
			m.Wake(ctx, d)
		}
	}()

	select {
	case <-done:
	case <-timeoutAfterSeconds(5):
		t.Fatal("Wake blocked the caller while a send was stuck")
	}
}

// A cancelled context must not leave a destination marked as sending, or it
// would never send again -- a deadlock of one server, which on a homeserver
// talking to thousands would go unnoticed.
func TestCancelledWakeReleasesTheClaim(t *testing.T) {
	s := &recordingSink{block: make(chan struct{})}
	defer close(s.block)
	m := testManager(t, s, 1)

	// Fill the single slot with a send that will not finish.
	blocker := m.Get("blocker.example")
	blocker.EnqueuePDU(pdu("$e", 1))
	m.Wake(context.Background(), blocker)
	waitFor(t, "the slot to be taken", func() bool { return s.inFlight.Load() == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	d := m.Get("b.example")
	d.EnqueuePDU(pdu("$e", 1))
	m.Wake(ctx, d)
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
