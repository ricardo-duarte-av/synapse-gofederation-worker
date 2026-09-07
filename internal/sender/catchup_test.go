package sender

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

type fakeCatchUpStore struct {
	mu sync.Mutex
	// owed maps destination to the event ids it is behind on, in stream order.
	owed map[string][]store.Event
	// lastSuccessful maps destination to its cursor; absent means never
	// delivered, which is a different thing from zero.
	lastSuccessful map[string]int64
	outstanding    []string
}

func (f *fakeCatchUpStore) GetDestinationLastSuccessfulStreamOrdering(_ context.Context, d string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.lastSuccessful[d]
	return v, ok, nil
}

func (f *fakeCatchUpStore) GetCatchUpRoomEventIDs(_ context.Context, d string, from int64, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, e := range f.owed[d] {
		if e.StreamOrdering > from {
			out = append(out, e.EventID)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeCatchUpStore) GetCatchUpOutstandingDestinations(_ context.Context, after string, _ int64, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, d := range f.outstanding {
		if d > after {
			out = append(out, d)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeCatchUpStore) GetEvents(_ context.Context, ids []string) ([]store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	byID := map[string]store.Event{}
	for _, evs := range f.owed {
		for _, e := range evs {
			byID[e.EventID] = e
		}
	}
	var out []store.Event
	for _, id := range ids {
		if e, ok := byID[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func catchUpHarness(t *testing.T, st *fakeCatchUpStore) (*CatchUp, *queue.Manager, *recordingCatchUpSink) {
	t.Helper()
	signer, err := txn.NewSigner(serverName, testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	s := &recordingCatchUpSink{}
	m := queue.NewManager(queue.ManagerConfig{
		Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix), Sink: s,
		Log: zerolog.New(io.Discard), MaxConcurrent: 8,
	})
	m.Start(context.Background())
	c := NewCatchUp(CatchUpConfig{
		Store: st, Queues: m, Log: zerolog.New(io.Discard),
		ShouldHandle: func(string) bool { return true },
	})
	return c, m, s
}

type recordingCatchUpSink struct {
	mu   sync.Mutex
	sent []string
}

func (r *recordingCatchUpSink) Mode() string { return "catchup-test" }
func (r *recordingCatchUpSink) Send(_ context.Context, req *txn.Request) (sink.Result, error) {
	r.mu.Lock()
	r.sent = append(r.sent, string(req.Body))
	r.mu.Unlock()
	return sink.Result{Delivered: true}, nil
}
func (r *recordingCatchUpSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

func owedEvent(id string, order int64) store.Event {
	return store.Event{
		EventID: id, RoomID: fmt.Sprintf("!r%d:a.example", order), StreamOrdering: order,
		JSON: []byte(fmt.Sprintf(`{"event_id":%q,"type":"m.room.message","depth":1}`, id)),
	}
}

// The whole point: what a destination missed while the sender was down exists
// only in destination_rooms, and nothing on the live stream will ever mention
// it again.
func TestCatchUpReplaysWhatWasMissed(t *testing.T) {
	st := &fakeCatchUpStore{
		owed: map[string][]store.Event{"b.example": {
			owedEvent("$a", 10), owedEvent("$b", 20), owedEvent("$c", 30),
		}},
		lastSuccessful: map[string]int64{"b.example": 5},
	}
	c, m, s := catchUpHarness(t, st)

	if err := c.Destination(context.Background(), "b.example"); err != nil {
		t.Fatal(err)
	}
	if got := s.count(); got != 3 {
		t.Errorf("%d catch-up transactions, want one per owed room", got)
	}
	d := m.Get("b.example")
	if d.CatchingUp() {
		t.Error("still catching up after replaying everything owed")
	}
	if got := d.LastSuccessfulStreamOrdering(); got != 30 {
		t.Errorf("cursor = %d, want the last replayed ordering", got)
	}
}

// A destination that has never had a successful transaction has no point to
// catch up FROM, and replaying a room's history to it would be wrong.
func TestNeverDeliveredDestinationDoesNotReplayHistory(t *testing.T) {
	st := &fakeCatchUpStore{
		owed:           map[string][]store.Event{"new.example": {owedEvent("$a", 10)}},
		lastSuccessful: map[string]int64{}, // absent, not zero
	}
	c, m, s := catchUpHarness(t, st)

	if err := c.Destination(context.Background(), "new.example"); err != nil {
		t.Fatal(err)
	}
	if got := s.count(); got != 0 {
		t.Errorf("%d transactions sent to a destination with no recorded success", got)
	}
	if m.Get("new.example").CatchingUp() {
		t.Error("a destination with nothing to catch up on is still catching up")
	}
}

// While catching up, live events are DROPPED rather than queued -- sending them
// would deliver recent events ahead of everything older. The durable record is
// what catch-up reads, so nothing is lost.
func TestLiveEventsAreDroppedWhileCatchingUp(t *testing.T) {
	_, m, s := catchUpHarness(t, &fakeCatchUpStore{})
	d := m.Get("b.example")
	d.SetLastSuccessfulStreamOrdering(5)

	for i := 0; i < 10; i++ {
		d.EnqueuePDU(queue.PDU{EventID: "$live", StreamOrdering: int64(100 + i),
			JSON: []byte(`{"type":"m.room.message"}`)})
	}
	if p, _ := d.Pending(); p != 0 {
		t.Errorf("%d PDUs queued while catching up, want them dropped for catch-up to find", p)
	}
	// The newest dropped ordering is remembered, which is what closes the race
	// between the last catch-up page and the decision that it is finished.
	if got := d.TakeCatchUpSkipped(); got != 109 {
		t.Errorf("skipped high-water = %d, want 109", got)
	}
	// Taking it clears it.
	if got := d.TakeCatchUpSkipped(); got != 0 {
		t.Errorf("skipped mark was not cleared: %d", got)
	}

	// Once caught up, live events queue normally again.
	d.FinishCatchUp()
	d.EnqueuePDU(queue.PDU{EventID: "$after", StreamOrdering: 200,
		JSON: []byte(`{"type":"m.room.message"}`)})
	if p, _ := d.Pending(); p != 1 {
		t.Errorf("%d PDUs queued after catch-up finished, want 1", p)
	}
	_ = s
}

// An event arriving between the last catch-up page and the decision that
// catch-up is done belongs to neither path unless this race is closed.
func TestSkippedEventReopensCatchUp(t *testing.T) {
	st := &fakeCatchUpStore{
		owed:           map[string][]store.Event{"b.example": {}},
		lastSuccessful: map[string]int64{"b.example": 5},
	}
	c, m, _ := catchUpHarness(t, st)
	d := m.Get("b.example")
	d.SetLastSuccessfulStreamOrdering(5)

	// An event was dropped while catching up, newer than the cursor.
	d.EnqueuePDU(queue.PDU{EventID: "$raced", StreamOrdering: 50,
		JSON: []byte(`{"type":"m.room.message"}`)})

	// With nothing owed, one pass would declare it finished -- but the skipped
	// mark forces another look, during which the row appears.
	st.mu.Lock()
	st.owed["b.example"] = []store.Event{owedEvent("$raced", 50)}
	st.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Destination(ctx, "b.example"); err != nil {
		t.Fatal(err)
	}
	if d.CatchingUp() {
		t.Error("still catching up")
	}
	if got := d.LastSuccessfulStreamOrdering(); got != 50 {
		t.Errorf("cursor = %d, want the raced event replayed", got)
	}
}
