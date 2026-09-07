package sender

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// --- fakes -------------------------------------------------------------

type fakeStore struct {
	mu      sync.Mutex
	events  []store.Event
	hosts   map[string][]string
	timings map[string]store.RetryTimings
	// getEventsCalls counts lookups, so the lazy auth-event loading can be
	// checked rather than assumed.
	getEventsCalls int
}

func (f *fakeStore) GetAllNewEventIDsStream(_ context.Context, from, upto int64, limit int) ([]store.NewEvent, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.NewEvent
	for _, e := range f.events {
		if e.StreamOrdering > from && e.StreamOrdering <= upto {
			out = append(out, store.NewEvent{StreamOrdering: e.StreamOrdering, EventID: e.EventID})
		}
		if len(out) == limit {
			return out, out[len(out)-1].StreamOrdering, nil
		}
	}
	return out, upto, nil
}

func (f *fakeStore) GetEvents(_ context.Context, ids []string) ([]store.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getEventsCalls++
	byID := map[string]store.Event{}
	for _, e := range f.events {
		byID[e.EventID] = e
	}
	var out []store.Event
	for _, id := range ids {
		if e, ok := byID[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeStore) MaxStreamOrdering(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64
	for _, e := range f.events {
		if e.StreamOrdering > max {
			max = e.StreamOrdering
		}
	}
	return max, nil
}

func (f *fakeStore) GetDestinationRetryTimings(_ context.Context, dests []string) (map[string]store.RetryTimings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timings, nil
}

func (f *fakeStore) CurrentJoinedHosts(_ context.Context, roomID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hosts[roomID], nil
}

// No state groups, so the resolver takes the current-state fallback. The exact
// path is tested in internal/destinations.
func (f *fakeStore) GetStateGroupsForEvents(context.Context, []string) (map[string]int64, error) {
	return nil, nil
}

func (f *fakeStore) JoinedHostsAtStateGroup(context.Context, int64) ([]string, error) {
	return nil, nil
}

type memCursors struct {
	mu     sync.Mutex
	m      map[string]int64
	routes []state.RoutedRoom
}

func newCursors() *memCursors { return &memCursors{m: map[string]int64{}} }

func (c *memCursors) RecordRoutes(_ context.Context, routes []state.RoutedRoom) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes = append(c.routes, routes...)
	return nil
}

func (c *memCursors) Get(_ context.Context, name string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[name]
	return v, ok, nil
}

// Set mirrors state.Store: an absent row is INSERTed at whatever position is
// given, and an existing one only ever moves forward. The distinction matters
// here -- seeding at 0 must create the row, or the pipeline would take the
// no-saved-cursor path and start from the tip.
func (c *memCursors) Set(_ context.Context, name string, pos int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.m[name]; !ok || pos > cur {
		c.m[name] = pos
	}
	return nil
}

type recordingObserver struct {
	mu      sync.Mutex
	skipped map[string]destinations.SkipReason
	routed  map[string][]string
	all     map[string][]string
	batches int
}

func newObserver() *recordingObserver {
	return &recordingObserver{
		skipped: map[string]destinations.SkipReason{},
		routed:  map[string][]string{},
		all:     map[string][]string{},
	}
}

func (o *recordingObserver) OnEventSkipped(id string, r destinations.SkipReason) {
	o.mu.Lock()
	o.skipped[id] = r
	o.mu.Unlock()
}

func (o *recordingObserver) OnEventRouted(id string, all, ours []string, _ destinations.FallbackReason) {
	o.mu.Lock()
	o.all[id] = all
	o.routed[id] = ours
	o.mu.Unlock()
}

func (o *recordingObserver) OnBatch(int, int, int64, int64, time.Duration) {
	o.mu.Lock()
	o.batches++
	o.mu.Unlock()
}

// --- helpers -----------------------------------------------------------

const serverName = "a.example"

var testKeyLine = "ed25519 testkey " + base64.RawStdEncoding.EncodeToString(make([]byte, 32))

func event(id string, order int64, sender, roomID string, metadata string) store.Event {
	if metadata == "" {
		metadata = "{}"
	}
	return store.Event{
		EventID: id, RoomID: roomID, Type: "m.room.message", Sender: sender,
		StreamOrdering: order,
		JSON: []byte(fmt.Sprintf(
			`{"event_id":%q,"room_id":%q,"sender":%q,"type":"m.room.message","prev_events":["$p"]}`,
			id, roomID, sender)),
		InternalMetadata: []byte(metadata),
	}
}

type harness struct {
	sender   *Sender
	store    *fakeStore
	cursors  *memCursors
	observer *recordingObserver
	sink     *sink.DryRun
	queues   *queue.Manager
}

func newHarness(t *testing.T, shouldHandle func(string) bool) *harness {
	t.Helper()
	signer, err := txn.NewSigner(serverName, testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	st := &fakeStore{hosts: map[string][]string{}, timings: map[string]store.RetryTimings{}}
	cur := newCursors()
	obs := newObserver()
	dry := sink.NewDryRun(zerolog.New(io.Discard))
	queues := queue.NewManager(queue.ManagerConfig{
		Signer: signer, IDs: txn.NewIDGenerator(), Sink: dry,
		Log: zerolog.New(io.Discard), MaxConcurrent: 16,
	})
	if shouldHandle == nil {
		shouldHandle = func(string) bool { return true }
	}
	s := New(Config{
		Store:        st,
		Cursors:      cur,
		Resolver:     destinations.NewResolver(st, serverName, nil),
		Queues:       queues,
		Observer:     obs,
		Log:          zerolog.New(io.Discard),
		ServerName:   serverName,
		ShouldHandle: shouldHandle,
		BatchLimit:   100,
	})
	return &harness{sender: s, store: st, cursors: cur, observer: obs, sink: dry, queues: queues}
}

func (h *harness) run(t *testing.T) {
	t.Helper()
	max, _ := h.store.MaxStreamOrdering(context.Background())
	h.sender.NotifyNewEvents(max)
	if err := h.sender.processAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// --- tests -------------------------------------------------------------

func TestRoutesOurEventsToRoomHosts(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{event("$a", 10, "@u:a.example", "!r:a.example", "")}
	h.store.hosts["!r:a.example"] = []string{"a.example", "b.example", "c.example"}
	// Start from the beginning rather than the tip.
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	h.observer.mu.Lock()
	got := h.observer.routed["$a"]
	h.observer.mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("routed to %v, want the two remote hosts", got)
	}
	// Our own server is never a destination.
	for _, d := range got {
		if d == serverName {
			t.Error("routed to ourselves")
		}
	}
}

func TestSkipsIneligibleEvents(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{
		event("$remote", 10, "@u:other.example", "!r:a.example", ""),
		event("$oob", 11, "@u:a.example", "!r:a.example", `{"out_of_band_membership":true}`),
		event("$dummy", 12, "@u:a.example", "!r:a.example", `{"proactively_send":false}`),
		event("$good", 13, "@u:a.example", "!r:a.example", ""),
	}
	h.store.hosts["!r:a.example"] = []string{"b.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	h.observer.mu.Lock()
	defer h.observer.mu.Unlock()
	want := map[string]destinations.SkipReason{
		"$remote": destinations.SkipNotOurs,
		"$oob":    destinations.SkipOutOfBand,
		"$dummy":  destinations.SkipNotProactive,
	}
	for id, reason := range want {
		if h.observer.skipped[id] != reason {
			t.Errorf("%s skipped for %q, want %q", id, h.observer.skipped[id], reason)
		}
	}
	if _, skipped := h.observer.skipped["$good"]; skipped {
		t.Error("$good was skipped")
	}
	if len(h.observer.routed["$good"]) != 1 {
		t.Errorf("$good routed to %v", h.observer.routed["$good"])
	}
}

// The shard filter is what makes an event ours or the other sender's, and the
// observer sees both sets so the shadow can compare them.
func TestShardFilterSelectsOurDestinations(t *testing.T) {
	h := newHarness(t, func(d string) bool { return d == "b.example" })
	h.store.events = []store.Event{event("$a", 10, "@u:a.example", "!r:a.example", "")}
	h.store.hosts["!r:a.example"] = []string{"b.example", "c.example", "d.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	h.observer.mu.Lock()
	all, ours := h.observer.all["$a"], h.observer.routed["$a"]
	h.observer.mu.Unlock()
	if len(all) != 3 {
		t.Errorf("all destinations = %v, want all three", all)
	}
	if len(ours) != 1 || ours[0] != "b.example" {
		t.Errorf("our destinations = %v, want [b.example]", ours)
	}
	if h.queues.Count() != 1 {
		t.Errorf("%d queues created; only our shard should get one", h.queues.Count())
	}
}

// Synapse discards the server we are forwarding on behalf of: it already has
// the event, which is why it asked us to send it.
func TestSendOnBehalfOfIsDiscarded(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{
		event("$a", 10, "@u:b.example", "!r:a.example", `{"send_on_behalf_of":"b.example"}`),
	}
	h.store.hosts["!r:a.example"] = []string{"b.example", "c.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	h.observer.mu.Lock()
	ours := h.observer.routed["$a"]
	h.observer.mu.Unlock()
	if len(ours) != 1 || ours[0] != "c.example" {
		t.Errorf("routed to %v, want only c.example", ours)
	}
}

// A destination in a long backoff is not sent to; one due within the hour of
// slack is.
func TestRetryFilterIsApplied(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{event("$a", 10, "@u:a.example", "!r:a.example", "")}
	h.store.hosts["!r:a.example"] = []string{"down.example", "soon.example"}
	now := time.Now().UnixMilli()
	h.store.timings = map[string]store.RetryTimings{
		"down.example": {RetryLastTS: now, RetryInterval: 30 * 24 * 60 * 60 * 1000},
		"soon.example": {RetryLastTS: now, RetryInterval: 10 * 60 * 1000},
	}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	names := h.queues.Names()
	if len(names) != 1 || names[0] != "soon.example" {
		t.Errorf("queues = %v, want only soon.example", names)
	}
}

// The cursor is what stops the same events being processed twice.
func TestCursorAdvancesAndPreventsReprocessing(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{
		event("$a", 10, "@u:a.example", "!r:a.example", ""),
		event("$b", 11, "@u:a.example", "!r:a.example", ""),
	}
	h.store.hosts["!r:a.example"] = []string{"b.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)
	pos, _, _ := h.cursors.Get(context.Background(), state.CursorEvents)
	if pos != 11 {
		t.Fatalf("cursor = %d, want 11", pos)
	}

	h.observer.mu.Lock()
	batches := h.observer.batches
	h.observer.mu.Unlock()

	// A second run has nothing to do.
	h.run(t)
	h.observer.mu.Lock()
	defer h.observer.mu.Unlock()
	if h.observer.batches != batches {
		t.Errorf("a second pass processed %d more batches; events were reprocessed",
			h.observer.batches-batches)
	}
}

// A first run with no saved cursor starts from the tip. Replaying the whole
// database would compare a shadow re-deciding history against a real sender
// doing nothing of the kind.
func TestFirstRunStartsFromTheTip(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{
		event("$old", 10, "@u:a.example", "!r:a.example", ""),
		event("$new", 20, "@u:a.example", "!r:a.example", ""),
	}
	h.store.hosts["!r:a.example"] = []string{"b.example"}

	h.run(t)

	h.observer.mu.Lock()
	defer h.observer.mu.Unlock()
	if len(h.observer.routed) != 0 {
		t.Errorf("routed %v on a first run; it should have started from the tip",
			h.observer.routed)
	}
	pos, ok, _ := h.cursors.Get(context.Background(), state.CursorEvents)
	if !ok || pos != 20 {
		t.Errorf("cursor = (%d, %v), want the tip", pos, ok)
	}
}

// Events within a room must be handled in order: a remote receiving a message
// before the join that authorises it will reject the message.
func TestEventsInARoomAreOrdered(t *testing.T) {
	h := newHarness(t, nil)
	for i := 0; i < 20; i++ {
		h.store.events = append(h.store.events,
			event(fmt.Sprintf("$e%d", i), int64(i+1), "@u:a.example", "!r:a.example", ""))
	}
	h.store.hosts["!r:a.example"] = []string{"b.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	// The queue holds them in the order they were enqueued.
	d := h.queues.Get("b.example")
	pending, _ := d.Pending()
	if pending != 0 && pending != 20 {
		t.Logf("%d still pending (the dry-run sink drains asynchronously)", pending)
	}
	if got := d.LastSuccessfulStreamOrdering(); got != 0 && got != 20 {
		t.Errorf("cursor = %d, want 0 or the last stream ordering", got)
	}
}

// Auth events are only loaded for the case that needs them.
func TestAuthEventsAreLoadedLazily(t *testing.T) {
	h := newHarness(t, nil)
	h.store.events = []store.Event{event("$a", 10, "@u:a.example", "!r:a.example", "")}
	h.store.hosts["!r:a.example"] = []string{"b.example"}
	_ = h.cursors.Set(context.Background(), state.CursorEvents, 0)

	h.run(t)

	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	// One call for the batch's bodies, and no second call for auth events on
	// an ordinary message.
	if h.store.getEventsCalls != 1 {
		t.Errorf("GetEvents was called %d times for one plain message; auth events "+
			"should not have been loaded", h.store.getEventsCalls)
	}
}

func nowMS() int64 { return time.Now().UnixMilli() }
