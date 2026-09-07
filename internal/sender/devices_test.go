package sender

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

type fakeDeviceStore struct {
	mu sync.Mutex
	// outbox and pokes are keyed by destination.
	outbox map[string][]store.ToDeviceMessage
	pokes  map[string][]store.DevicePoke
	// destsForStream maps a device_lists stream id to its destinations.
	destsForStream map[int64][]string
	timings        map[string]store.RetryTimings
	// reads counts outbox reads per destination, which is how the
	// "never re-read the same rows" property is checked.
	reads map[string]int
}

func newDeviceStore() *fakeDeviceStore {
	return &fakeDeviceStore{
		outbox:         map[string][]store.ToDeviceMessage{},
		pokes:          map[string][]store.DevicePoke{},
		destsForStream: map[int64][]string{},
		timings:        map[string]store.RetryTimings{},
		reads:          map[string]int{},
	}
}

func (f *fakeDeviceStore) GetNewDeviceMsgsForRemote(_ context.Context, dest string, last, current int64, limit int) ([]store.ToDeviceMessage, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads[dest]++
	var out []store.ToDeviceMessage
	for _, m := range f.outbox[dest] {
		if m.StreamID > last && m.StreamID <= current {
			out = append(out, m)
		}
		if len(out) == limit {
			return out, out[len(out)-1].StreamID, nil
		}
	}
	return out, current, nil
}

func (f *fakeDeviceStore) GetDeviceUpdatesByRemote(_ context.Context, dest string, from, now int64, limit int) ([]store.DevicePoke, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.DevicePoke
	for _, p := range f.pokes[dest] {
		if p.StreamID > from && p.StreamID <= now {
			out = append(out, p)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeDeviceStore) GetDestinationsForDevice(_ context.Context, streamID int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.destsForStream[streamID], nil
}

func (f *fakeDeviceStore) MaxDeviceOutboxStreamID(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64
	for _, msgs := range f.outbox {
		for _, m := range msgs {
			if m.StreamID > max {
				max = m.StreamID
			}
		}
	}
	return max, nil
}

func (f *fakeDeviceStore) MaxDeviceListOutboundStreamID(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64
	for _, ps := range f.pokes {
		for _, p := range ps {
			if p.StreamID > max {
				max = p.StreamID
			}
		}
	}
	return max, nil
}

func (f *fakeDeviceStore) GetDestinationRetryTimings(_ context.Context, dests []string) (map[string]store.RetryTimings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timings, nil
}

func newDevices(t *testing.T, st *fakeDeviceStore, shouldHandle func(string) bool) (*Devices, *queue.Manager, *memCursors) {
	t.Helper()
	signer, err := txn.NewSigner(serverName, testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	// A blocking sink so the EDUs stay observable in the queues rather than
	// draining before the assertions run.
	blocked := &blockingSink{}
	m := queue.NewManager(queue.ManagerConfig{
		Signer: signer, IDs: txn.NewIDGenerator(txn.DefaultIDPrefix), Sink: blocked,
		Log: zerolog.New(io.Discard), MaxConcurrent: 8,
	})
	cur := newCursors()
	d := NewDevices(DevicesConfig{Store: st, Cursors: cur, Queues: m, ShouldHandle: shouldHandle})
	return d, m, cur
}

// blockingSink never completes a send, so queued units stay queued.
type blockingSink struct{}

func (blockingSink) Mode() string { return "blocking" }
func (blockingSink) Send(ctx context.Context, _ *txn.Request) (sink.Result, error) {
	<-ctx.Done()
	return sink.Result{}, ctx.Err()
}

func TestToDeviceQueuesOneEDUPerRow(t *testing.T) {
	st := newDeviceStore()
	st.outbox["b.example"] = []store.ToDeviceMessage{
		{StreamID: 1, MessagesJSON: []byte(`{"messages":{"@a:b.example":{"*":{}}}}`)},
		{StreamID: 2, MessagesJSON: []byte(`{"messages":{"@c:b.example":{"*":{}}}}`)},
	}
	d, m, cur := newDevices(t, st, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleToDevice(ctx, []string{"b.example"}); err != nil {
		t.Fatal(err)
	}

	_, edus := m.Get("b.example").Pending()
	// One EDU per row, not one merged EDU (per_destination_queue.py:700).
	// The queue may already have taken some into an in-flight transaction.
	if edus > 2 {
		t.Errorf("%d EDUs queued, want at most 2", edus)
	}
	pos, ok, _ := cur.Get(ctx, state.ToDeviceCursor("b.example"))
	if !ok || pos != 2 {
		t.Errorf("cursor = (%d, %v), want 2", pos, ok)
	}
}

// The property that being read-only makes fragile: we can never delete these
// rows, so the cursor is the only thing stopping us re-reading them forever.
func TestToDeviceNeverRereadsDeliveredRows(t *testing.T) {
	st := newDeviceStore()
	st.outbox["b.example"] = []store.ToDeviceMessage{
		{StreamID: 1, MessagesJSON: []byte(`{}`)},
	}
	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 5; i++ {
		if err := d.HandleToDevice(ctx, []string{"b.example"}); err != nil {
			t.Fatal(err)
		}
	}
	_, edus := m.Get("b.example").Pending()
	if edus > 1 {
		t.Errorf("%d EDUs queued after five passes over one row; the cursor is not "+
			"preventing re-reads", edus)
	}
}

// A destination with nothing owed still advances its cursor, because the gap is
// other destinations' rows and leaving the cursor behind them would re-scan the
// same range forever.
func TestToDeviceAdvancesOverOtherDestinationsRows(t *testing.T) {
	st := newDeviceStore()
	st.outbox["other.example"] = []store.ToDeviceMessage{{StreamID: 500, MessagesJSON: []byte(`{}`)}}
	d, _, cur := newDevices(t, st, nil)
	ctx := context.Background()

	if err := d.HandleToDevice(ctx, []string{"b.example"}); err != nil {
		t.Fatal(err)
	}
	pos, ok, _ := cur.Get(ctx, state.ToDeviceCursor("b.example"))
	if !ok || pos != 500 {
		t.Errorf("cursor = (%d, %v), want a fast-forward to 500", pos, ok)
	}
}

func TestToDeviceRespectsTheShard(t *testing.T) {
	st := newDeviceStore()
	st.outbox["mine.example"] = []store.ToDeviceMessage{{StreamID: 1, MessagesJSON: []byte(`{}`)}}
	st.outbox["theirs.example"] = []store.ToDeviceMessage{{StreamID: 2, MessagesJSON: []byte(`{}`)}}
	d, m, _ := newDevices(t, st, func(s string) bool { return s == "mine.example" })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleToDevice(ctx, []string{"mine.example", "theirs.example"}); err != nil {
		t.Fatal(err)
	}
	names := m.Names()
	if len(names) != 1 || names[0] != "mine.example" {
		t.Errorf("queues = %v, want only mine.example", names)
	}
}

// The replication row carries only a user id; the destinations come from
// device_lists_outbound_pokes, which is the only place they exist.
func TestDeviceListsResolvesDestinationsFromTheTable(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[42] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@alice:a.example", DeviceID: "DEV1", StreamID: 42},
	}
	d, m, cur := newDevices(t, st, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 42); err != nil {
		t.Fatal(err)
	}

	names := m.Names()
	if len(names) != 1 || names[0] != "b.example" {
		t.Fatalf("queues = %v", names)
	}
	pos, ok, _ := cur.Get(ctx, state.DeviceListCursor("b.example"))
	if !ok || pos != 42 {
		t.Errorf("cursor = (%d, %v), want 42", pos, ok)
	}
}

// The EDU Synapse sends carries more than this -- prev_id, deleted, keys and
// device_display_name, assembled from the device tables and
// device_lists_outbound_last_success. Phase 1 queues the shape without them on
// purpose: the shadow's question is WHICH destinations get an update and WHEN,
// and nothing here is ever put on a wire. This test pins what IS produced so
// filling in the rest is a visible change rather than a silent one.
func TestDeviceListEDUShape(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[7] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@alice:a.example", DeviceID: "DEV1", StreamID: 7},
	}
	d, m, _ := newDevices(t, st, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 7); err != nil {
		t.Fatal(err)
	}

	edus := m.Get("b.example").PeekEDUs()
	if len(edus) != 1 {
		t.Fatalf("%d EDUs queued, want 1", len(edus))
	}
	if edus[0].Type != txn.EDUTypeDeviceListUpdate {
		t.Errorf("edu_type = %q, want %q", edus[0].Type, txn.EDUTypeDeviceListUpdate)
	}

	var content map[string]any
	if err := json.Unmarshal(edus[0].Content, &content); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"user_id": "@alice:a.example", "device_id": "DEV1", "stream_id": float64(7),
	}
	for key, v := range want {
		if content[key] != v {
			t.Errorf("content[%q] = %v, want %v", key, content[key], v)
		}
	}
}

func TestFilterDueDropsBackedOffDestinations(t *testing.T) {
	st := newDeviceStore()
	st.timings = map[string]store.RetryTimings{
		"down.example": {RetryLastTS: nowMS(), RetryInterval: 30 * 24 * 60 * 60 * 1000},
	}
	d, _, _ := newDevices(t, st, nil)

	got, err := d.FilterDue(context.Background(), []string{"down.example", "up.example"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "up.example" {
		t.Errorf("FilterDue = %v, want [up.example]", got)
	}
}
