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
	// details and lastSuccess back the m.device_list_update body.
	details      map[string]store.DeviceDetail
	lastSuccess  map[string]int64
	crossSigning map[string][]store.CrossSigningKey
}

// readCount is how many times the to-device outbox was read for a destination,
// which is what "the work was skipped" looks like from outside.
func (f *fakeDeviceStore) readCount(dest string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads[dest]
}

func newDeviceStore() *fakeDeviceStore {
	return &fakeDeviceStore{
		outbox:         map[string][]store.ToDeviceMessage{},
		pokes:          map[string][]store.DevicePoke{},
		destsForStream: map[int64][]string{},
		timings:        map[string]store.RetryTimings{},
		reads:          map[string]int{},
		details:        map[string]store.DeviceDetail{},
		lastSuccess:    map[string]int64{},
		crossSigning:   map[string][]store.CrossSigningKey{},
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

func (f *fakeDeviceStore) GetDeviceDetails(_ context.Context, users, devices []string) (map[string]store.DeviceDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]store.DeviceDetail{}
	for i := range users {
		k := store.DeviceDetailKey(users[i], devices[i])
		if d, ok := f.details[k]; ok {
			out[k] = d
		}
	}
	return out, nil
}

func (f *fakeDeviceStore) GetLastDeviceUpdateForRemoteUser(_ context.Context, dest, user string, from int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSuccess[dest+"|"+user], nil
}

func (f *fakeDeviceStore) GetCrossSigningKeys(_ context.Context, users []string) ([]store.CrossSigningKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.CrossSigningKey
	for _, u := range users {
		out = append(out, f.crossSigning[u]...)
	}
	return out, nil
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
// The body Synapse actually sends, taken from a real capture.
//
// Comparing a live device list update against Synapse's own showed ours
// carrying only user_id, device_id and stream_id where Synapse sent:
//
//	{"device_display_name":"gofed-test-harness","device_id":"KRGIBNAWGL",
//	 "prev_id":[],"stream_id":40958034,"user_id":"@test:aguiarvieira.pt"}
//
// The missing fields are not cosmetic. prev_id chains the updates so a
// receiver can notice a gap and resync; without it a missed update is never
// repaired and the receiver keeps encrypting to a device that may be gone.
func TestDeviceListEDUMatchesSynapsesBody(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[40958034] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@test:a.example", DeviceID: "KRGIBNAWGL", StreamID: 40958034},
	}
	st.details[store.DeviceDetailKey("@test:a.example", "KRGIBNAWGL")] = store.DeviceDetail{
		UserID: "@test:a.example", DeviceID: "KRGIBNAWGL", Exists: true,
		DisplayName: "gofed-test-harness",
		KeysJSON:    []byte(`{"algorithms":["m.olm.v1.curve25519-aes-sha2"]}`),
	}

	d, m, _ := newDevices(t, st, nil)
	d.allowDeviceNameLookup = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 40958034); err != nil {
		t.Fatal(err)
	}

	edus := m.Get("b.example").PeekEDUs()
	if len(edus) != 1 {
		t.Fatalf("%d EDUs queued, want 1", len(edus))
	}
	var got map[string]any
	if err := json.Unmarshal(edus[0].Content, &got); err != nil {
		t.Fatal(err)
	}

	if got["user_id"] != "@test:a.example" || got["device_id"] != "KRGIBNAWGL" {
		t.Errorf("identity wrong: %v", got)
	}
	if got["stream_id"] != float64(40958034) {
		t.Errorf("stream_id = %v", got["stream_id"])
	}
	// No predecessor delivered yet, so an empty list -- present, not absent.
	prev, ok := got["prev_id"].([]any)
	if !ok || len(prev) != 0 {
		t.Errorf("prev_id = %v, want an empty list", got["prev_id"])
	}
	if got["device_display_name"] != "gofed-test-harness" {
		t.Errorf("device_display_name = %v", got["device_display_name"])
	}
	if _, ok := got["keys"]; !ok {
		t.Error("keys missing; the receiver cannot encrypt to a device without them")
	}
	if _, ok := got["deleted"]; ok {
		t.Error("deleted set for a device that exists")
	}
}

// prev_id chains WITHIN a batch, so the second update points at the first.
// A flat prev_id would let a receiver accept them out of order.
func TestDeviceListPrevIDChainsWithinTheBatch(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[20] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@u:a.example", DeviceID: "D1", StreamID: 10},
		{UserID: "@u:a.example", DeviceID: "D2", StreamID: 20},
	}
	for _, id := range []string{"D1", "D2"} {
		st.details[store.DeviceDetailKey("@u:a.example", id)] = store.DeviceDetail{
			UserID: "@u:a.example", DeviceID: id, Exists: true}
	}
	// One already delivered, so the first update chains from it.
	st.lastSuccess["b.example|@u:a.example"] = 5

	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 20); err != nil {
		t.Fatal(err)
	}

	edus := m.Get("b.example").PeekEDUs()
	if len(edus) != 2 {
		t.Fatalf("%d EDUs, want 2", len(edus))
	}
	want := []float64{5, 10}
	for i, e := range edus {
		var body map[string]any
		if err := json.Unmarshal(e.Content, &body); err != nil {
			t.Fatal(err)
		}
		prev := body["prev_id"].([]any)
		if len(prev) != 1 || prev[0] != want[i] {
			t.Errorf("update %d prev_id = %v, want [%v]", i, body["prev_id"], want[i])
		}
	}
}

// A device that no longer exists is reported as deleted rather than as one
// with no keys. Without it the receiver keeps encrypting to a device that is
// gone.
func TestDeletedDeviceIsMarkedDeleted(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[7] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@u:a.example", DeviceID: "GONE", StreamID: 7},
	}
	// Deliberately no details entry: the device row is gone.

	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 7); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(m.Get("b.example").PeekEDUs()[0].Content, &body); err != nil {
		t.Fatal(err)
	}
	if body["deleted"] != true {
		t.Errorf("deleted = %v, want true", body["deleted"])
	}
	if _, ok := body["keys"]; ok {
		t.Error("keys sent for a deleted device")
	}
}

// The display name is only sent when Synapse's own config allows it. Sending
// it otherwise leaks a name the homeserver had chosen to withhold.
func TestDisplayNameRespectsSynapsesConfig(t *testing.T) {
	st := newDeviceStore()
	st.destsForStream[7] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: "@u:a.example", DeviceID: "D1", StreamID: 7},
	}
	st.details[store.DeviceDetailKey("@u:a.example", "D1")] = store.DeviceDetail{
		UserID: "@u:a.example", DeviceID: "D1", Exists: true, DisplayName: "secret laptop"}

	d, m, _ := newDevices(t, st, nil) // lookup disabled by default
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 7); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(m.Get("b.example").PeekEDUs()[0].Content, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["device_display_name"]; ok {
		t.Error("a display name was sent although the homeserver disallows the lookup")
	}
}

// A destination in a long backoff must be dropped BEFORE the outbox is read and
// an EDU built, as Synapse does in send_device_messages
// (federation/sender/__init__.py:1063).
//
// This replaces a test of a FilterDue method that did the same thing and was
// never called from anywhere -- which is why this path went unfiltered while
// looking covered. The assertion is now on the behaviour rather than on the
// helper, so it fails if the filter is ever unwired again.
func TestDeviceMessagesSkipBackingOffDestinations(t *testing.T) {
	st := newDeviceStore()
	d, _, _ := newDevices(t, st, nil)
	d.dueWithin = func(dest string) bool { return dest != "down.example" }

	if err := d.HandleToDevice(context.Background(),
		[]string{"down.example", "up.example"}); err != nil {
		t.Fatal(err)
	}
	if st.readCount("down.example") != 0 {
		t.Error("read the to-device outbox for a destination that is backing off")
	}
	if st.readCount("up.example") == 0 {
		t.Error("skipped a reachable destination")
	}
}

// A poke whose device_id is a user's cross-signing key version is a KEY
// rotation, not a device change.
//
// Getting this wrong is not a missing feature, it is an E2EE trust failure:
// the far side is told about a device that does not exist, never learns the
// signing key was replaced, and goes on trusting the old one.
func TestCrossSigningPokeBecomesASigningKeyUpdate(t *testing.T) {
	const user = "@u:a.example"
	const masterVersion = "LSndJVYdqFXULiUjSdyaoR"
	masterKey := []byte(`{"user_id":"` + user + `","usage":["master"],` +
		`"keys":{"ed25519:` + masterVersion + `":"` + masterVersion + `"}}`)

	st := newDeviceStore()
	st.destsForStream[7] = []string{"b.example"}
	// The poke carries the KEY VERSION where a device id would normally be.
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: user, DeviceID: masterVersion, StreamID: 7},
	}
	st.crossSigning[user] = []store.CrossSigningKey{
		{UserID: user, KeyType: "master", KeyData: masterKey, PseudoDeviceID: masterVersion},
	}

	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 7); err != nil {
		t.Fatal(err)
	}

	edus := m.Get("b.example").PeekEDUs()
	// Both names, with identical content: a server predating the stable name
	// understands only the unstable one and would otherwise never learn of the
	// rotation.
	if len(edus) != 2 {
		t.Fatalf("%d EDUs, want the stable and unstable signing key updates", len(edus))
	}
	types := map[string]string{}
	for _, e := range edus {
		types[e.Type] = string(e.Content)
	}
	stable, ok := types[txn.EDUTypeSigningKeyUpdate]
	if !ok {
		t.Fatalf("no %s emitted: %v", txn.EDUTypeSigningKeyUpdate, types)
	}
	unstable, ok := types[txn.EDUTypeUnstableSigningKeyUpdate]
	if !ok {
		t.Fatalf("no %s emitted: %v", txn.EDUTypeUnstableSigningKeyUpdate, types)
	}
	if stable != unstable {
		t.Errorf("the two names carry different content:\n %s\n %s", stable, unstable)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(stable), &body); err != nil {
		t.Fatal(err)
	}
	if body["user_id"] != user {
		t.Errorf("user_id = %v", body["user_id"])
	}
	if _, ok := body["master_key"]; !ok {
		t.Error("master_key missing")
	}
	// Emphatically not a device update.
	for _, absent := range []string{"device_id", "prev_id", "stream_id", "deleted"} {
		if _, ok := body[absent]; ok {
			t.Errorf("%q is present; this is a key update, not a device update", absent)
		}
	}
}

// A user's master and self-signing keys arrive as two separate pokes and must
// be sent as ONE update carrying both, not two updates each carrying half.
func TestMasterAndSelfSigningMergeIntoOneUpdate(t *testing.T) {
	const user = "@u:a.example"
	const masterV, selfV = "MMMMmaster", "SSSSself"

	st := newDeviceStore()
	st.destsForStream[9] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: user, DeviceID: masterV, StreamID: 8},
		{UserID: user, DeviceID: selfV, StreamID: 9},
	}
	st.crossSigning[user] = []store.CrossSigningKey{
		{UserID: user, KeyType: "master", PseudoDeviceID: masterV,
			KeyData: []byte(`{"usage":["master"],"keys":{"ed25519:` + masterV + `":"k"}}`)},
		{UserID: user, KeyType: "self_signing", PseudoDeviceID: selfV,
			KeyData: []byte(`{"usage":["self_signing"],"keys":{"ed25519:` + selfV + `":"k"}}`)},
	}

	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 9); err != nil {
		t.Fatal(err)
	}

	edus := m.Get("b.example").PeekEDUs()
	if len(edus) != 2 {
		t.Fatalf("%d EDUs, want one update under each name", len(edus))
	}
	var body map[string]any
	if err := json.Unmarshal(edus[0].Content, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["master_key"]; !ok {
		t.Error("master_key missing from the merged update")
	}
	if _, ok := body["self_signing_key"]; !ok {
		t.Error("self_signing_key missing from the merged update")
	}
}

// A real device and a key rotation in the same batch must produce both kinds,
// and the device's prev_id chain must not be disturbed by the key poke.
func TestDeviceAndKeyUpdatesCoexist(t *testing.T) {
	const user = "@u:a.example"
	const keyV = "KKKKkey"

	st := newDeviceStore()
	st.destsForStream[11] = []string{"b.example"}
	st.pokes["b.example"] = []store.DevicePoke{
		{UserID: user, DeviceID: "REALDEVICE", StreamID: 10},
		{UserID: user, DeviceID: keyV, StreamID: 11},
	}
	st.details[store.DeviceDetailKey(user, "REALDEVICE")] = store.DeviceDetail{
		UserID: user, DeviceID: "REALDEVICE", Exists: true}
	st.crossSigning[user] = []store.CrossSigningKey{
		{UserID: user, KeyType: "master", PseudoDeviceID: keyV,
			KeyData: []byte(`{"usage":["master"],"keys":{"ed25519:` + keyV + `":"k"}}`)},
	}

	d, m, _ := newDevices(t, st, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.HandleDeviceLists(ctx, 11); err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for _, e := range m.Get("b.example").PeekEDUs() {
		counts[e.Type]++
	}
	if counts[txn.EDUTypeDeviceListUpdate] != 1 {
		t.Errorf("%d device list updates, want 1", counts[txn.EDUTypeDeviceListUpdate])
	}
	if counts[txn.EDUTypeSigningKeyUpdate] != 1 || counts[txn.EDUTypeUnstableSigningKeyUpdate] != 1 {
		t.Errorf("signing key updates = %v", counts)
	}
}
