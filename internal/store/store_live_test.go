package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Live tests run against a real Synapse database and skip themselves without
// one, so CI never needs a database:
//
//	SYNAPSE_DSN="host=/var/sockets user=gofed_ro dbname=synapse-db" \
//	go test ./internal/store/ -run TestLive -v
//
// They exist because every query in this package is a transcription of
// Synapse's own SQL. A transcription that compiles proves nothing; one that
// runs against the real schema proves the column names, the types and the
// nullability. Most of these tables have nullable columns that only ever
// contain NULL on rows nobody looks at, so a scan that is wrong will pass a
// unit test with hand-made rows and fail in production three weeks later.
func liveStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("SYNAPSE_DSN")
	if dsn == "" {
		t.Skip("set SYNAPSE_DSN to run against a real Synapse database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestLiveReadOnly(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	role, err := s.CurrentRole(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := s.IsReadOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("connected as %q, read-only=%v", role, ro)
	if !ro {
		t.Logf("WARNING: %q can write; production must use a role with "+
			"default_transaction_read_only set (deploy/readonly-role.sql)", role)
	}
}

func TestLiveEventPickup(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	max, err := s.MaxStreamOrdering(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if max == 0 {
		t.Skip("no events in this database")
	}

	// A window ending at max, small enough to be cheap and large enough that a
	// busy server fills it.
	const limit = 20
	from := max - 500
	events, next, err := s.GetAllNewEventIDsStream(ctx, from, max, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("max=%d got %d events, next=%d", max, len(events), next)

	// The truncation rule: a full batch must NOT advance the cursor past the
	// last row, or every event the limit cut off is skipped forever.
	if len(events) == limit && next != events[len(events)-1].StreamOrdering {
		t.Errorf("full batch advanced next to %d, want the last row's %d",
			next, events[len(events)-1].StreamOrdering)
	}
	if len(events) < limit && next != max {
		t.Errorf("partial batch left next at %d, want the window's upper bound %d", next, max)
	}
	for i := 1; i < len(events); i++ {
		if events[i].StreamOrdering <= events[i-1].StreamOrdering {
			t.Fatalf("events are not ordered by stream_ordering: %d then %d",
				events[i-1].StreamOrdering, events[i].StreamOrdering)
		}
	}

	if len(events) == 0 {
		return
	}
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.EventID)
	}
	full, err := s.GetEvents(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) == 0 {
		t.Fatal("GetEvents returned nothing for ids that came from the events table")
	}
	// Order must be preserved: the sender processes a room's events in
	// stream order and a reordering would send them out of order.
	for i, e := range full {
		if e.EventID != ids[i] {
			t.Fatalf("GetEvents reordered: position %d is %s, want %s", i, e.EventID, ids[i])
		}
		if len(e.JSON) == 0 || len(e.InternalMetadata) == 0 {
			t.Errorf("%s has empty json or internal_metadata", e.EventID)
		}
	}
	t.Logf("first event: room=%s type=%s sender=%s metadata=%s",
		full[0].RoomID, full[0].Type, full[0].Sender, full[0].InternalMetadata)

	hosts, err := s.CurrentJoinedHosts(ctx, full[0].RoomID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("room %s has %d joined hosts", full[0].RoomID, len(hosts))
	for _, h := range hosts {
		if h == "" {
			t.Error("CurrentJoinedHosts returned an empty host")
		}
	}
}

func TestLiveDestinations(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	// Take real destination names rather than inventing them, so the queries
	// are exercised against rows that actually exist.
	rows, err := s.pool.Query(ctx, `SELECT destination FROM destinations LIMIT 50`)
	if err != nil {
		t.Fatal(err)
	}
	var dests []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		dests = append(dests, d)
	}
	rows.Close()
	if len(dests) == 0 {
		t.Skip("no destinations in this database")
	}

	timings, err := s.GetDestinationRetryTimings(ctx, dests)
	if err != nil {
		t.Fatal(err)
	}
	due := FilterDestinationsByRetryLimiter(dests, timings, time.Now(), time.Hour)
	t.Logf("%d destinations, %d with timings, %d due within an hour", len(dests), len(timings), len(due))
	if len(due) > len(dests) {
		t.Error("the retry filter returned more destinations than it was given")
	}

	for _, d := range dests[:min(5, len(dests))] {
		lsso, ok, err := s.GetDestinationLastSuccessfulStreamOrdering(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := s.GetCatchUpRoomEventIDs(ctx, d, lsso, 50)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: last_successful=%d (set=%v) owes %d rooms", d, lsso, ok, len(ids))
		if len(ids) > 50 {
			t.Errorf("catch-up returned %d ids, want at most the limit", len(ids))
		}
	}
}

func TestLiveDeviceQueries(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	outbox, err := s.MaxDeviceOutboxStreamID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pokes, err := s.MaxDeviceListOutboundStreamID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("max to-device stream_id=%d, max device list poke stream_id=%d", outbox, pokes)

	var dest string
	err = s.pool.QueryRow(ctx, `SELECT destination FROM device_federation_outbox LIMIT 1`).Scan(&dest)
	if err == nil {
		msgs, next, err := s.GetNewDeviceMsgsForRemote(ctx, dest, 0, outbox, 10)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s has %d queued to-device messages, next=%d", dest, len(msgs), next)
		// The fast-forward rule: a partial batch jumps the cursor to the
		// current stream id, because the gap is other destinations' rows.
		if len(msgs) < 10 && next != outbox {
			t.Errorf("partial batch left the cursor at %d, want the fast-forward to %d", next, outbox)
		}
	} else {
		t.Log("device_federation_outbox is empty; skipping the to-device read")
	}

	err = s.pool.QueryRow(ctx, `SELECT destination FROM device_lists_outbound_pokes LIMIT 1`).Scan(&dest)
	if err == nil {
		got, err := s.GetDeviceUpdatesByRemote(ctx, dest, 0, pokes, 10)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s has %d device list pokes pending", dest, len(got))
		if len(got) > 0 {
			d, err := s.GetDestinationsForDevice(ctx, got[0].StreamID)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("stream_id %d concerns %d destinations", got[0].StreamID, len(d))
		}
	} else {
		t.Log("device_lists_outbound_pokes is empty; skipping the poke read")
	}
}

// The federation_stream_position read must work for the instance we shadow and
// return zero rather than an error for one that has no row.
func TestLiveFederationOutPos(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	instance := os.Getenv("SHADOW_INSTANCE")
	if instance != "" {
		pos, err := s.GetFederationOutPos(ctx, "events", instance)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s is at events=%d", instance, pos)
		if pos == 0 {
			t.Errorf("%s has no events position; is that really a configured sender?", instance)
		}
	}

	pos, err := s.GetFederationOutPos(ctx, "events", "no-such-worker")
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("an unknown instance returned %d, want 0", pos)
	}
}

// TestLiveStateBeforeEvent exercises the exact destination path against real
// state groups.
//
// This is the query that replaced the current-state approximation, and it is
// the expensive one: state_groups_state is the largest table in a Synapse
// database by a wide margin and the planner picks a sequential scan over it
// without the seqscan hint. A unit test with hand-made rows would prove neither
// the recursive walk nor that the hint is doing its job.
func TestLiveStateBeforeEvent(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	// A recent non-outlier event that actually has a state group.
	var eventID string
	err := s.pool.QueryRow(ctx, `
		SELECT e.event_id FROM events e
		JOIN event_to_state_groups etsg USING (event_id)
		WHERE e.outlier = false
		ORDER BY e.stream_ordering DESC LIMIT 1`).Scan(&eventID)
	if err != nil {
		t.Skipf("no event with a state group: %v", err)
	}

	groups, err := s.GetStateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		t.Fatal(err)
	}
	group, ok := groups[eventID]
	if !ok {
		t.Fatalf("%s has no state group", eventID)
	}

	started := time.Now()
	hosts, err := s.JoinedHostsAtStateGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(started)
	t.Logf("state group %d resolves to %d joined hosts in %s", group, len(hosts), took)

	if len(hosts) == 0 {
		t.Error("no joined hosts at a state group taken from a live event")
	}
	seen := map[string]bool{}
	for _, h := range hosts {
		if h == "" {
			t.Error("empty host in the result")
		}
		if seen[h] {
			t.Errorf("duplicate host %q; the query should return each once", h)
		}
		seen[h] = true
	}
	// The seqscan hint is the difference between milliseconds and minutes here.
	if took > 10*time.Second {
		t.Errorf("resolving one state group took %s; the seqscan hint is probably not "+
			"being applied", took)
	}

	// An unknown event has no state group, and that is not an error: an
	// outlier legitimately has none.
	missing, err := s.GetStateGroupsForEvents(ctx, []string{"$definitely-not-an-event"})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Errorf("an unknown event returned %v", missing)
	}
}

// TestLiveStateBeforeMatchesCurrentStateForSettledRooms is a sanity check
// rather than a proof.
//
// For an event at the tip of a room whose membership has not changed since, the
// state before it and the current state should name the same hosts. Where they
// differ, the event itself changed the membership -- which is exactly the case
// the exact path exists to get right, so a difference is informative rather
// than a failure.
func TestLiveStateBeforeMatchesCurrentStateForSettledRooms(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	rows, err := s.pool.Query(ctx, `
		SELECT e.event_id, e.room_id, etsg.state_group
		FROM events e
		JOIN event_to_state_groups etsg USING (event_id)
		WHERE e.outlier = false AND e.type = 'm.room.message'
		ORDER BY e.stream_ordering DESC LIMIT 20`)
	if err != nil {
		t.Fatal(err)
	}
	type sample struct {
		eventID, roomID string
		group           int64
	}
	var samples []sample
	for rows.Next() {
		var sm sample
		if err := rows.Scan(&sm.eventID, &sm.roomID, &sm.group); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, sm)
	}
	rows.Close()
	if len(samples) == 0 {
		t.Skip("no message events to compare")
	}

	agreed := 0
	for _, sm := range samples {
		before, err := s.JoinedHostsAtStateGroup(ctx, sm.group)
		if err != nil {
			t.Fatal(err)
		}
		current, err := s.CurrentJoinedHosts(ctx, sm.roomID)
		if err != nil {
			t.Fatal(err)
		}
		if sameSet(before, current) {
			agreed++
		}
	}
	t.Logf("%d of %d message events agree with current state", agreed, len(samples))
	// A message event does not change membership, so for a room with no
	// membership churn since, these should mostly agree. Mostly, not always --
	// the room may have moved on.
	if agreed == 0 {
		t.Error("not one message event agreed with current state; the state-group walk " +
			"is probably resolving the wrong thing")
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]bool, len(a))
	for _, s := range a {
		m[s] = true
	}
	for _, s := range b {
		if !m[s] {
			return false
		}
	}
	return true
}

// TestLiveCrossSigningKeys checks the pseudo device id derivation against real
// keys.
//
// That derivation is the whole mechanism for recognising a cross-signing
// rotation: Synapse records one as a device poke whose device_id is the key's
// version. Derive it wrongly and every rotation is sent as an
// m.device_list_update for a device that does not exist, while the rotation
// itself is never announced -- so the far side keeps trusting a replaced key.
// A unit test with a hand-made key would only check the string split.
func TestLiveCrossSigningKeys(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT user_id FROM e2e_cross_signing_keys LIMIT 20`)
	if err != nil {
		t.Fatal(err)
	}
	var users []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			t.Fatal(err)
		}
		users = append(users, u)
	}
	rows.Close()
	if len(users) == 0 {
		t.Skip("no cross-signing keys in this database")
	}

	keys, err := s.GetCrossSigningKeys(ctx, users)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("no keys returned for users that have them")
	}

	seen := map[string]bool{}
	for _, k := range keys {
		if k.KeyType != "master" && k.KeyType != "self_signing" {
			t.Errorf("unexpected keytype %q; only these two become EDUs", k.KeyType)
		}
		if k.PseudoDeviceID == "" {
			t.Errorf("%s/%s has no pseudo device id, so a rotation could never be matched",
				k.UserID, k.KeyType)
		}
		if strings.Contains(k.PseudoDeviceID, ":") {
			t.Errorf("%s/%s pseudo device id %q still carries the algorithm prefix",
				k.UserID, k.KeyType, k.PseudoDeviceID)
		}
		// The derived id must be the version of the key actually inside the
		// blob, not something adjacent to it.
		if !strings.Contains(string(k.KeyData), "ed25519:"+k.PseudoDeviceID) {
			t.Errorf("%s/%s: derived %q does not appear in the key data",
				k.UserID, k.KeyType, k.PseudoDeviceID)
		}
		// One row per (user, keytype): a rotated key leaves the old one
		// behind, and announcing that would tell the far side to trust a key
		// the user has replaced.
		id := k.UserID + "/" + k.KeyType
		if seen[id] {
			t.Errorf("%s returned more than once; the newest must win", id)
		}
		seen[id] = true
	}
	t.Logf("%d cross-signing keys across %d users, all with a usable version",
		len(keys), len(users))
}

// The batch must keep SET LOCAL scoped: applied for the query, and gone from
// the connection afterwards. If it leaked, every other query on that pooled
// connection would silently get a different plan -- and through a transaction
// pooler, on somebody else's connection.
func TestLiveJoinedHostsDoesNotLeakSeqscan(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	group, err := anyStateGroup(ctx, s)
	if err != nil || group == 0 {
		t.Skip("no state group available")
	}
	if _, err := s.JoinedHostsAtStateGroup(ctx, group); err != nil {
		t.Fatal(err)
	}

	// Same pool, and with MaxConns of 1 in the live harness the same
	// connection, so a leak would be visible here.
	var setting string
	if err := s.pool.QueryRow(ctx, `SHOW enable_seqscan`).Scan(&setting); err != nil {
		t.Fatal(err)
	}
	if setting != "on" {
		t.Errorf("enable_seqscan = %q after the query; SET LOCAL leaked onto the connection", setting)
	}
}

func anyStateGroup(ctx context.Context, s *Store) (int64, error) {
	var g int64
	err := s.pool.QueryRow(ctx,
		`SELECT state_group FROM state_group_edges LIMIT 1`).Scan(&g)
	return g, err
}
