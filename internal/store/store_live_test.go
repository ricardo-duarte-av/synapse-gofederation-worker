package store

import (
	"context"
	"os"
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
