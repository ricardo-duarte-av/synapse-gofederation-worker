package state

import (
	"context"
	"os"
	"testing"
	"time"
)

// These run against a real PostgreSQL and skip without one. Any throwaway
// database will do -- they never touch Synapse's schema:
//
//	docker run -d --rm --name pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=testdb \
//	  -p 55432:5432 postgres:16-alpine
//	psql ... -f deploy/state-role.sql   # or just the CREATE TABLE from it
//	STATE_DSN="host=localhost port=55432 user=postgres password=test dbname=testdb" \
//	  go test ./internal/state/ -v
//
// The upsert is the reason these exist. Its ON CONFLICT clause has to name the
// target row through an alias -- a schema-qualified name is not usable there --
// and that is a runtime error, not a compile-time one.
func liveState(t *testing.T, instance string) *Store {
	t.Helper()
	dsn := os.Getenv("STATE_DSN")
	if dsn == "" {
		t.Skip("set STATE_DSN to run against a real PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, Config{
		DSN:            dsn,
		Table:          "gofederation.stream_positions",
		InstanceName:   instance,
		MaxConns:       4,
		ConnectTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestLiveSetAndGet(t *testing.T) {
	s := liveState(t, "test-"+t.Name())
	ctx := context.Background()

	// Unset is not the same as zero.
	if _, ok, err := s.Get(ctx, CursorEvents); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("a fresh instance already has a cursor")
	}

	if err := s.Set(ctx, CursorEvents, 100); err != nil {
		t.Fatal(err)
	}
	pos, ok, err := s.Get(ctx, CursorEvents)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || pos != 100 {
		t.Fatalf("Get = (%d, %v), want (100, true)", pos, ok)
	}

	if err := s.Set(ctx, CursorEvents, 200); err != nil {
		t.Fatal(err)
	}
	if pos, _, _ := s.Get(ctx, CursorEvents); pos != 200 {
		t.Fatalf("after advancing, Get = %d, want 200", pos)
	}
}

// The rule that matters: a cursor never moves backwards. Every way it could
// happen -- two workers sharing an instance name, a delayed write landing after
// a newer one -- is a case where the older value is simply wrong, and letting
// it through would re-send everything in between.
func TestLiveSetNeverGoesBackwards(t *testing.T) {
	s := liveState(t, "test-"+t.Name())
	ctx := context.Background()

	if err := s.Set(ctx, CursorEvents, 500); err != nil {
		t.Fatal(err)
	}
	// Not an error -- a late write is not a failure, it is simply ignored.
	if err := s.Set(ctx, CursorEvents, 400); err != nil {
		t.Fatalf("a backwards Set should be silently ignored, not fail: %v", err)
	}
	if pos, _, _ := s.Get(ctx, CursorEvents); pos != 500 {
		t.Fatalf("cursor moved backwards to %d, want 500", pos)
	}
	// Equal is also a no-op and must not error.
	if err := s.Set(ctx, CursorEvents, 500); err != nil {
		t.Fatal(err)
	}
}

// Two shadows of two different senders share a database and must not overwrite
// each other.
func TestLiveInstancesAreIsolated(t *testing.T) {
	ctx := context.Background()
	a := liveState(t, "test-"+t.Name()+"-a")
	b := liveState(t, "test-"+t.Name()+"-b")

	if err := a.Set(ctx, CursorEvents, 111); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(ctx, CursorEvents, 222); err != nil {
		t.Fatal(err)
	}
	if pos, _, _ := a.Get(ctx, CursorEvents); pos != 111 {
		t.Errorf("instance a sees %d, want 111", pos)
	}
	if pos, _, _ := b.Get(ctx, CursorEvents); pos != 222 {
		t.Errorf("instance b sees %d, want 222", pos)
	}
}

func TestLiveGetAllAndPerDestinationCursors(t *testing.T) {
	s := liveState(t, "test-"+t.Name())
	ctx := context.Background()

	if err := s.Set(ctx, CursorEvents, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, ToDeviceCursor("matrix.org"), 20); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, DeviceListCursor("matrix.org"), 30); err != nil {
		t.Fatal(err)
	}

	all, err := s.GetAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{
		CursorEvents:                   10,
		ToDeviceCursor("matrix.org"):   20,
		DeviceListCursor("matrix.org"): 30,
	}
	if len(all) != len(want) {
		t.Fatalf("GetAll = %v, want %v", all, want)
	}
	for k, v := range want {
		if all[k] != v {
			t.Errorf("GetAll[%q] = %d, want %d", k, all[k], v)
		}
	}
}

// Pointing the state role at a database with no schema must fail at startup,
// not on the first write hours later.
func TestLiveOpenRejectsMissingTable(t *testing.T) {
	dsn := os.Getenv("STATE_DSN")
	if dsn == "" {
		t.Skip("set STATE_DSN")
	}
	ctx := context.Background()
	_, err := Open(ctx, Config{
		DSN: dsn, Table: "gofederation.no_such_table", InstanceName: "x",
	})
	if err == nil {
		t.Fatal("Open succeeded against a table that does not exist")
	}
}
