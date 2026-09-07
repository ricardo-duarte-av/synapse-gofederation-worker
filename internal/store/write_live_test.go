package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// The writer is tested against a REAL PostgreSQL with Synapse's schema, never
// against production. Point it at a throwaway database with the relevant tables:
//
//	WRITE_DSN="host=localhost port=55432 user=postgres password=test dbname=testdb" \
//	  go test ./internal/store/ -run TestLiveWriter -v
//
// These are the statements whose semantics are easy to get subtly wrong -- the
// conditional upserts in particular -- and a unit test with a fake would only
// check that Go can build a string.
func liveWriter(t *testing.T) *Writer {
	t.Helper()
	dsn := os.Getenv("WRITE_DSN")
	if dsn == "" {
		t.Skip("set WRITE_DSN to a throwaway database with Synapse's schema")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := OpenWriter(ctx, Config{DSN: dsn, MaxConns: 4, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	if err := w.CanWrite(ctx); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestLiveWriterDeviceLists(t *testing.T) {
	w := liveWriter(t)
	ctx := context.Background()
	const dest = "write-test.example"

	_, err := w.pool.Exec(ctx, `DELETE FROM device_lists_outbound_pokes WHERE destination = $1`, dest)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.pool.Exec(ctx, `DELETE FROM device_lists_outbound_last_success WHERE destination = $1`, dest)

	// Two pokes for one user, one for another.
	for _, p := range []struct {
		user, device string
		stream       int64
	}{
		{"@a:x.example", "D1", 10},
		{"@a:x.example", "D2", 20},
		{"@b:x.example", "D3", 15},
	} {
		_, err := w.pool.Exec(ctx, `
			INSERT INTO device_lists_outbound_pokes
			  (destination, stream_id, user_id, device_id, sent, ts)
			VALUES ($1, $2, $3, $4, false, 0)`, dest, p.stream, p.user, p.device)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := w.MarkAsSentDevicesByRemote(ctx, dest, 20); err != nil {
		t.Fatal(err)
	}

	var remaining int
	if err := w.pool.QueryRow(ctx,
		`SELECT count(*) FROM device_lists_outbound_pokes WHERE destination = $1`, dest).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Errorf("%d pokes left after marking sent", remaining)
	}

	// The HIGHEST stream id per user, which is what the next batch's prev_id
	// chains from. Recording the lowest, or the last one seen, would break the
	// chain silently.
	rows, err := w.pool.Query(ctx,
		`SELECT user_id, stream_id FROM device_lists_outbound_last_success
		 WHERE destination = $1 ORDER BY user_id`, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var u string
		var s int64
		if err := rows.Scan(&u, &s); err != nil {
			t.Fatal(err)
		}
		got[u] = s
	}
	want := map[string]int64{"@a:x.example": 20, "@b:x.example": 15}
	for u, v := range want {
		if got[u] != v {
			t.Errorf("last_success[%s] = %d, want %d", u, got[u], v)
		}
	}
}
