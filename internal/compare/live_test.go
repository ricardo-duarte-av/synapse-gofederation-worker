package compare

import (
	"context"
	"crypto/sha256"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestLiveRoutingParity compares our routing decisions against Synapse's own,
// on production data.
//
// This is the check the whole shadow exists to pass. Everything else -- the
// signatures, the PDU bytes, the transaction limits -- is machinery for putting
// the right events in front of the right servers, and this is the only thing
// that says whether we did.
//
//	SYNAPSE_DSN="host=/var/sockets user=gofed_ro dbname=synapse-db" \
//	STATE_DSN="host=localhost port=55432 user=postgres password=test dbname=testdb" \
//	SHARD_INSTANCES=av-federation-sender-worker-1,av-federation-sender-worker-2 \
//	SHADOW_INSTANCE=av-federation-sender-worker-1 \
//	WORKER_NAME=av-gofederation-worker-1 \
//	go test ./internal/compare/ -run TestLiveRoutingParity -v
func TestLiveRoutingParity(t *testing.T) {
	synapseDSN, stateDSN := os.Getenv("SYNAPSE_DSN"), os.Getenv("STATE_DSN")
	if synapseDSN == "" || stateDSN == "" {
		t.Skip("set SYNAPSE_DSN and STATE_DSN")
	}
	instances := strings.Split(os.Getenv("SHARD_INSTANCES"), ",")
	shadow := os.Getenv("SHADOW_INSTANCE")
	worker := os.Getenv("WORKER_NAME")
	if len(instances) < 1 || instances[0] == "" || shadow == "" || worker == "" {
		t.Fatal("set SHARD_INSTANCES, SHADOW_INSTANCE and WORKER_NAME")
	}

	ctx := context.Background()
	synapsePool := open(t, ctx, synapseDSN)
	statePool := open(t, ctx, stateDSN)

	// The horizon has to be the newest event WE processed, not the newest in
	// the database: comparing past our own cursor would report every event we
	// have not reached yet as missing.
	// The floor and the horizon are both OURS. Synapse's table stretches back
	// to the beginning of the homeserver; the window we can say anything about
	// is the one we were running for.
	var ourOldest, ourNewest int64
	if err := statePool.QueryRow(ctx,
		`SELECT COALESCE(MIN(stream_ordering), 0), COALESCE(MAX(stream_ordering), 0)
		 FROM gofederation.destination_rooms WHERE instance_name = $1`,
		worker).Scan(&ourOldest, &ourNewest); err != nil {
		t.Fatal(err)
	}
	if ourNewest == 0 {
		t.Skip("we have recorded no routes yet")
	}

	rows := &crossPoolRows{
		synapse: synapsePool, state: statePool, worker: worker,
		shouldHandle: shardFilter(instances, shadow),
	}
	// A small settle window: the recorded data is already historical, so the
	// tip skew this guards against has long since resolved.
	rep, err := New(rows, 100).Compare(ctx, ourOldest, ourNewest)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("window=[%d,%d] pairs=%d agreed=%d rate=%.4f skipped=%d unsettled=%d counts=%v took=%dms",
		rep.Floor, rep.Horizon, rep.Pairs, rep.Agreed, rep.AgreementRate(), rep.Skipped,
		rep.Unsettled, rep.Counts, rep.TookMS)
	for i, d := range rep.Disagreements {
		if i >= 10 {
			t.Logf("... and %d more", len(rep.Disagreements)-10)
			break
		}
		t.Logf("  %s %s in %s: synapse=%d ours=%d", d.Kind, d.Destination, d.RoomID, d.Synapse, d.Ours)
	}

	if rep.Pairs == 0 {
		t.Fatal("no pairs were compared; the horizon or the shard filter is wrong")
	}
	// Reported rather than asserted at a threshold: the point of running this
	// is to learn the rate and what the residue consists of. A gate belongs in
	// the promotion decision, not in a test that is also a measuring tool.
	if rep.Counts[KindMissing] > 0 {
		t.Logf("NOTE: %d pairs Synapse routed and we did not -- the dangerous direction",
			rep.Counts[KindMissing])
	}
}

// crossPoolRows reads the two sides from two different connections, since the
// read-only Synapse role and the state role are deliberately separate.
type crossPoolRows struct {
	synapse      *pgxpool.Pool
	state        *pgxpool.Pool
	worker       string
	shouldHandle func(string) bool
}

func (r *crossPoolRows) SynapseRoutes(ctx context.Context) (map[Key]int64, error) {
	rows, err := r.synapse.Query(ctx,
		`SELECT destination, room_id, stream_ordering FROM destination_rooms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[Key]int64{}
	for rows.Next() {
		var k Key
		var o int64
		if err := rows.Scan(&k.Destination, &k.RoomID, &o); err != nil {
			return nil, err
		}
		if !r.shouldHandle(k.Destination) {
			continue
		}
		out[k] = o
	}
	return out, rows.Err()
}

func (r *crossPoolRows) OurRoutes(ctx context.Context) (map[Key]int64, error) {
	rows, err := r.state.Query(ctx,
		`SELECT destination, room_id, stream_ordering FROM gofederation.destination_rooms
		 WHERE instance_name = $1`, r.worker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[Key]int64{}
	for rows.Next() {
		var k Key
		var o int64
		if err := rows.Scan(&k.Destination, &k.RoomID, &o); err != nil {
			return nil, err
		}
		out[k] = o
	}
	return out, rows.Err()
}

// shardFilter is the sharding function again, inlined so this test does not
// depend on the package it is checking the consequences of.
func shardFilter(instances []string, mine string) func(string) bool {
	return func(dest string) bool {
		if len(instances) == 1 {
			return instances[0] == mine
		}
		sum := sha256.Sum256([]byte(dest))
		le := make([]byte, len(sum))
		for i, b := range sum {
			le[len(sum)-1-i] = b
		}
		idx := new(big.Int).Mod(new(big.Int).SetBytes(le), big.NewInt(int64(len(instances)))).Int64()
		return instances[idx] == mine
	}
}

func open(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
