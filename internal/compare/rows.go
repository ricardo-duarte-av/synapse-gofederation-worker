package compare

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBRows reads both records from PostgreSQL.
//
// Synapse's destination_rooms holds every sender's decisions, since each
// destination belongs to exactly one shard and the rows never collide. Ours
// holds only our shard, so the Synapse side is filtered to the same shard --
// otherwise every destination belonging to the OTHER sender would show up as
// "missing", which is true and useless.
//
// The shard filter is applied in Go rather than in SQL because the shard
// function is sha256-mod-N and reproducing it in PostgreSQL would mean
// maintaining a second implementation of the one algorithm that must not
// diverge.
type DBRows struct {
	pool *pgxpool.Pool
	// synapseTable and ourTable are qualified names, so a test can point them
	// somewhere harmless.
	synapseTable string
	ourTable     string
	instance     string
	// shouldHandle is the shard filter, shared with the worker itself.
	shouldHandle func(destination string) bool
}

// NewDBRows builds a reader.
func NewDBRows(pool *pgxpool.Pool, synapseTable, ourTable, instance string, shouldHandle func(string) bool) *DBRows {
	return &DBRows{
		pool: pool, synapseTable: synapseTable, ourTable: ourTable,
		instance: instance, shouldHandle: shouldHandle,
	}
}

// SynapseRoutes reads Synapse's own record, filtered to our shard.
//
// Unfiltered by ordering: see the Rows interface for why narrowing the window
// here would hide rows rather than exclude them.
func (r *DBRows) SynapseRoutes(ctx context.Context) (map[Key]int64, error) {
	q := fmt.Sprintf(`SELECT destination, room_id, stream_ordering FROM %s`, r.synapseTable)
	return r.scan(ctx, q, true)
}

// OurRoutes reads our record.
func (r *DBRows) OurRoutes(ctx context.Context) (map[Key]int64, error) {
	q := fmt.Sprintf(`
		SELECT destination, room_id, stream_ordering FROM %s
		WHERE instance_name = $1`, r.ourTable)
	return r.scan(ctx, q, false, r.instance)
}

func (r *DBRows) scan(ctx context.Context, q string, filterShard bool, args ...any) (map[Key]int64, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[Key]int64{}
	for rows.Next() {
		var k Key
		var ordering int64
		if err := rows.Scan(&k.Destination, &k.RoomID, &ordering); err != nil {
			return nil, err
		}
		if filterShard && r.shouldHandle != nil && !r.shouldHandle(k.Destination) {
			continue
		}
		out[k] = ordering
	}
	return out, rows.Err()
}
