// Package compare answers the only question that matters before this worker is
// allowed to send: does it route the same events to the same servers as the
// real federation sender?
//
// The comparison is against Synapse's own destination_rooms table, not against
// its logs and not against a reconstruction. Synapse's _send_pdu upserts
// (destination, room_id) -> stream_ordering for the full sharded destination set
// BEFORE the retry filter (federation/sender/__init__.py:828), so that table is
// its own durable record of every routing decision it made, written by Synapse
// and independent of whether delivery succeeded. We keep ours in the identical
// shape, and the comparison is a join.
//
// # What is NOT compared, and why
//
// Transaction framing is not comparable, and chasing it would be a mistake.
// Which PDUs share a transaction depends on what happened to be queued when a
// sender's loop ran; two correct implementations legitimately produce different
// groupings, different transaction ids and different origin_server_ts. A
// byte-for-byte transaction diff would report constant, meaningless failures.
//
// So the confirmation is split into three questions that ARE answerable:
//
//  1. Did we route the same (event, destination) pairs?  -- this package,
//     continuously, against destination_rooms.
//  2. Are our PDU bytes identical to Synapse's?  -- internal/txn, offline,
//     against Synapse's own canonicaljson.
//  3. Would a remote accept our transaction?  -- verified absolutely by
//     checking our signature with the published key, rather than by comparing
//     it to Synapse's.
package compare

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Rows reads both sides of the comparison.
//
// Both return EVERY row, unfiltered. That is deliberate and was learned the
// hard way: these tables hold a high-water mark per (destination, room), not a
// log, so filtering rows by stream ordering in SQL does not narrow the window --
// it hides pairs whose mark has since moved past it, and a hidden row is
// indistinguishable from one that was never written. The first version filtered
// on Synapse's side and reported a pair as "extra" that Synapse had in fact
// routed, 361 orderings later than our copy. Windowing is done in Compare,
// where a value beyond the horizon can be recognised for what it is.
type Rows interface {
	SynapseRoutes(ctx context.Context) (map[Key]int64, error)
	OurRoutes(ctx context.Context) (map[Key]int64, error)
}

// Key is a (destination, room) pair, the grain both tables are keyed by.
type Key struct {
	Destination string
	RoomID      string
}

// Disagreement is one pair the two senders do not agree about.
type Disagreement struct {
	Key
	// Synapse and Ours are the stream orderings each recorded. Zero means the
	// pair is absent from that side entirely.
	Synapse int64
	Ours    int64
	Kind    Kind
}

// Kind classifies a disagreement. The three mean very different things and
// lumping them into one "mismatch" count would hide the only one that is
// unambiguously a bug.
type Kind string

const (
	// KindMissing is a pair Synapse routed and we did not, or where we are
	// behind. This is the dangerous direction: in production it would be an
	// event a server never receives.
	KindMissing Kind = "missing"
	// KindExtra is a pair we routed and Synapse did not, or where we are
	// ahead. In production it would be a server receiving an event it should
	// not have -- less damaging than a miss, but still wrong.
	KindExtra Kind = "extra"
	// KindAhead is a pair where we are ahead of Synapse but Synapse has simply
	// not caught up yet. Distinguished from KindExtra by the settle window: it
	// is a timing artefact, not a disagreement, and it resolves itself.
	KindAhead Kind = "ahead"
)

// Report is the outcome of one comparison.
type Report struct {
	// Horizon is the stream ordering both sides were compared up to, and Floor
	// the ordering they were compared from.
	Horizon int64
	Floor   int64
	// Skipped is how many pairs fell entirely below the floor, i.e. describe
	// history from before this worker ran.
	Skipped int
	// Unsettled is how many pairs had a mark beyond the horizon on one side or
	// the other, so the two marks describe different instants and cannot be
	// equated. Not agreement and not disagreement -- they settle on their own
	// as the horizon advances.
	Unsettled int
	// Pairs is how many (destination, room) pairs were considered.
	Pairs int
	// Agreed is how many matched exactly.
	Agreed int
	// Disagreements, most significant first: missing before extra before ahead.
	Disagreements []Disagreement
	// Counts by kind, for metrics.
	Counts map[Kind]int
	TookMS int64
}

// AgreementRate is the fraction of pairs that matched, in [0, 1]. A comparison
// with no pairs is reported as 1 rather than 0: nothing to disagree about is
// not a failure.
func (r Report) AgreementRate() float64 {
	if r.Pairs == 0 {
		return 1
	}
	return float64(r.Agreed) / float64(r.Pairs)
}

// Comparer runs the comparison.
type Comparer struct {
	rows Rows
	// settle is how far behind the newest event to draw the horizon.
	//
	// Both senders are working through the same stream at their own pace, so
	// near the tip one is always slightly ahead of the other. Comparing there
	// would report a stream of disagreements that are really just clock skew.
	// The horizon is drawn back by this much so both have had time to arrive.
	settle int64
}

// New builds a Comparer. settle is in stream orderings.
func New(rows Rows, settle int64) *Comparer {
	if settle <= 0 {
		// A few thousand orderings is minutes of traffic on a busy server and
		// seconds on a quiet one, which is the right shape: the wait should
		// scale with how fast events are actually moving.
		settle = 5000
	}
	return &Comparer{rows: rows, settle: settle}
}

// Compare diffs the two records over [floor, newest-settle].
//
// The floor is not optional and getting it wrong makes the whole comparison
// meaningless. Synapse's destination_rooms is a high-water mark accumulated over
// the homeserver's entire lifetime -- on this deployment it spans stream
// orderings from 3,605 to the present across 40,562 rows -- while ours begins
// the moment this worker first ran. Comparing everything reports every pair
// Synapse routed before we existed as "missing", which on the first run here
// was 19,314 of 19,822 pairs and says nothing at all.
//
// So a pair is only considered when at least one side falls at or above the
// floor. Below it, both sides are describing history we were not present for.
// Pass the oldest ordering this worker recorded.
func (c *Comparer) Compare(ctx context.Context, floor, newest int64) (Report, error) {
	started := time.Now()
	horizon := newest - c.settle
	if horizon <= 0 || horizon < floor {
		return Report{Horizon: 0, Floor: floor, Counts: map[Kind]int{}}, nil
	}

	synapse, err := c.rows.SynapseRoutes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("compare: reading Synapse's routes: %w", err)
	}
	ours, err := c.rows.OurRoutes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("compare: reading our routes: %w", err)
	}

	rep := Report{Horizon: horizon, Floor: floor, Counts: map[Kind]int{}}
	seen := make(map[Key]bool, len(synapse)+len(ours))

	for k, s := range synapse {
		seen[k] = true
		rep.classify(k, s, ours[k], floor, horizon)
	}
	for k, o := range ours {
		if !seen[k] {
			// Synapse has no row at all for a pair we routed.
			rep.classify(k, 0, o, floor, horizon)
		}
	}

	sort.Slice(rep.Disagreements, func(i, j int) bool {
		a, b := rep.Disagreements[i], rep.Disagreements[j]
		if a.Kind != b.Kind {
			return kindRank(a.Kind) < kindRank(b.Kind)
		}
		if a.Destination != b.Destination {
			return a.Destination < b.Destination
		}
		return a.RoomID < b.RoomID
	})

	rep.TookMS = time.Since(started).Milliseconds()
	return rep, nil
}

// classify decides what one pair says, given both high-water marks.
//
// The two marks are only comparable for equality when BOTH sit at or below the
// horizon. A mark beyond it means that sender has already routed a newer event
// to this pair, and because the row holds only the newest ordering there is no
// way to recover what it was at the horizon. Asserting equality there compares
// two different instants and manufactures disagreements; that is precisely the
// mistake the first version made.
func (r *Report) classify(k Key, synapse, ours, floor, horizon int64) {
	// Neither side moved inside our window: this pair is history from before
	// the worker ran.
	if synapse < floor && ours < floor {
		r.Skipped++
		return
	}
	// Either side has moved past the horizon, so the marks describe different
	// instants and cannot be equated. Not agreement and not disagreement.
	if synapse > horizon || ours > horizon {
		r.Unsettled++
		return
	}

	r.Pairs++
	switch {
	case ours == synapse:
		r.Agreed++
	case ours < synapse:
		// Includes ours == 0, meaning we never routed this pair at all. The
		// dangerous direction: in production, an event a server never receives.
		r.add(Disagreement{Key: k, Synapse: synapse, Ours: ours, Kind: KindMissing})
	default:
		// We routed something Synapse did not. Synapse writes
		// destination_rooms before it does anything else with an event, so it
		// should never be the laggard below the horizon.
		r.add(Disagreement{Key: k, Synapse: synapse, Ours: ours, Kind: KindExtra})
	}
}

func (r *Report) add(d Disagreement) {
	r.Disagreements = append(r.Disagreements, d)
	r.Counts[d.Kind]++
}

func kindRank(k Kind) int {
	switch k {
	case KindMissing:
		return 0
	case KindExtra:
		return 1
	default:
		return 2
	}
}
