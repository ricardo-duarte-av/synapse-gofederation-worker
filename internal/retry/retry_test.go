package retry

import (
	"context"
	"testing"
	"time"
)

func limiter(t *testing.T) *Limiter {
	t.Helper()
	return New(Config{
		MinInterval: 30 * time.Second,
		Multiplier:  5,
		MaxInterval: 12 * time.Hour,
	}, nil, nil)
}

// A destination that has never failed is always due. Synapse reads a falsy
// retry_last_ts that way regardless of what the interval says.
func TestNeverFailedIsDue(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)
	if !l.Due(ctx, "fresh.example") {
		t.Error("a destination with no history was refused")
	}
	if !(Timings{RetryInterval: 60_000}).Due(time.Now().UnixMilli()) {
		t.Error("an interval with no last attempt should still be due")
	}
}

// The point of the package: a failure stops the destination being attempted,
// and the interval grows on each subsequent one.
func TestBackoffGrowsAndBlocks(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)

	first := l.Failure(ctx, "dead.example")
	if first.RetryInterval != (30 * time.Second).Milliseconds() {
		t.Errorf("first backoff = %dms, want the configured minimum", first.RetryInterval)
	}
	if l.Due(ctx, "dead.example") {
		t.Error("a destination that just failed is still due")
	}

	second := l.Failure(ctx, "dead.example")
	if second.RetryInterval <= first.RetryInterval {
		t.Errorf("backoff did not grow: %d then %d", first.RetryInterval, second.RetryInterval)
	}
	// Multiplier 5 with jitter 0.8-1.4 puts the second between 4x and 7x.
	ratio := float64(second.RetryInterval) / float64(first.RetryInterval)
	if ratio < 4 || ratio > 7 {
		t.Errorf("growth ratio %.2f is outside multiplier*jitter", ratio)
	}
	// The failure timestamp marks the START of the run, not the last attempt,
	// so "how long has this been broken" stays answerable.
	if second.FailureTS != first.FailureTS {
		t.Errorf("FailureTS moved from %d to %d", first.FailureTS, second.FailureTS)
	}
}

// A dead server must eventually settle at the cap rather than growing without
// bound; twelve hours, not twelve years.
func TestBackoffIsCapped(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)
	for i := 0; i < 40; i++ {
		l.Failure(ctx, "gone.example")
	}
	got := l.Timings("gone.example").RetryInterval
	if got != (12 * time.Hour).Milliseconds() {
		t.Errorf("interval settled at %dms, want the 12h cap", got)
	}
}

// One working request is enough evidence. A decay would keep a recovered
// server throttled for as long as it had been broken.
func TestSuccessClearsCompletely(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)
	for i := 0; i < 5; i++ {
		l.Failure(ctx, "flaky.example")
	}
	l.Success(ctx, "flaky.example")

	if got := l.Timings("flaky.example"); got.RetryInterval != 0 || got.RetryLastTS != 0 || got.FailureTS != 0 {
		t.Errorf("after success: %+v, want everything cleared", got)
	}
	if !l.Due(ctx, "flaky.example") {
		t.Error("a recovered destination is not due")
	}
}

// Success on a healthy destination must not write or notify, or every
// transaction would hit the database and wake queues that were never asleep.
func TestSuccessOnAHealthyDestinationIsANoOp(t *testing.T) {
	ctx := context.Background()
	writes, wakes := 0, 0
	l := New(Config{MinInterval: time.Second, Multiplier: 2, MaxInterval: time.Hour}, nil,
		func(context.Context, string, Timings) error { writes++; return nil })
	l.SetOnRecovered(func(string) { wakes++ })

	for i := 0; i < 10; i++ {
		l.Success(ctx, "fine.example")
	}
	if writes != 0 || wakes != 0 {
		t.Errorf("%d writes and %d wakes for a destination that never failed", writes, wakes)
	}

	// But a real recovery does both, exactly once.
	l.Failure(ctx, "fine.example")
	writes = 0
	l.Success(ctx, "fine.example")
	if writes != 1 || wakes != 1 {
		t.Errorf("recovery produced %d writes and %d wakes, want 1 each", writes, wakes)
	}
}

// A restart must not forget every backoff, or the first event after a deploy
// retries thousands of dead servers at once -- the stampede the backoff exists
// to prevent, delivered on a schedule.
func TestPersistedStateIsSeededOnFirstUse(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()
	loads := 0
	l := New(Config{MinInterval: time.Minute, Multiplier: 2, MaxInterval: time.Hour},
		func(_ context.Context, dests []string) (map[string]Timings, error) {
			loads++
			return map[string]Timings{
				"dead.example": {FailureTS: now, RetryLastTS: now, RetryInterval: 3_600_000},
			}, nil
		}, nil)

	if l.Due(ctx, "dead.example") {
		t.Error("a destination backing off in the database was treated as fresh")
	}
	// Seeded once, not on every send.
	for i := 0; i < 5; i++ {
		l.Due(ctx, "dead.example")
	}
	if loads != 1 {
		t.Errorf("the database was read %d times, want once per destination", loads)
	}
}

// Another worker seeing the server work is reason enough to drop our backoff:
// retrying once and failing is cheap next to leaving a working server
// unreachable for hours.
func TestRecoveredClearsTheBackoff(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)
	l.Failure(ctx, "back.example")
	if l.Due(ctx, "back.example") {
		t.Fatal("precondition: should be backing off")
	}
	l.Recovered(ctx, "back.example")
	if !l.Due(ctx, "back.example") {
		t.Error("REMOTE_SERVER_UP did not clear the backoff")
	}
}

// The clear must be PERSISTED, not only held in memory. Otherwise a server
// somebody else has seen working goes straight back into its old backoff on the
// next restart, read from a row recording an outage that is over -- and with a
// destination_max_retry_interval of 365d, as this deployment uses, "the next
// restart" can mean a year of never trying.
func TestRecoveredPersistsTheClear(t *testing.T) {
	ctx := context.Background()
	written := map[string]Timings{}
	l := New(Config{MinInterval: time.Minute, Multiplier: 2, MaxInterval: time.Hour},
		nil,
		func(_ context.Context, d string, timings Timings) error {
			written[d] = timings
			return nil
		})

	l.Failure(ctx, "back.example")
	if written["back.example"].RetryInterval == 0 {
		t.Fatal("precondition: the failure was not persisted")
	}

	var woken string
	l.SetOnRecovered(func(d string) { woken = d })
	l.Recovered(ctx, "back.example")

	if got := written["back.example"]; got != (Timings{}) {
		t.Errorf("persisted %+v, want the backoff cleared on disk too", got)
	}
	if woken != "back.example" {
		t.Error("the recovery callback did not fire, so the queue is never woken")
	}
}

func TestBackoffCount(t *testing.T) {
	ctx := context.Background()
	l := limiter(t)
	l.Failure(ctx, "a.example")
	l.Failure(ctx, "b.example")
	l.Success(ctx, "a.example")
	if got := l.Backoff(); got != 1 {
		t.Errorf("Backoff = %d, want 1", got)
	}
}

// The hour of slack is Synapse's retry_due_within_ms, passed as
// CATCHUP_RETRY_INTERVAL by every sender before it decides whether to build an
// EDU at all. Without it a destination coming back in two minutes is skipped
// now and not reconsidered until something else happens to be routed to it.
func TestDueWithinLookahead(t *testing.T) {
	ctx := context.Background()
	l := New(Config{MinInterval: 10 * time.Minute, Multiplier: 2, MaxInterval: time.Hour}, nil, nil)

	l.Failure(ctx, "soon.example") // 10 minutes out
	if l.Due(ctx, "soon.example") {
		t.Fatal("precondition: should not be due now")
	}
	if !l.DueWithin(ctx, "soon.example", time.Hour) {
		t.Error("a destination due in 10 minutes was excluded by an hour of slack")
	}
	if l.DueWithin(ctx, "soon.example", time.Minute) {
		t.Error("a destination due in 10 minutes was included by a minute of slack")
	}

	// A destination that has never failed is due under any slack, including
	// none: zero timings must not be read as "backing off since the epoch".
	if !l.DueWithin(ctx, "fresh.example", 0) {
		t.Error("an unknown destination was treated as backing off")
	}
}
