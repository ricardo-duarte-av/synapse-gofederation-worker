package store

import (
	"testing"
	"time"
)

func TestDueAt(t *testing.T) {
	const now = 1_000_000

	cases := map[string]struct {
		timings RetryTimings
		want    bool
	}{
		// No retry_last_ts means the destination has never failed. Synapse's
		// get_retry_limiter treats a falsy retry_last_ts as "not retrying" even
		// when a retry_interval is somehow set, so a row like the second case
		// is due despite the interval.
		"never failed":            {RetryTimings{}, true},
		"interval but no last ts": {RetryTimings{RetryInterval: 60_000}, true},
		"backoff still running":   {RetryTimings{RetryLastTS: now - 1000, RetryInterval: 60_000}, false},
		"backoff expired":         {RetryTimings{RetryLastTS: now - 60_001, RetryInterval: 60_000}, true},
		// The boundary is inclusive: due exactly when the interval elapses.
		"exactly due": {RetryTimings{RetryLastTS: now - 60_000, RetryInterval: 60_000}, true},
	}
	for name, tc := range cases {
		if got := tc.timings.DueAt(now); got != tc.want {
			t.Errorf("%s: DueAt = %v, want %v", name, got, tc.want)
		}
	}
}

func TestFilterDestinationsByRetryLimiter(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	dests := []string{"due.example", "backing-off.example", "soon.example", "unknown.example"}
	timings := map[string]RetryTimings{
		"due.example":         {RetryLastTS: 900_000, RetryInterval: 10_000},
		"backing-off.example": {RetryLastTS: 999_000, RetryInterval: 86_400_000},
		// Not due now, but due within the hour of slack the sender passes.
		"soon.example": {RetryLastTS: 999_000, RetryInterval: 600_000},
	}

	// With no slack, only the genuinely due one survives.
	got := FilterDestinationsByRetryLimiter(dests, timings, now, 0)
	want := map[string]bool{"due.example": true, "unknown.example": true}
	if len(got) != len(want) {
		t.Fatalf("no slack: got %v, want %v", got, want)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("no slack: %q should not be due", d)
		}
	}

	// The hour of slack is what stops a recovering server waiting an extra
	// full interval, so it must pull soon.example in.
	got = FilterDestinationsByRetryLimiter(dests, timings, now, time.Hour)
	if len(got) != 3 {
		t.Fatalf("with an hour of slack: got %v, want three destinations", got)
	}
	for _, d := range got {
		if d == "backing-off.example" {
			t.Error("a destination backing off for a day was included")
		}
	}
}

// A destination with no row has never been heard from and is due.
func TestFilterKeepsUnknownDestinations(t *testing.T) {
	got := FilterDestinationsByRetryLimiter([]string{"new.example"}, nil, time.Now(), 0)
	if len(got) != 1 || got[0] != "new.example" {
		t.Errorf("got %v, want [new.example]", got)
	}
}
