package compare

import (
	"context"
	"testing"
)

type fakeRows struct {
	synapse map[Key]int64
	ours    map[Key]int64
}

// Both return everything; windowing is Compare's job.
func (f fakeRows) SynapseRoutes(context.Context) (map[Key]int64, error) { return f.synapse, nil }
func (f fakeRows) OurRoutes(context.Context) (map[Key]int64, error)     { return f.ours, nil }

func key(dest, room string) Key { return Key{Destination: dest, RoomID: room} }

func TestAgreementIsExactMatch(t *testing.T) {
	c := New(fakeRows{
		synapse: map[Key]int64{key("b.example", "!r"): 100, key("c.example", "!r"): 100},
		ours:    map[Key]int64{key("b.example", "!r"): 100, key("c.example", "!r"): 100},
	}, 10)

	rep, err := c.Compare(context.Background(), 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pairs != 2 || rep.Agreed != 2 || len(rep.Disagreements) != 0 {
		t.Errorf("got %+v", rep)
	}
	if rep.AgreementRate() != 1 {
		t.Errorf("AgreementRate = %v", rep.AgreementRate())
	}
}

// The dangerous direction: Synapse routed something we did not. In production
// that is an event a server never receives.
func TestMissingIsReportedAsMissing(t *testing.T) {
	c := New(fakeRows{
		synapse: map[Key]int64{
			key("b.example", "!r"):    100,
			key("gone.example", "!r"): 100,
		},
		ours: map[Key]int64{key("b.example", "!r"): 50},
	}, 10)

	rep, err := c.Compare(context.Background(), 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Counts[KindMissing] != 2 {
		t.Fatalf("Counts = %v, want two missing (one behind, one absent)", rep.Counts)
	}
	// Absent counts as missing with Ours == 0, which is what distinguishes
	// "we never routed this at all" from "we are behind".
	var absent, behind bool
	for _, d := range rep.Disagreements {
		if d.Destination == "gone.example" && d.Ours == 0 {
			absent = true
		}
		if d.Destination == "b.example" && d.Ours == 50 && d.Synapse == 100 {
			behind = true
		}
	}
	if !absent || !behind {
		t.Errorf("disagreements do not distinguish absent from behind: %+v", rep.Disagreements)
	}
	// Missing sorts first, because it is the only unambiguously dangerous kind.
	if rep.Disagreements[0].Kind != KindMissing {
		t.Errorf("most significant disagreement is %q", rep.Disagreements[0].Kind)
	}
}

// A pair we have and Synapse does not, below the horizon. Synapse writes
// destination_rooms before it does anything else with an event, so it should
// never be the laggard here.
func TestExtraIsReported(t *testing.T) {
	c := New(fakeRows{
		synapse: map[Key]int64{},
		ours:    map[Key]int64{key("phantom.example", "!r"): 100},
	}, 10)

	rep, err := c.Compare(context.Background(), 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Counts[KindExtra] != 1 || rep.Pairs != 1 {
		t.Errorf("got %+v", rep)
	}
}

// The settle window is what stops normal clock skew near the tip being reported
// as disagreement. Both senders work the same stream at their own pace.
func TestSettleWindowExcludesTheTip(t *testing.T) {
	rows := fakeRows{
		// Synapse has reached 195; we have only reached 150. Below the
		// horizon of 200-60=140 both agree.
		synapse: map[Key]int64{key("b.example", "!r"): 195, key("c.example", "!r"): 100},
		ours:    map[Key]int64{key("b.example", "!r"): 150, key("c.example", "!r"): 100},
	}

	// With no settle window the tip disagreement is reported.
	rep, _ := New(rows, 1).Compare(context.Background(), 0, 200)
	if rep.Counts[KindMissing] == 0 {
		t.Error("without a settle window the tip disagreement should be visible")
	}

	// With a window that excludes it, only the settled pair is compared.
	rep, _ = New(rows, 60).Compare(context.Background(), 0, 200)
	if rep.Horizon != 140 {
		t.Errorf("Horizon = %d, want 140", rep.Horizon)
	}
	if len(rep.Disagreements) != 0 {
		t.Errorf("settled window still reports disagreements: %+v", rep.Disagreements)
	}
	if rep.Pairs != 1 {
		t.Errorf("Pairs = %d, want only the settled pair", rep.Pairs)
	}
}

// Nothing to disagree about is not a failure.
func TestEmptyComparisonIsFullAgreement(t *testing.T) {
	rep, err := New(fakeRows{}, 10).Compare(context.Background(), 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if rep.AgreementRate() != 1 {
		t.Errorf("AgreementRate = %v for an empty comparison", rep.AgreementRate())
	}
}

// Before enough events exist to draw a horizon, the comparison is a no-op
// rather than an error or a false pass over zero rows.
func TestHorizonBelowZeroIsANoOp(t *testing.T) {
	rep, err := New(fakeRows{
		synapse: map[Key]int64{key("b.example", "!r"): 1},
	}, 5000).Compare(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pairs != 0 || rep.Horizon != 0 {
		t.Errorf("got %+v", rep)
	}
}

// The floor is what makes the comparison mean anything. Synapse's
// destination_rooms is a high-water mark spanning the homeserver's whole
// lifetime; ours starts when this worker first ran. Without a floor every pair
// Synapse routed before we existed reads as "missing" -- on the first real run
// that was 19,314 of 19,822 pairs.
func TestFloorExcludesHistoryFromBeforeWeRan(t *testing.T) {
	rows := fakeRows{
		synapse: map[Key]int64{
			// Ancient: Synapse routed to this pair long before we started.
			key("old.example", "!r"): 500,
			// In our window and agreed.
			key("b.example", "!r"): 1500,
			// In our window and genuinely missed.
			key("missed.example", "!r"): 1600,
		},
		ours: map[Key]int64{key("b.example", "!r"): 1500},
	}

	// No floor: the ancient pair is reported as a miss.
	rep, _ := New(rows, 10).Compare(context.Background(), 0, 2000)
	if rep.Counts[KindMissing] != 2 {
		t.Fatalf("without a floor, got %v, want both the ancient and the real miss", rep.Counts)
	}

	// With a floor at the start of our window, only the real miss remains.
	rep, _ = New(rows, 10).Compare(context.Background(), 1000, 2000)
	if rep.Counts[KindMissing] != 1 {
		t.Errorf("Counts = %v, want only the genuine miss", rep.Counts)
	}
	if rep.Skipped != 1 {
		t.Errorf("Skipped = %d, want the one pair below the floor", rep.Skipped)
	}
	if rep.Pairs != 2 || rep.Agreed != 1 {
		t.Errorf("Pairs = %d, Agreed = %d, want 2 and 1", rep.Pairs, rep.Agreed)
	}
	for _, d := range rep.Disagreements {
		if d.Destination == "old.example" {
			t.Error("a pair from before we ran was still compared")
		}
	}
}

// A pair Synapse last touched before the floor but that WE routed inside our
// window is a real disagreement, not history: Synapse should have moved its
// high-water mark too.
func TestFloorKeepsPairsWeRoutedRecently(t *testing.T) {
	rep, _ := New(fakeRows{
		synapse: map[Key]int64{key("b.example", "!r"): 500},
		ours:    map[Key]int64{key("b.example", "!r"): 1500},
	}, 10).Compare(context.Background(), 1000, 2000)

	if rep.Counts[KindExtra] != 1 {
		t.Errorf("Counts = %v, want one extra; we routed inside the window and Synapse did not",
			rep.Counts)
	}
	if rep.Skipped != 0 {
		t.Errorf("Skipped = %d, want 0", rep.Skipped)
	}
}

// The mistake the first version made, pinned so it cannot come back.
//
// These tables are high-water marks, not logs. A pair whose mark has moved past
// the horizon cannot be compared for equality -- the two marks describe
// different instants, and the row holds no way to recover what the value was AT
// the horizon. On production this reported a pair as "extra" that Synapse had
// in fact routed, 361 orderings later than our copy of it.
func TestMarksBeyondTheHorizonAreUnsettledNotDisagreements(t *testing.T) {
	rep, _ := New(fakeRows{
		// Synapse has moved on past the horizon for this pair; we recorded it
		// earlier. Nothing here is wrong.
		synapse: map[Key]int64{key("b.example", "!r"): 1900},
		ours:    map[Key]int64{key("b.example", "!r"): 1500},
		// settle 200 puts the horizon at 1800, below Synapse's mark of 1900.
	}, 200).Compare(context.Background(), 1000, 2000)

	if len(rep.Disagreements) != 0 {
		t.Errorf("a mark beyond the horizon was reported as %+v", rep.Disagreements)
	}
	if rep.Unsettled != 1 {
		t.Errorf("Unsettled = %d, want 1", rep.Unsettled)
	}
	if rep.Pairs != 0 {
		t.Errorf("Pairs = %d; an unsettled pair must not count towards the rate", rep.Pairs)
	}
}

// And the same when it is OUR mark that has moved past the horizon.
func TestOurMarkBeyondTheHorizonIsAlsoUnsettled(t *testing.T) {
	rep, _ := New(fakeRows{
		synapse: map[Key]int64{key("b.example", "!r"): 1500},
		ours:    map[Key]int64{key("b.example", "!r"): 1950},
	}, 100).Compare(context.Background(), 1000, 2000)

	if rep.Unsettled != 1 || len(rep.Disagreements) != 0 {
		t.Errorf("got unsettled=%d disagreements=%+v", rep.Unsettled, rep.Disagreements)
	}
}

// A genuine miss must still be caught once both sides are settled.
func TestGenuineMissSurvivesTheUnsettledFilter(t *testing.T) {
	rep, _ := New(fakeRows{
		synapse: map[Key]int64{key("b.example", "!r"): 1500},
		ours:    map[Key]int64{},
	}, 100).Compare(context.Background(), 1000, 2000)

	if rep.Counts[KindMissing] != 1 {
		t.Errorf("Counts = %v, want the miss to survive", rep.Counts)
	}
}
