package destinations

import (
	"context"
	"reflect"
	"testing"
)

const forkedEvent = `{
  "room_id": "!r:a.example", "type": "m.room.message", "sender": "@u:a.example",
  "prev_events": ["$p1", "$p2"]
}`

// The exact path: prev events share one state group, so the answer is the state
// BEFORE the event, which is what Synapse uses.
func TestResolvesFromStateBeforeTheEvent(t *testing.T) {
	calls := 0
	r := NewResolver(fakeHosts{
		hosts:      []string{"current-only.example"},
		groups:     map[string]int64{"$prev": 77},
		exactHosts: []string{"b.example", "a.example"},
		exactCalls: &calls,
	}, "a.example", nil)

	res, err := r.Resolve(context.Background(), []byte(messageEvent), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Approximate {
		t.Error("Approximate is true; a single state group needs no resolution")
	}
	if res.Fallback != "" {
		t.Errorf("Fallback = %q, want empty", res.Fallback)
	}
	if calls != 1 {
		t.Errorf("JoinedHostsAtStateGroup called %d times, want 1", calls)
	}
	// The exact answer, with our own server dropped -- not the current-state one.
	if !reflect.DeepEqual(res.Destinations, []string{"b.example"}) {
		t.Errorf("Destinations = %v, want the state-before answer", res.Destinations)
	}
}

// A genuine DAG fork needs full state resolution, which we do not do yet. The
// fallback is taken and NAMED, because a silent one would be an answer that is
// usually right, occasionally wrong, and never distinguishable.
func TestForkedDAGFallsBackAndSaysSo(t *testing.T) {
	calls := 0
	r := NewResolver(fakeHosts{
		hosts:      []string{"c.example"},
		groups:     map[string]int64{"$p1": 1, "$p2": 2},
		exactHosts: []string{"b.example"},
		exactCalls: &calls,
	}, "a.example", nil)

	res, err := r.Resolve(context.Background(), []byte(forkedEvent), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Approximate {
		t.Error("Approximate is false for two distinct state groups")
	}
	if res.Fallback != FallbackForkedDAG {
		t.Errorf("Fallback = %q, want %q", res.Fallback, FallbackForkedDAG)
	}
	if calls != 0 {
		t.Error("resolved a state group despite the fork; that would answer from one branch")
	}
	if !reflect.DeepEqual(res.Destinations, []string{"c.example"}) {
		t.Errorf("Destinations = %v, want the current-state answer", res.Destinations)
	}
}

// Prev events that AGREE are exact even when there are several of them.
func TestSeveralPrevEventsOnOneGroupIsExact(t *testing.T) {
	r := NewResolver(fakeHosts{
		groups:     map[string]int64{"$p1": 9, "$p2": 9},
		exactHosts: []string{"b.example"},
	}, "a.example", nil)

	res, err := r.Resolve(context.Background(), []byte(forkedEvent), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Approximate {
		t.Errorf("Approximate is true although both prev events share group 9: %+v", res)
	}
}

// A prev event with no state group is an outlier: we have the event but not the
// state around it. Taking the other prev event's group would answer confidently
// from half the DAG.
func TestMissingStateGroupFallsBack(t *testing.T) {
	calls := 0
	r := NewResolver(fakeHosts{
		hosts:      []string{"c.example"},
		groups:     map[string]int64{"$p1": 5},
		exactHosts: []string{"b.example"},
		exactCalls: &calls,
	}, "a.example", nil)

	res, err := r.Resolve(context.Background(), []byte(forkedEvent), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Approximate || res.Fallback != FallbackNoStateGroup {
		t.Errorf("got Approximate=%v Fallback=%q, want the no-state-group fallback",
			res.Approximate, res.Fallback)
	}
	if calls != 0 {
		t.Error("resolved a state group with one prev event missing")
	}
}

func TestSingleGroup(t *testing.T) {
	cases := []struct {
		name   string
		groups map[string]int64
		prev   []string
		want   int64
		ok     bool
	}{
		{"one event", map[string]int64{"a": 7}, []string{"a"}, 7, true},
		{"two agreeing", map[string]int64{"a": 7, "b": 7}, []string{"a", "b"}, 7, true},
		{"two disagreeing", map[string]int64{"a": 7, "b": 8}, []string{"a", "b"}, 0, false},
		{"one missing", map[string]int64{"a": 7}, []string{"a", "b"}, 0, false},
		{"all missing", map[string]int64{}, []string{"a"}, 0, false},
		// A stale extra entry must not make a missing prev event look present.
		{"extra unrelated entry", map[string]int64{"a": 7, "z": 7}, []string{"a", "b"}, 0, false},
	}
	for _, tc := range cases {
		got, ok := singleGroup(tc.groups, tc.prev)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: singleGroup = (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// Room versions 1 and 2 write prev_events as [event_id, hashes] pairs; later
// versions write bare ids. Both still exist in a database this old.
func TestPrevEventIDsHandlesBothFormats(t *testing.T) {
	r := NewResolver(fakeHosts{
		groups:     map[string]int64{"$old": 3},
		exactHosts: []string{"b.example"},
	}, "a.example", nil)

	res, err := r.Resolve(context.Background(), []byte(`{
	  "room_id":"!r:a.example","type":"m.room.message","sender":"@u:a.example",
	  "prev_events":[["$old",{"sha256":"abc"}]]}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Approximate {
		t.Error("the v1 prev_events format was not parsed; it fell back")
	}
}
