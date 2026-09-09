package destinations

import (
	"context"
	"fmt"

	"github.com/tidwall/gjson"
)

// FallbackReason says why an exact answer was not available.
type FallbackReason string

// The two reasons, and only two. Anything else should be an error rather than a
// silent approximation.
const (
	// FallbackForkedDAG means the prev events sit on two or more distinct state
	// groups. An exact answer needs full state resolution, which is
	// room-version-specific and expensive; Synapse does it, we do not yet.
	FallbackForkedDAG FallbackReason = "forked dag"
	// FallbackNoStateGroup means a prev event has no state group, which means
	// it is an outlier -- we have the event but not the state around it.
	FallbackNoStateGroup FallbackReason = "prev event has no state group"
	// FallbackPartialState means the room was still being joined, so the
	// servers recorded at join were used instead of resolving state. Counted as
	// approximate for the same reason as the others -- it is a set that may
	// include servers that have left and miss servers that arrived -- but it is
	// not a shortcut we chose: during a partial join there is no state to
	// resolve, and Synapse does the same (federation/sender/__init__.py:614).
	FallbackPartialState FallbackReason = "partial state room"
)

// hostsBeforeEvent resolves who was in the room immediately before an event.
//
// The state group recorded against an event is the state AFTER it, so the state
// before THIS event is the state after its prev events. When they all share one
// state group there is nothing to resolve and the answer is exact -- which is
// the case for the overwhelming majority of events.
//
// Otherwise we fall back to current state and say so. Falling back silently
// would be the worst of both: an answer that is usually right, occasionally
// wrong, and never distinguishable.
func (r *Resolver) hostsBeforeEvent(ctx context.Context, roomID string, prevIDs []string) (
	hosts []string, approximate bool, fallback FallbackReason, err error,
) {
	if len(prevIDs) > 0 {
		groups, err := r.hosts.GetStateGroupsForEvents(ctx, prevIDs)
		if err != nil {
			return nil, false, "", fmt.Errorf("destinations: %s: %w", roomID, err)
		}

		group, ok := singleGroup(groups, prevIDs)
		switch {
		case ok:
			hosts, err := r.hosts.JoinedHostsAtStateGroup(ctx, group)
			if err != nil {
				return nil, false, "", fmt.Errorf("destinations: %s: %w", roomID, err)
			}
			return hosts, false, "", nil
		case len(groups) < len(prevIDs):
			fallback = FallbackNoStateGroup
		default:
			fallback = FallbackForkedDAG
		}
	}

	hosts, err = r.hosts.CurrentJoinedHosts(ctx, roomID)
	if err != nil {
		return nil, false, "", fmt.Errorf("destinations: %s: %w", roomID, err)
	}
	return hosts, true, fallback, nil
}

// singleGroup reports the one state group shared by every prev event, if there
// is one.
//
// Every prev event must be present AND they must agree. A missing one means an
// outlier, and taking the group of the others would answer confidently from
// half the DAG.
func singleGroup(groups map[string]int64, prevIDs []string) (int64, bool) {
	if len(groups) != len(prevIDs) {
		return 0, false
	}
	var first int64
	for i, id := range prevIDs {
		g, ok := groups[id]
		if !ok {
			return 0, false
		}
		if i == 0 {
			first = g
			continue
		}
		if g != first {
			return 0, false
		}
	}
	return first, true
}

// prevEventIDs reads an event's prev_events.
//
// Room versions 1 and 2 write them as [event_id, hashes] pairs; every later
// version writes bare event ids. Both still exist in a database this old.
func prevEventIDs(ev gjson.Result) []string {
	refs := ev.Get("prev_events").Array()
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		if r.IsArray() {
			if a := r.Array(); len(a) > 0 {
				out = append(out, a[0].String())
			}
			continue
		}
		if s := r.String(); s != "" {
			out = append(out, s)
		}
	}
	return out
}
