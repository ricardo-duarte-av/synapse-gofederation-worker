package destinations

import (
	"context"
	"fmt"
	"sort"

	"github.com/tidwall/gjson"
)

// RoomHosts is the database access this package needs.
type RoomHosts interface {
	CurrentJoinedHosts(ctx context.Context, roomID string) ([]string, error)
}

// Resolver works out where a PDU goes.
type Resolver struct {
	hosts      RoomHosts
	serverName string
	// whitelist is federation_domain_whitelist. Nil means no whitelist; an
	// empty non-nil map means send to nobody. The two are not the same and
	// Synapse allows both.
	whitelist map[string]bool
}

// NewResolver builds a Resolver.
func NewResolver(hosts RoomHosts, serverName string, whitelist map[string]bool) *Resolver {
	return &Resolver{hosts: hosts, serverName: serverName, whitelist: whitelist}
}

// Result is where an event goes and how confident we are about it.
type Result struct {
	// Destinations are the remote servers, sorted, with our own removed and
	// the whitelist applied.
	Destinations []string
	// Approximate is true when the answer came from CURRENT room state rather
	// than the state before the event.
	//
	// Synapse resolves the state at the event's prev_events
	// (federation/sender/__init__.py:661), specifically so that the last member
	// on a server still receives their own ban. We use current state, which
	// differs exactly when the event itself changes who is in the room. This
	// flag is what lets the shadow MEASURE that gap instead of guessing at it;
	// see internal/difflog.
	Approximate bool
	// RescindedInvite records that the rescinded-invite rule added a domain,
	// because it is rare enough that its absence from the diff would be
	// indistinguishable from it never firing.
	RescindedInvite bool
}

// Resolve returns the destinations for an event.
//
// eventJSON is the stored event body; authEvents are the event's auth events,
// needed only for the rescinded-invite case and only for membership leaves, so
// the caller can load them lazily.
func (r *Resolver) Resolve(ctx context.Context, eventJSON []byte, authEvents func() ([][]byte, error)) (Result, error) {
	ev := gjson.ParseBytes(eventJSON)
	roomID := ev.Get("room_id").String()

	// An event with no prev_events has no state before it, so there is nobody
	// in the room to send to (federation/sender/__init__.py:610). This is the
	// room-creation case.
	if len(ev.Get("prev_events").Array()) == 0 {
		return Result{}, nil
	}

	hosts, err := r.hosts.CurrentJoinedHosts(ctx, roomID)
	if err != nil {
		return Result{}, fmt.Errorf("destinations: %s: %w", roomID, err)
	}

	set := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		set[h] = true
	}

	res := Result{Approximate: true}

	// The rescinded-invite rule (federation/sender/__init__.py:674). A leave
	// whose sender is not its subject is a kick, a ban, an unban, or the
	// rescinding of an invite. Only the last needs telling, and the only way to
	// tell them apart is whether the auth events contain an invite for that
	// user -- if they do, the user was never joined, so their server is not in
	// the room and would otherwise never learn the invite was withdrawn.
	if ev.Get("type").String() == "m.room.member" &&
		ev.Get("content.membership").String() == "leave" {
		sender := ev.Get("sender").String()
		stateKey := ev.Get("state_key").String()
		if stateKey != "" && sender != stateKey && authEvents != nil {
			auths, err := authEvents()
			if err != nil {
				return Result{}, fmt.Errorf("destinations: auth events: %w", err)
			}
			if hasInviteFor(auths, stateKey) {
				if d := DomainOf(stateKey); d != "" && !set[d] {
					set[d] = true
					res.RescindedInvite = true
				}
			}
		}
	}

	// Our own server is not a destination, and the whitelist is applied last,
	// both as in _send_pdu (federation/sender/__init__.py:794-827).
	delete(set, r.serverName)
	out := make([]string, 0, len(set))
	for d := range set {
		if d == "" {
			continue
		}
		if r.whitelist != nil && !r.whitelist[d] {
			continue
		}
		out = append(out, d)
	}
	// Sorted so two runs over the same event produce the same list, which is
	// what makes a diff against the real sender readable.
	sort.Strings(out)
	res.Destinations = out
	return res, nil
}

// hasInviteFor reports whether any auth event is an invite for stateKey.
func hasInviteFor(authEvents [][]byte, stateKey string) bool {
	for _, raw := range authEvents {
		e := gjson.ParseBytes(raw)
		if e.Get("type").String() != "m.room.member" {
			continue
		}
		if e.Get("state_key").String() != stateKey {
			continue
		}
		if e.Get("content.membership").String() == "invite" {
			return true
		}
	}
	return false
}
