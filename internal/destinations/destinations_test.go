package destinations

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// The defaults matter more than the parsing: most events have `{}` metadata, so
// a wrong default is applied to essentially all traffic while every individual
// check still looks right.
func TestParseMetadataDefaults(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(""), []byte("{}"), []byte("not json")} {
		m := ParseMetadata(raw)
		if !m.ProactivelySend {
			t.Errorf("%q: ProactivelySend defaulted to false; that would drop almost "+
				"all federation traffic", raw)
		}
		if m.OutOfBandMembership {
			t.Errorf("%q: OutOfBandMembership defaulted to true", raw)
		}
		if m.SendOnBehalfOf != "" {
			t.Errorf("%q: SendOnBehalfOf defaulted to %q", raw, m.SendOnBehalfOf)
		}
	}
}

func TestParseMetadataReadsFlags(t *testing.T) {
	m := ParseMetadata([]byte(`{"out_of_band_membership":true,"proactively_send":false,` +
		`"send_on_behalf_of":"other.example","stream_ordering":123}`))
	if !m.OutOfBandMembership || m.ProactivelySend || m.SendOnBehalfOf != "other.example" {
		t.Errorf("got %+v", m)
	}
}

func TestEligible(t *testing.T) {
	const me = "a.example"
	cases := []struct {
		name      string
		sender    string
		meta      Metadata
		rejection string
		want      bool
		reason    SkipReason
	}{
		{"our own event", "@u:a.example", Metadata{ProactivelySend: true}, "", true, ""},
		{"remote origin", "@u:b.example", Metadata{ProactivelySend: true}, "", false, SkipNotOurs},
		// A remote-origin event IS sent when we are forwarding on its server's
		// behalf; that is what send_join does.
		{"remote but on behalf of", "@u:b.example",
			Metadata{ProactivelySend: true, SendOnBehalfOf: "b.example"}, "", true, ""},
		{"out of band membership", "@u:a.example",
			Metadata{ProactivelySend: true, OutOfBandMembership: true}, "", false, SkipOutOfBand},
		{"dummy event", "@u:a.example", Metadata{}, "", false, SkipNotProactive},
		{"rejected", "@u:a.example", Metadata{ProactivelySend: true}, "auth failed", false, SkipRejected},
	}
	for _, tc := range cases {
		ok, reason := Eligible(tc.sender, me, tc.meta, tc.rejection)
		if ok != tc.want || reason != tc.reason {
			t.Errorf("%s: Eligible = (%v, %q), want (%v, %q)", tc.name, ok, reason, tc.want, tc.reason)
		}
	}
}

func TestDomainOf(t *testing.T) {
	for id, want := range map[string]string{
		"@alice:a.example":  "a.example",
		"!room:a.example":   "a.example",
		"@a:b.example:8448": "b.example:8448",
		"nocolon":           "",
		"":                  "",
	} {
		if got := DomainOf(id); got != want {
			t.Errorf("DomainOf(%q) = %q, want %q", id, got, want)
		}
	}
}

type fakeHosts struct {
	// partial is the servers recorded at join, for a room still being joined.
	partial []string
	// hosts is the CURRENT state answer, used only when we fall back.
	hosts []string
	err   error
	// groups maps prev event id to state group; exactHosts is what that group
	// resolves to. Together they drive the exact path.
	groups     map[string]int64
	exactHosts []string
	// exactCalls counts state-group resolutions, so a test can assert the
	// exact path was taken rather than inferring it from the answer.
	exactCalls *int
}

// partial, when set, makes this a partial state room, which replaces the
// state-derived destinations with the servers recorded at join.
func (f fakeHosts) PartialStateServersAtJoin(context.Context, string) ([]string, bool, error) {
	return f.partial, len(f.partial) > 0, nil
}

func (f fakeHosts) CurrentJoinedHosts(context.Context, string) ([]string, error) {
	return f.hosts, f.err
}

func (f fakeHosts) GetStateGroupsForEvents(_ context.Context, ids []string) (map[string]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]int64{}
	for _, id := range ids {
		if g, ok := f.groups[id]; ok {
			out[id] = g
		}
	}
	return out, nil
}

func (f fakeHosts) JoinedHostsAtStateGroup(context.Context, int64) ([]string, error) {
	if f.exactCalls != nil {
		*f.exactCalls++
	}
	return f.exactHosts, f.err
}

// resolver builds a Resolver whose prev events have no state group, so it takes
// the current-state fallback. The exact path has tests of its own.
func resolver(hosts []string, whitelist map[string]bool) *Resolver {
	return NewResolver(fakeHosts{hosts: hosts}, "a.example", whitelist)
}

const messageEvent = `{
  "room_id": "!r:a.example", "type": "m.room.message", "sender": "@u:a.example",
  "prev_events": ["$prev"]
}`

func TestResolveDropsOurselvesAndSorts(t *testing.T) {
	r := resolver([]string{"z.example", "a.example", "b.example", "a.example"}, nil)
	res, err := r.Resolve(context.Background(), []byte(messageEvent), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b.example", "z.example"}
	if !reflect.DeepEqual(res.Destinations, want) {
		t.Errorf("Destinations = %v, want %v", res.Destinations, want)
	}
	if !res.Approximate {
		t.Error("Approximate is false; current-state resolution must say so")
	}
}

// An event with no prev_events has no state before it, so nobody is in the room.
func TestResolveNoPrevEventsMeansNobody(t *testing.T) {
	r := resolver([]string{"b.example"}, nil)
	res, err := r.Resolve(context.Background(),
		[]byte(`{"room_id":"!r:a.example","type":"m.room.create","sender":"@u:a.example","prev_events":[]}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Destinations) != 0 {
		t.Errorf("Destinations = %v, want none", res.Destinations)
	}
}

// Nil and empty whitelists mean opposite things.
func TestResolveWhitelist(t *testing.T) {
	hosts := []string{"b.example", "c.example"}

	res, _ := resolver(hosts, nil).Resolve(context.Background(), []byte(messageEvent), nil)
	if len(res.Destinations) != 2 {
		t.Errorf("no whitelist should send to everyone, got %v", res.Destinations)
	}

	res, _ = resolver(hosts, map[string]bool{}).Resolve(context.Background(), []byte(messageEvent), nil)
	if len(res.Destinations) != 0 {
		t.Errorf("an empty whitelist should send to nobody, got %v", res.Destinations)
	}

	res, _ = resolver(hosts, map[string]bool{"b.example": true}).
		Resolve(context.Background(), []byte(messageEvent), nil)
	if !reflect.DeepEqual(res.Destinations, []string{"b.example"}) {
		t.Errorf("got %v, want [b.example]", res.Destinations)
	}
}

const rescindedInvite = `{
  "room_id": "!r:a.example", "type": "m.room.member", "sender": "@admin:a.example",
  "state_key": "@invitee:c.example", "content": {"membership": "leave"},
  "prev_events": ["$prev"]
}`

// A leave whose sender is not its subject is a kick, a ban, an unban, or a
// rescinded invite. Only the last adds a destination, and only the auth events
// distinguish them.
func TestResolveRescindedInvite(t *testing.T) {
	auth := func(membership string) func() ([][]byte, error) {
		return func() ([][]byte, error) {
			return [][]byte{
				[]byte(`{"type":"m.room.power_levels","state_key":""}`),
				[]byte(`{"type":"m.room.member","state_key":"@invitee:c.example",` +
					`"content":{"membership":"` + membership + `"}}`),
			}, nil
		}
	}

	r := resolver([]string{"b.example"}, nil)
	res, err := r.Resolve(context.Background(), []byte(rescindedInvite), auth("invite"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Destinations, []string{"b.example", "c.example"}) {
		t.Errorf("Destinations = %v, want the invitee's server added", res.Destinations)
	}
	if !res.RescindedInvite {
		t.Error("RescindedInvite is false")
	}

	// A kick: the user was joined, so their server is already in the room and
	// nothing is added.
	res, err = r.Resolve(context.Background(), []byte(rescindedInvite), auth("join"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Destinations, []string{"b.example"}) {
		t.Errorf("Destinations = %v, want no addition for a kick", res.Destinations)
	}
	if res.RescindedInvite {
		t.Error("RescindedInvite is true for a kick")
	}
}

// A self-leave is somebody leaving, not an invite being withdrawn.
func TestResolveSelfLeaveIsNotARescindedInvite(t *testing.T) {
	called := false
	auth := func() ([][]byte, error) {
		called = true
		return nil, nil
	}
	r := resolver([]string{"b.example"}, nil)
	_, err := r.Resolve(context.Background(), []byte(`{
	  "room_id":"!r:a.example","type":"m.room.member","sender":"@u:c.example",
	  "state_key":"@u:c.example","content":{"membership":"leave"},"prev_events":["$p"]}`), auth)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("auth events were loaded for a self-leave; the caller loads them lazily " +
			"and this is the case that makes that worth doing")
	}
}

func TestResolvePropagatesErrors(t *testing.T) {
	r := NewResolver(fakeHosts{err: errors.New("boom")}, "a.example", nil)
	if _, err := r.Resolve(context.Background(), []byte(messageEvent), nil); err == nil {
		t.Fatal("expected the host lookup error to propagate")
	}
}

// A room still being joined has no state to resolve against, so the servers
// recorded at join REPLACE the computed set -- Synapse checks this first, before
// its caches and before resolving (federation/sender/__init__.py:614).
//
// Under-counting is the failure that matters: during a faster join we hold only
// part of the room's membership, so computing hosts from it means our own
// events silently never reach servers that are in the room.
func TestPartialStateRoomUsesTheServersRecordedAtJoin(t *testing.T) {
	r := NewResolver(fakeHosts{
		// What we can see is incomplete during a partial join, and must not win.
		hosts:   []string{"seen.example"},
		partial: []string{"atjoin.example", "other.example", "example.com"},
	}, "example.com", nil)

	res, err := r.Resolve(context.Background(), []byte(`{
		"room_id":"!r:example.com","type":"m.room.message","sender":"@a:example.com",
		"prev_events":["$p"]}`), nil)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"atjoin.example", "other.example"}
	if len(res.Destinations) != len(want) {
		t.Fatalf("destinations = %v, want %v", res.Destinations, want)
	}
	for i, d := range want {
		if res.Destinations[i] != d {
			t.Errorf("destinations = %v, want %v", res.Destinations, want)
			break
		}
	}
	// Recorded as approximate, with a cause of its own: this is not a shortcut
	// we chose, there was no state to resolve.
	if !res.Approximate {
		t.Error("a partial state room was not marked approximate")
	}
	if res.Fallback != FallbackPartialState {
		t.Errorf("fallback = %q, want %q", res.Fallback, FallbackPartialState)
	}
}

// A fully-stated room must be unaffected: the partial list is empty, so state
// resolution runs exactly as before.
func TestFullStateRoomIsUnaffectedByThePartialCheck(t *testing.T) {
	r := NewResolver(fakeHosts{hosts: []string{"seen.example", "example.com"}},
		"example.com", nil)

	res, err := r.Resolve(context.Background(), []byte(`{
		"room_id":"!r:example.com","type":"m.room.message","sender":"@a:example.com",
		"prev_events":["$p"]}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Destinations) != 1 || res.Destinations[0] != "seen.example" {
		t.Errorf("destinations = %v, want [seen.example]", res.Destinations)
	}
	if res.Fallback == FallbackPartialState {
		t.Error("a fully-stated room was treated as partial")
	}
}
