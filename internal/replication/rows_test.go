package replication

import (
	"testing"

	"github.com/tidwall/gjson"
)

// Row shapes come from replication/tcp/streams/events.py:107,122,132. The field
// order of an "ev" row is checked here explicitly because a positional array
// gives no error when it is read wrong -- rejected and outlier sit at indices 7
// and 8, and reading them one apart would quietly mean "outlier" whenever
// Synapse said "rejected".
func TestParseEventRow(t *testing.T) {
	ev, ok := ParseEventRow(
		`["ev",["$eid","!room:a.example","m.room.member","@u:a.example",null,null,"join",false,true]]`)
	if !ok {
		t.Fatal("did not parse")
	}
	if ev.Kind != "ev" || ev.EventID != "$eid" || ev.RoomID != "!room:a.example" {
		t.Errorf("got %+v", ev)
	}
	if ev.Type != "m.room.member" || ev.StateKey != "@u:a.example" {
		t.Errorf("type/state_key wrong: %+v", ev)
	}
	if ev.Rejected {
		t.Error("Rejected is true; index 7 was false")
	}
	if !ev.Outlier {
		t.Error("Outlier is false; index 8 was true")
	}

	// And the other way round, so the two cannot be swapped and still pass.
	ev, _ = ParseEventRow(
		`["ev",["$e","!r:a.example","m.room.message","",null,null,null,true,false]]`)
	if !ev.Rejected || ev.Outlier {
		t.Errorf("rejected/outlier swapped: %+v", ev)
	}
}

func TestParseEventRowStateVariants(t *testing.T) {
	ev, ok := ParseEventRow(`["state",["!room:a.example","m.room.name","","$eid"]]`)
	if !ok {
		t.Fatal("did not parse a state row")
	}
	if ev.Kind != "state" || ev.RoomID != "!room:a.example" || ev.Type != "m.room.name" ||
		ev.EventID != "$eid" {
		t.Errorf("got %+v", ev)
	}

	ev, ok = ParseEventRow(`["state-all",["!room:a.example"]]`)
	if !ok {
		t.Fatal("did not parse a state-all row")
	}
	if ev.Kind != "state-all" || ev.RoomID != "!room:a.example" {
		t.Errorf("got %+v", ev)
	}
}

func TestParseEventRowRejectsJunk(t *testing.T) {
	for _, row := range []string{
		``, `{}`, `[]`, `["ev"]`, `["ev",[]]`, `["unknown",["!r"]]`, `not json`,
	} {
		if _, ok := ParseEventRow(row); ok {
			t.Errorf("%q parsed but should not have", row)
		}
	}
}

// A sender wants only the remote-server entities; Synapse distinguishes them by
// the leading "@" rather than by a lookup.
func TestParseToDeviceEntity(t *testing.T) {
	entity, remote, ok := ParseToDeviceEntity(`["matrix.org"]`)
	if !ok || entity != "matrix.org" || !remote {
		t.Errorf("got (%q, %v, %v), want a remote server", entity, remote, ok)
	}

	entity, remote, ok = ParseToDeviceEntity(`["@alice:a.example"]`)
	if !ok || entity != "@alice:a.example" || remote {
		t.Errorf("got (%q, %v, %v), want a local user", entity, remote, ok)
	}

	if _, _, ok := ParseToDeviceEntity(`[""]`); ok {
		t.Error("an empty entity parsed")
	}
	if _, _, ok := ParseToDeviceEntity(`[]`); ok {
		t.Error("an empty row parsed")
	}
}

func TestParseDeviceListRow(t *testing.T) {
	row, ok := ParseDeviceListRow(`["@alice:a.example",false,true]`)
	if !ok {
		t.Fatal("did not parse")
	}
	if row.UserID != "@alice:a.example" || row.IsSignature || !row.HostsCalculated {
		t.Errorf("got %+v", row)
	}

	// Short rows are real: older Synapse emitted two fields.
	row, ok = ParseDeviceListRow(`["@bob:a.example"]`)
	if !ok || row.UserID != "@bob:a.example" || row.HostsCalculated {
		t.Errorf("got %+v, %v", row, ok)
	}
}

// The exact bytes Synapse puts on the wire for a typing notification, from
// KeyedEduRow.to_data() -> {"key": self.key, "edu": self.edu.get_internal_dict()}
// (federation/send_queue.py:444) with get_internal_dict returning edu_type,
// content, origin and destination (federation/units.py:58).
//
// Typing is the ONLY caller of build_and_send_edu in Synapse
// (handlers/typing.py:188), so this row shape is the entire reason the
// federation stream is consumed at all.
func TestParseFederationRowTyping(t *testing.T) {
	const row = `["k", {"key": ["!room:example.com", "@alice:example.com"], ` +
		`"edu": {"edu_type": "m.typing", "content": {"room_id": "!room:example.com", ` +
		`"user_id": "@alice:example.com", "typing": true}, "origin": "example.com", ` +
		`"destination": "remote.example"}}]`

	r, ok := ParseFederationRow(row)
	if !ok {
		t.Fatal("a real typing row did not parse")
	}
	if r.Kind != "k" {
		t.Errorf("Kind = %q, want k", r.Kind)
	}
	if r.EDUType != "m.typing" {
		t.Errorf("EDUType = %q", r.EDUType)
	}
	if r.Destination != "remote.example" {
		t.Errorf("Destination = %q", r.Destination)
	}
	// The key is opaque and positional; what matters is that two different
	// (room, user) pairs produce different keys, since the queue clobbers on it.
	if r.Key == "" {
		t.Error("Key is empty; every typing update would clobber every other")
	}
	if !gjson.GetBytes(r.Content, "typing").Bool() {
		t.Errorf("Content did not survive: %s", r.Content)
	}
}

// A typing row is the whole set for a room, and an EMPTY set is the message
// that everybody stopped. Treating an empty list as a parse failure would drop
// every stop and leave remote indicators on until they timed out.
func TestParseTypingRow(t *testing.T) {
	r, ok := ParseTypingRow(`["!room:example.com", ["@alice:example.com", "@bob:example.com"]]`)
	if !ok {
		t.Fatal("a typing row did not parse")
	}
	if r.RoomID != "!room:example.com" || len(r.UserIDs) != 2 {
		t.Errorf("parsed %+v", r)
	}

	empty, ok := ParseTypingRow(`["!room:example.com", []]`)
	if !ok {
		t.Fatal("an empty typing set did not parse")
	}
	if empty.UserIDs == nil {
		t.Error("an empty set must stay non-nil: it means everybody stopped")
	}
	if len(empty.UserIDs) != 0 {
		t.Errorf("UserIDs = %v", empty.UserIDs)
	}

	if _, ok := ParseTypingRow(`{"room_id": "!r"}`); ok {
		t.Error("an object parsed as a positional row")
	}
	if _, ok := ParseTypingRow(`["", []]`); ok {
		t.Error("a row with no room id was accepted")
	}
}
