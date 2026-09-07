package replication

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

// Replication rows are positional JSON arrays, one shape per stream
// (replication/tcp/streams/*.py). Getting a shape wrong is not a parse error --
// it is a row silently understood as something else -- so each parser states
// the shape it expects in a comment taken from the stream definition.

// EventRow is one row of the events stream.
//
// The stream merges three sources and each has its own leading type id
// (streams/events.py:107,122,132):
//
//	["ev",        [event_id, room_id, type, state_key, redacts, relates_to,
//	               membership, rejected, outlier]]
//	["state",     [room_id, type, state_key, event_id]]
//	["state-all", [room_id]]
//
// Only "ev" names an event to send. The others are current-state deltas, and a
// sender ignores them: it re-reads the events table for content and resolves
// destinations itself.
type EventRow struct {
	// Kind is "ev", "state" or "state-all".
	Kind     string
	EventID  string
	RoomID   string
	Type     string
	StateKey string
	// Outlier and Rejected are set for "ev" rows. Both mean the event is not
	// sent, and knowing it here saves a database round trip.
	Outlier  bool
	Rejected bool
}

// ParseEventRow reads one row of the events stream.
func ParseEventRow(row string) (EventRow, bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return EventRow{}, false
	}
	a := r.Array()
	if len(a) < 2 {
		return EventRow{}, false
	}
	kind := a[0].String()
	inner := a[1].Array()
	at := func(i int) gjson.Result {
		if i < len(inner) {
			return inner[i]
		}
		return gjson.Result{}
	}

	switch kind {
	case "ev":
		if len(inner) < 2 {
			return EventRow{}, false
		}
		return EventRow{
			Kind:     kind,
			EventID:  at(0).String(),
			RoomID:   at(1).String(),
			Type:     at(2).String(),
			StateKey: at(3).String(),
			// rejected is index 7 and outlier index 8, per
			// EventsStreamEventRow's field order.
			Rejected: at(7).Bool(),
			Outlier:  at(8).Bool(),
		}, true
	case "state":
		if len(inner) < 1 {
			return EventRow{}, false
		}
		return EventRow{
			Kind:     kind,
			RoomID:   at(0).String(),
			Type:     at(1).String(),
			StateKey: at(2).String(),
			EventID:  at(3).String(),
		}, true
	case "state-all":
		if len(inner) < 1 {
			return EventRow{}, false
		}
		return EventRow{Kind: kind, RoomID: at(0).String()}, true
	}
	return EventRow{}, false
}

// ParseToDeviceEntity reads one row of the to_device stream: [entity].
//
// The entity is a user id for a local delivery or a remote server name for a
// federated one. A sender wants only the latter, which is why Synapse filters
// on the leading "@" (replication/tcp/client.py:472) rather than looking
// anything up.
func ParseToDeviceEntity(row string) (entity string, isRemoteServer bool, ok bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return "", false, false
	}
	a := r.Array()
	if len(a) < 1 {
		return "", false, false
	}
	entity = a[0].String()
	if entity == "" {
		return "", false, false
	}
	return entity, entity[0] != '@', true
}

// DeviceListRow is one row of the device_lists stream:
//
//	[user_id, is_signature, hosts_calculated]
//
// The destinations are deliberately not in the row. HostsCalculated true means
// Synapse has already written device_lists_outbound_pokes for this stream id,
// and that table is the only place the destination list exists
// (devices.py:1933).
type DeviceListRow struct {
	UserID          string
	IsSignature     bool
	HostsCalculated bool
}

// ParseDeviceListRow reads one row of the device_lists stream.
func ParseDeviceListRow(row string) (DeviceListRow, bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return DeviceListRow{}, false
	}
	a := r.Array()
	if len(a) < 1 {
		return DeviceListRow{}, false
	}
	out := DeviceListRow{UserID: a[0].String()}
	if len(a) > 1 {
		out.IsSignature = a[1].Bool()
	}
	if len(a) > 2 {
		out.HostsCalculated = a[2].Bool()
	}
	return out, true
}

// ReceiptRow is one row of the receipts stream:
//
//	[room_id, receipt_type, user_id, event_id, thread_id, data]
type ReceiptRow struct {
	RoomID      string
	ReceiptType string
	UserID      string
	EventID     string
	// ThreadID is empty for an unthreaded receipt. It is not decoration: two
	// receipts from one user in one room for different threads are both real,
	// and merging them into one slot would lose the older.
	ThreadID string
	// Data is the receipt body, forwarded as-is.
	Data json.RawMessage
}

// ParseReceiptRow reads one row of the receipts stream.
func ParseReceiptRow(row string) (ReceiptRow, bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return ReceiptRow{}, false
	}
	a := r.Array()
	if len(a) < 3 {
		return ReceiptRow{}, false
	}
	out := ReceiptRow{
		RoomID:      a[0].String(),
		ReceiptType: a[1].String(),
		UserID:      a[2].String(),
	}
	if len(a) > 3 {
		out.EventID = a[3].String()
	}
	if len(a) > 4 && a[4].Type != gjson.Null {
		out.ThreadID = a[4].String()
	}
	if len(a) > 5 {
		out.Data = json.RawMessage(a[5].Raw)
	}
	if out.RoomID == "" || out.UserID == "" {
		return ReceiptRow{}, false
	}
	return out, true
}

// PresenceFederationRow is one row of the presence_federation stream:
//
//	[destination, user_id]
//
// The state itself is not in the row -- only who it is about and where it goes
// -- so the sender reads the current presence from the database. That is
// deliberate on Synapse's part: presence changes far faster than it can be
// delivered, and sending the state as it was when the row was written would put
// stale presence on the wire.
type PresenceFederationRow struct {
	Destination string
	UserID      string
}

// ParsePresenceFederationRow reads one row of the presence_federation stream.
func ParsePresenceFederationRow(row string) (PresenceFederationRow, bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return PresenceFederationRow{}, false
	}
	a := r.Array()
	if len(a) < 2 {
		return PresenceFederationRow{}, false
	}
	out := PresenceFederationRow{Destination: a[0].String(), UserID: a[1].String()}
	if out.Destination == "" || out.UserID == "" {
		return PresenceFederationRow{}, false
	}
	return out, true
}

// FederationRow is one row of the federation stream, which carries EDUs the
// main process or another worker wants sent.
//
//	["k", {"key": [...], "edu": {...}}]   a keyed EDU, e.g. typing
//	["e", {...}]                          a plain EDU
//	["pd", {"state": {...}, "dests": [...]}]  presence destinations
//
// Typing arrives this way rather than off the typing stream: the typing handler
// runs on its own worker and hands the EDU to the federation sender through
// here (handlers/typing.py:188).
type FederationRow struct {
	// Kind is "k", "e" or "pd".
	Kind string
	// Key identifies a keyed EDU, so a later update replaces an earlier one.
	Key string
	// EDUType and Content are set for "k" and "e".
	EDUType string
	Content json.RawMessage
	// Destination is set for "k" and "e" when the EDU names one.
	Destination string
	// PresenceState and PresenceDestinations are set for "pd".
	PresenceState        json.RawMessage
	PresenceDestinations []string
}

// ParseFederationRow reads one row of the federation stream.
func ParseFederationRow(row string) (FederationRow, bool) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return FederationRow{}, false
	}
	a := r.Array()
	if len(a) < 2 {
		return FederationRow{}, false
	}
	kind := a[0].String()
	body := a[1]

	switch kind {
	case "k":
		edu := body.Get("edu")
		if !edu.Exists() {
			return FederationRow{}, false
		}
		return FederationRow{
			Kind: kind,
			// The key is a positional array; its exact shape is the sender's
			// business, so it is used verbatim as an opaque identity rather
			// than interpreted.
			Key:         body.Get("key").Raw,
			EDUType:     edu.Get("edu_type").String(),
			Content:     json.RawMessage(edu.Get("content").Raw),
			Destination: edu.Get("destination").String(),
		}, true
	case "e":
		return FederationRow{
			Kind:        kind,
			EDUType:     body.Get("edu_type").String(),
			Content:     json.RawMessage(body.Get("content").Raw),
			Destination: body.Get("destination").String(),
		}, true
	case "pd":
		var dests []string
		for _, d := range body.Get("dests").Array() {
			dests = append(dests, d.String())
		}
		return FederationRow{
			Kind:                 kind,
			PresenceState:        json.RawMessage(body.Get("state").Raw),
			PresenceDestinations: dests,
		}, true
	}
	return FederationRow{}, false
}
