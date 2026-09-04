package replication

import "github.com/tidwall/gjson"

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
