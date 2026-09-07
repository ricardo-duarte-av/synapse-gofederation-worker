package txn

import (
	"encoding/json"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Canonical JSON's integer range, from Synapse's CANONICALJSON_MIN_INT and
// CANONICALJSON_MAX_INT. It is 2^53-1 because that is the largest integer a
// JSON parser using IEEE 754 doubles can represent exactly.
const (
	CanonicalJSONMaxInt = 9007199254740991
	CanonicalJSONMinInt = -9007199254740991
)

// SerialisePDU turns a stored event into the form Synapse puts on the wire, and
// reports whether it should be sent at all.
//
// This is Synapse's serialize_and_filter_pdus (federation/units.py:130), which
// is get_pdu_json followed by filter_pdus_for_valid_depth. Two things happen,
// and only two:
//
//   - unsigned.redacted_because is removed. Synapse adds it when LOADING a
//     redacted event and strips it again on the way out, so a stored event never
//     has it; we strip it anyway, because relying on the round trip cancelling
//     out is relying on a coincidence.
//   - a PDU whose depth is outside canonical JSON's integer range is DROPPED
//     entirely, not merely trimmed. Such an event cannot be encoded in a way a
//     remote can parse, so sending it would produce a transaction the far side
//     rejects wholesale -- taking every other PDU in it down too.
//
// Note what does NOT happen: the age_ts to age rewrite in
// transaction_manager.py's json_data_cb looks for age_ts at the PDU's top level,
// while Synapse stores it inside unsigned. The condition is never true. Its own
// FIXME says as much, and this deployment confirms it -- of 64,297 recent
// events, 3,468 carry unsigned.age_ts and not one carries a top-level age_ts.
// Implementing the rewrite would make our bytes differ from Synapse's for 5% of
// events.
func SerialisePDU(stored []byte) (json.RawMessage, bool) {
	if !gjson.ValidBytes(stored) {
		return nil, false
	}

	if depth := gjson.GetBytes(stored, "depth"); depth.Exists() {
		d := depth.Int()
		if d > CanonicalJSONMaxInt || d < CanonicalJSONMinInt {
			return nil, false
		}
	}

	if gjson.GetBytes(stored, "unsigned.redacted_because").Exists() {
		out, err := sjson.DeleteBytes(stored, "unsigned.redacted_because")
		if err != nil {
			return nil, false
		}
		return out, true
	}
	return stored, true
}
