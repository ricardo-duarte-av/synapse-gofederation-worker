package sink

import "github.com/tidwall/gjson"

// countUnits counts the PDUs and EDUs in a serialised transaction.
//
// Counted from the encoded body rather than from the Transaction struct so the
// figure describes what would actually go on the wire. The two can differ --
// canonical JSON is where the last transformation happens -- and if they ever
// do, the number that matters for a comparison against the real sender is this
// one.
func countUnits(body []byte) (pdus, edus int) {
	r := gjson.ParseBytes(body)
	return len(r.Get("pdus").Array()), len(r.Get("edus").Array())
}

// countEDUTypes counts the EDUs in a transaction by edu_type.
//
// Synapse breaks its own count down this way
// (synapse_federation_client_sent_edus_by_type_total) and the difference
// matters more than it sounds: on this deployment presence was 15.257 of the
// 15.322 EDUs per second the Python senders sent -- 99.6% of everything. An
// unlabelled total cannot tell "we route less presence" from "we route no
// typing", which are a tuning question and a broken feature respectively.
func countEDUTypes(body []byte) map[string]int {
	out := map[string]int{}
	for _, e := range gjson.GetBytes(body, "edus").Array() {
		t := e.Get("edu_type").String()
		if t == "" {
			t = "unknown"
		}
		out[t]++
	}
	return out
}
