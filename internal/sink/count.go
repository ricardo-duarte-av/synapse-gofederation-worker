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
