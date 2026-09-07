package txn

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"

	"maunium.net/go/mautrix/crypto/canonicaljson"
)

// TestLivePDUBytesMatchSynapse compares our serialised PDUs against Synapse's,
// byte for byte, over real events.
//
// This is the layer of the shadow comparison that CAN be exact. Transaction
// framing cannot be -- two correct senders batch differently depending on what
// happened to be queued when their loop ran -- but the bytes of an individual
// PDU are a pure function of the stored event, with no timing in them at all.
//
// The fixture is produced by Synapse's own canonicaljson, applying exactly what
// serialize_and_filter_pdus does. Generate it with:
//
//	psql -h /var/sockets -U gofed_ro -d synapse-db -At -c "
//	  SELECT ej.json FROM event_json ej JOIN events e USING (event_id)
//	  WHERE e.stream_ordering > 14000000 AND e.outlier = false
//	  ORDER BY e.stream_ordering DESC LIMIT 200;" > raw-events.jsonl
//	docker cp raw-events.jsonl av-federation-sender-worker-1:/tmp/
//	docker exec av-federation-sender-worker-1 python3 -c '
//	import json
//	from canonicaljson import encode_canonical_json
//	out = open("/tmp/syn-pdus.jsonl", "wb")
//	for line in open("/tmp/raw-events.jsonl"):
//	    line = line.strip()
//	    if not line: continue
//	    d = json.loads(line)
//	    u = d.get("unsigned")
//	    if isinstance(u, dict): u.pop("redacted_because", None)
//	    depth = d.get("depth")
//	    if depth is not None and not (-9007199254740991 <= depth <= 9007199254740991):
//	        continue
//	    out.write(encode_canonical_json(d) + b"\n")
//	out.close()'
//	docker cp av-federation-sender-worker-1:/tmp/syn-pdus.jsonl .
//
// Then:
//
//	PDU_RAW=raw-events.jsonl PDU_EXPECTED=syn-pdus.jsonl \
//	  go test ./internal/txn/ -run TestLivePDUBytes -v
func TestLivePDUBytesMatchSynapse(t *testing.T) {
	rawPath, wantPath := os.Getenv("PDU_RAW"), os.Getenv("PDU_EXPECTED")
	if rawPath == "" || wantPath == "" {
		t.Skip("set PDU_RAW and PDU_EXPECTED to compare against Synapse's own serialisation")
	}

	raw := readLines(t, rawPath)
	want := readLines(t, wantPath)

	var got [][]byte
	for _, line := range raw {
		pdu, ok := SerialisePDU([]byte(line))
		if !ok {
			// Dropped by the depth filter; Synapse's fixture drops it too, so
			// it simply does not appear in either list.
			continue
		}
		// The bytes that reach the wire are the transaction's canonical JSON,
		// so the PDU is compared as it appears there rather than as stored.
		b, err := canonicaljson.Marshal(json.RawMessage(pdu))
		if err != nil {
			t.Fatalf("canonicalising %s: %v", pdu, err)
		}
		got = append(got, b)
	}

	if len(got) != len(want) {
		t.Fatalf("serialised %d PDUs, Synapse serialised %d; the depth filter disagrees",
			len(got), len(want))
	}

	mismatched := 0
	for i := range got {
		if string(got[i]) != want[i] {
			mismatched++
			if mismatched <= 3 {
				t.Errorf("PDU %d differs:\n ours: %s\n syn:  %s", i, got[i], want[i])
			}
		}
	}
	if mismatched > 0 {
		t.Fatalf("%d of %d PDUs differ from Synapse's serialisation", mismatched, len(got))
	}
	t.Logf("%d PDUs are byte-identical to Synapse's serialisation", len(got))
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
