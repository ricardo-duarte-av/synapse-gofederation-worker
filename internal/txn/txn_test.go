package txn

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// A deterministic throwaway key. NOT the server's real one -- the expected
// signatures below were generated with this same seed.
var testKeyLine = "ed25519 testkey " +
	base64.RawStdEncoding.EncodeToString([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	})

func testSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := NewSigner("a.example", testKeyLine)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSignatureMatchesSynapse is the test this package exists for.
//
// The expected bodies and signatures were produced by Synapse's OWN signing
// stack -- signedjson and canonicaljson, inside a running federation sender
// container -- over the same auth object:
//
//	docker exec av-federation-sender-worker-1 python3 -c '
//	import signedjson.key, signedjson.sign, json, base64
//	from canonicaljson import encode_canonical_json
//	key = signedjson.key.read_signing_keys([
//	    "ed25519 testkey " + base64.b64encode(bytes(range(32))).decode().rstrip("=")])[0]
//	body = encode_canonical_json(content)
//	auth = {"method":"PUT","uri":"/_matrix/federation/v1/send/12345","origin":"a.example",
//	        "destination":"b.example","content":json.loads(body.decode())}
//	print(signedjson.sign.sign_json(auth,"a.example",key)["signatures"]["a.example"]["ed25519:testkey"])'
//
// Regenerate them the same way if this ever needs extending. Asserting against
// our own output would only prove the code is self-consistent, which is exactly
// what a canonical-JSON bug also is.
func TestSignatureMatchesSynapse(t *testing.T) {
	s := testSigner(t)

	cases := []struct {
		name string
		txn  Transaction
		body string
		sig  string
	}{
		{
			name: "empty transaction",
			txn:  Transaction{OriginServerTS: 1700000000000},
			body: `{"origin":"a.example","origin_server_ts":1700000000000,"pdus":[]}`,
			sig:  "86+YVwpL3G5x21fOEZedNKYIuNAqTqEjWM15diC4KRSJZyoxvB/3yWJSm2ikH8a2SLP+6Seb7ozSwUtGcjnMDQ",
		},
		{
			// The characters Go's encoding/json HTML-escapes by default.
			// Canonical JSON does not escape them, and "&" is common in real
			// message bodies, so getting this wrong would break a large
			// fraction of transactions with no visible cause.
			name: "html-escapable characters in a pdu",
			txn: Transaction{
				OriginServerTS: 1,
				PDUs: []json.RawMessage{
					json.RawMessage(`{"type":"m.room.message","content":{"body":"a & b < c > d"}}`),
				},
			},
			body: `{"origin":"a.example","origin_server_ts":1,"pdus":[{"content":{"body":"a & b < c > d"},"type":"m.room.message"}]}`,
			sig:  "gL3sNJu95h/LVyaRBVRzxKTg7L1f4pDMnNs5thNTA4/aZCGc39v4alaoVzEWNE74071zcC+klxC2vvXy9dKPDA",
		},
		{
			// Non-ASCII stays as UTF-8 rather than \u escapes, and "edus"
			// sorts before "origin".
			name: "edus and unicode",
			txn: Transaction{
				OriginServerTS: 2,
				EDUs: []EDU{{
					Type:    EDUTypeDirectToDevice,
					Content: json.RawMessage(`{"k":"é中"}`),
				}},
			},
			body: `{"edus":[{"content":{"k":"é中"},"edu_type":"m.direct_to_device"}],"origin":"a.example","origin_server_ts":2,"pdus":[]}`,
			sig:  "Omx2fI57e5t5nQdC0MaQzoQ6R24sK9OtwRzE/Rs/qvH9bHFjJtJGEvIw8lahyXuSpuqLLCECs4QGVRSSOxenBQ",
		},
	}

	for _, tc := range cases {
		req, err := s.Build("12345", "b.example", tc.txn)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(req.Body) != tc.body {
			t.Errorf("%s: body\n got %s\nwant %s", tc.name, req.Body, tc.body)
		}
		wantHeader := `X-Matrix origin="a.example",key="ed25519:testkey",sig="` + tc.sig +
			`",destination="b.example"`
		if req.AuthHeader != wantHeader {
			t.Errorf("%s: auth header\n got %s\nwant %s", tc.name, req.AuthHeader, wantHeader)
		}
	}
}

// The txn id travels in the URL only. Synapse's Transaction object carries
// transaction_id and destination but get_dict drops both (units.py:104), so
// including either would change the signed bytes and every remote would reject
// the signature.
func TestBodyOmitsTransactionIDAndDestination(t *testing.T) {
	req, err := testSigner(t).Build("98765", "b.example", Transaction{OriginServerTS: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"transaction_id", "98765", "destination", "b.example"} {
		if strings.Contains(string(req.Body), forbidden) {
			t.Errorf("body contains %q: %s", forbidden, req.Body)
		}
	}
	if req.Path != "/_matrix/federation/v1/send/98765" {
		t.Errorf("Path = %q", req.Path)
	}
}

// An empty edus list is omitted entirely, and pdus is always present even when
// empty -- both as Synapse does, and both change the signed bytes.
func TestEmptyEDUsAreOmittedButPDUsAreNot(t *testing.T) {
	req, err := testSigner(t).Build("1", "b.example", Transaction{OriginServerTS: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(req.Body), "edus") {
		t.Errorf("empty edus was included: %s", req.Body)
	}
	if !strings.Contains(string(req.Body), `"pdus":[]`) {
		t.Errorf("empty pdus was omitted: %s", req.Body)
	}
}

// The txn id goes in a URL path, so a value needing escaping must be escaped --
// and the signature covers the escaped form, since Synapse signs the path.
func TestTransactionIDIsPathEscaped(t *testing.T) {
	req, err := testSigner(t).Build("a/b c", "b.example", Transaction{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(req.Path, " ") {
		t.Errorf("Path was not escaped: %q", req.Path)
	}
}

// Synapse seeds the counter from the clock and shares it across destinations,
// so two destinations in the same batch get different ids and a restart cannot
// reuse one (transaction_manager.py:75).
func TestIDGenerator(t *testing.T) {
	g := NewIDGenerator()
	seen := map[string]bool{}
	var prev int64 = -1
	for i := 0; i < 1000; i++ {
		id := g.Next()
		if seen[id] {
			t.Fatalf("duplicate transaction id %q", id)
		}
		seen[id] = true
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatalf("id %q is not an integer: %v", id, err)
		}
		if n <= prev {
			t.Fatalf("ids are not increasing: %d after %d", n, prev)
		}
		prev = n
	}
	if prev < 1_700_000_000_000 {
		t.Errorf("the counter was not seeded from the clock: %d", prev)
	}
}

func TestNewSignerRejectsBadKeys(t *testing.T) {
	for name, line := range map[string]string{
		"empty":       "",
		"two fields":  "ed25519 testkey",
		"not ed25519": "rsa testkey AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
		"bad base64":  "ed25519 testkey !!!!",
	} {
		if _, err := NewSigner("a.example", line); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestKeyID(t *testing.T) {
	if got := testSigner(t).KeyID(); got != "ed25519:testkey" {
		t.Errorf("KeyID = %q", got)
	}
}
