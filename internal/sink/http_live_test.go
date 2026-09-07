package sink

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// TestLiveSendEmptyTransaction sends a real, signed, EMPTY transaction to a real
// homeserver.
//
// It is the only test that proves the outbound path end to end: server
// discovery through .well-known, TLS with the right name, the X-Matrix header,
// and -- the part no fixture can establish -- that a REAL Synapse accepts our
// signature. A wrong signature comes back as 401 here, where a comparison
// against signedjson would have said everything was fine.
//
// An empty transaction is deliberate. It carries no PDUs and no EDUs, so it
// changes nothing on the receiving server and cannot be sent to the wrong room
// or the wrong people; the only thing under test is whether the envelope is
// accepted. Synapse answers a well-formed empty transaction with
// {"pdus":{}} and a rejected one with 401.
//
//	FEDERATION_TEST_DESTINATION=testing.aguiarvieira.pt \
//	HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml \
//	SIGNING_KEY=/opt/matrix/synapse/synapse/aguiarvieira.pt.signing.key \
//	go test ./internal/sink/ -run TestLiveSend -v
//
// Only ever point it at a server you control.
func TestLiveSendEmptyTransaction(t *testing.T) {
	destination := os.Getenv("FEDERATION_TEST_DESTINATION")
	configPath := os.Getenv("HOMESERVER_YAML")
	if destination == "" || configPath == "" {
		t.Skip("set FEDERATION_TEST_DESTINATION and HOMESERVER_YAML to send a real transaction")
	}

	scfg, err := synapsecfg.LoadWithOptions(configPath,
		synapsecfg.Options{SigningKeyPath: os.Getenv("SIGNING_KEY")})
	if err != nil {
		t.Fatal(err)
	}
	key, err := scfg.SigningKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := txn.NewSigner(scfg.ServerName,
		key.Algorithm+" "+key.Version+" "+encodeSeedForTest(key.Seed))
	if err != nil {
		t.Fatal(err)
	}

	ids := txn.NewIDGenerator(txn.DefaultIDPrefix)
	req, err := signer.Build(ids.Next(), destination, txn.Transaction{
		OriginServerTS: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PUT %s to %s as %s, signed with %s",
		req.Path, destination, scfg.ServerName, signer.KeyID())
	t.Logf("body: %s", req.Body)

	// FEDERATION_TEST_URL sends to a fixed URL instead of resolving the server
	// name, so the request can be pushed through a proxy under test --
	// cmd/fedrecorder -- rather than straight at the homeserver. The signature
	// is unaffected: it covers the path, the origin and the destination, none
	// of which change with the route taken.
	if direct := os.Getenv("FEDERATION_TEST_URL"); direct != "" {
		sendDirect(t, direct, req)
		return
	}

	h := NewHTTP(HTTPConfig{
		Log:      zerolog.New(zerolog.NewTestWriter(t)),
		Timeout:  scfg.ClientTimeout,
		Retries:  1,
		MaxDelay: 5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := h.Send(ctx, req)
	if err != nil {
		t.Fatalf("the remote rejected our transaction: %v", err)
	}
	if !res.Delivered {
		t.Fatalf("not delivered: %+v", res)
	}
	t.Logf("ACCEPTED by %s: status %d, %d per-PDU errors",
		destination, res.StatusCode, len(res.PDUErrors))
}

func encodeSeedForTest(seed []byte) string {
	return base64RawStdForTest.EncodeToString(seed)
}

// sendDirect PUTs a signed transaction to an explicit URL, bypassing Matrix
// server discovery.
//
// Test-only, and deliberately not a feature of the HTTP sink: a way to send a
// signed transaction somewhere other than the server it is addressed to is
// exactly the kind of thing that should not exist in production code.
func sendDirect(t *testing.T, base string, req *txn.Request) {
	t.Helper()
	httpReq, err := http.NewRequest(http.MethodPut, base+req.Path, bytes.NewReader(req.Body))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Authorization", req.AuthHeader)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", "synapse-gofederation-worker/test")
	// What a reverse proxy in front of the recorder would set, and what the
	// recorder reads to learn which name the sender addressed.
	httpReq.Header.Set("X-Forwarded-Host", req.Destination)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(httpReq)
	if err != nil {
		t.Fatalf("sending to %s: %v", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s returned %s: %s", base, resp.Status, body)
	}
	t.Logf("ACCEPTED via %s: status %d, body %s", base, resp.StatusCode, body)
}
