package sink

import (
	"context"
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

	ids := txn.NewIDGenerator()
	req, err := signer.Build(ids.Next(), destination, txn.Transaction{
		OriginServerTS: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PUT %s to %s as %s, signed with %s",
		req.Path, destination, scfg.ServerName, signer.KeyID())
	t.Logf("body: %s", req.Body)

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
