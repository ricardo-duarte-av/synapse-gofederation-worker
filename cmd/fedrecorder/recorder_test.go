package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/capture"
)

// upstream stands in for the test homeserver and remembers exactly what
// reached it.
type upstream struct {
	mu     sync.Mutex
	bodies []string
	paths  []string
	auths  []string
	status int
}

func (u *upstream) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.bodies = append(u.bodies, string(b))
		u.paths = append(u.paths, r.URL.RequestURI())
		u.auths = append(u.auths, r.Header.Get("Authorization"))
		u.mu.Unlock()

		code := u.status
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"pdus":{}}`))
	})
}

func newTestRecorder(t *testing.T, originOnly string) (*recorder, *upstream, string, func()) {
	t.Helper()
	up := &upstream{}
	backend := httptest.NewServer(up.handler())

	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "syn.jsonl")
	w, err := capture.Open(capture.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	log := zerolog.New(io.Discard)
	rec := &recorder{writer: w, log: log, originOnly: originOnly, proxy: newProxy(target, log)}

	return rec, up, path, func() {
		w.Close()
		backend.Close()
	}
}

const txnBody = `{"origin":"a.example","origin_server_ts":1,` +
	`"pdus":[{"event_id":"$a","type":"m.room.message"}],` +
	`"edus":[{"edu_type":"m.receipt","content":{}}]}`

func put(t *testing.T, rec *recorder, path, body, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	req.Header.Set("X-Forwarded-Host", "testing.a.example")
	rw := httptest.NewRecorder()
	rec.ServeHTTP(rw, req)
	return rw
}

// The property everything else depends on: the proxy must forward the EXACT
// bytes it received. Re-encoding would invalidate the signature the sender
// computed over them, and the homeserver would reject a transaction that was in
// fact correct -- turning the instrumentation into the bug.
func TestForwardsBodyUnaltered(t *testing.T) {
	rec, up, _, done := newTestRecorder(t, "")
	defer done()

	auth := `X-Matrix origin="a.example",key="ed25519:k",sig="s",destination="testing.a.example"`
	rw := put(t, rec, "/_matrix/federation/v1/send/12345", txnBody, auth)

	if rw.Code != http.StatusOK {
		t.Fatalf("status = %d", rw.Code)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 1 {
		t.Fatalf("upstream saw %d requests", len(up.bodies))
	}
	if up.bodies[0] != txnBody {
		t.Errorf("body was altered in transit:\n sent: %s\n got:  %s", txnBody, up.bodies[0])
	}
	// The signature covers the path and the auth header carries it, so both
	// must arrive untouched too.
	if up.paths[0] != "/_matrix/federation/v1/send/12345" {
		t.Errorf("path = %q", up.paths[0])
	}
	if up.auths[0] != auth {
		t.Errorf("Authorization = %q", up.auths[0])
	}
}

func TestRecordsTransaction(t *testing.T) {
	rec, _, path, done := newTestRecorder(t, "")
	defer done()

	auth := `X-Matrix origin="a.example",key="ed25519:k",sig="s",destination="testing.a.example"`
	put(t, rec, "/_matrix/federation/v1/send/12345", txnBody, auth)
	rec.writer.Close()

	records, skipped, err := capture.ReadFile(path)
	if err != nil || skipped != 0 {
		t.Fatalf("read: %v, skipped %d", err, skipped)
	}
	if len(records) != 1 {
		t.Fatalf("recorded %d transactions", len(records))
	}
	r := records[0]
	if r.Source != capture.SourceSynapse {
		t.Errorf("Source = %q", r.Source)
	}
	if r.TxnID != "12345" {
		t.Errorf("TxnID = %q", r.TxnID)
	}
	if r.Origin != "a.example" {
		t.Errorf("Origin = %q", r.Origin)
	}
	// The destination is the name the sender addressed, taken from the
	// forwarding header the reverse proxy sets.
	if r.Destination != "testing.a.example" {
		t.Errorf("Destination = %q", r.Destination)
	}
	if r.Auth != auth {
		t.Errorf("Auth = %q", r.Auth)
	}
	if r.Status != http.StatusOK {
		t.Errorf("Status = %d", r.Status)
	}
	if string(r.Body) != txnBody {
		t.Errorf("recorded body differs from the wire:\n %s", r.Body)
	}
}

// A rejected transaction is a different fact from an accepted one, and just as
// much evidence of what was sent.
func TestRecordsRejectedTransactions(t *testing.T) {
	rec, up, path, done := newTestRecorder(t, "")
	defer done()
	up.status = http.StatusForbidden

	put(t, rec, "/_matrix/federation/v1/send/1", txnBody, "")
	rec.writer.Close()

	records, _, _ := capture.ReadFile(path)
	if len(records) != 1 {
		t.Fatalf("recorded %d", len(records))
	}
	if records[0].Status != http.StatusForbidden {
		t.Errorf("Status = %d, want the rejection to be recorded", records[0].Status)
	}
}

// The origin filter reads the BODY, not the header: the body is what was
// signed, and taking it from an unsigned header would let a forged header
// decide what we record.
func TestOriginFilterUsesTheSignedBody(t *testing.T) {
	rec, _, path, done := newTestRecorder(t, "a.example")
	defer done()

	// Right origin in the body, wrong one in the header: recorded.
	put(t, rec, "/_matrix/federation/v1/send/1", txnBody,
		`X-Matrix origin="evil.example",key="ed25519:k",sig="s"`)
	// Wrong origin in the body, right one in the header: not recorded.
	other := strings.Replace(txnBody, `"origin":"a.example"`, `"origin":"other.example"`, 1)
	put(t, rec, "/_matrix/federation/v1/send/2", other,
		`X-Matrix origin="a.example",key="ed25519:k",sig="s"`)
	rec.writer.Close()

	records, _, _ := capture.ReadFile(path)
	if len(records) != 1 {
		t.Fatalf("recorded %d transactions, want only the one from a.example", len(records))
	}
	if records[0].TxnID != "1" {
		t.Errorf("recorded the wrong transaction: %+v", records[0])
	}
}

// Everything that is not a transaction -- key lookups, joins, state -- must be
// forwarded untouched and unrecorded.
func TestNonSendRequestsAreForwardedNotRecorded(t *testing.T) {
	rec, up, path, done := newTestRecorder(t, "")
	defer done()

	req := httptest.NewRequest(http.MethodGet, "/_matrix/key/v2/server", nil)
	rw := httptest.NewRecorder()
	rec.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Errorf("key request status = %d", rw.Code)
	}
	up.mu.Lock()
	forwarded := len(up.bodies)
	up.mu.Unlock()
	if forwarded != 1 {
		t.Errorf("key request was not forwarded")
	}
	rec.writer.Close()
	records, _, _ := capture.ReadFile(path)
	if len(records) != 0 {
		t.Errorf("a non-send request was recorded: %+v", records)
	}
}

func TestHostOf(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "example.com:8448"
	if got := hostOf(req); got != "example.com" {
		t.Errorf("hostOf = %q, want the port stripped", got)
	}
	req.Header.Set("X-Forwarded-Host", "testing.example.com")
	if got := hostOf(req); got != "testing.example.com" {
		t.Errorf("hostOf = %q, want the forwarded host to win", got)
	}
}
