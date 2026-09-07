package sink

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// spy stands in for the live sink and records what it was asked to send. In
// these tests, anything reaching it is a destination that would have received
// real federation traffic.
type spy struct {
	mu   sync.Mutex
	sent []string
}

func (s *spy) Mode() string { return "spy" }

func (s *spy) Send(_ context.Context, req *txn.Request) (Result, error) {
	s.mu.Lock()
	s.sent = append(s.sent, req.Destination)
	s.mu.Unlock()
	return Result{Delivered: true}, nil
}

func (s *spy) destinations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

func router(t *testing.T, allowed []string, all bool) (*Router, *spy) {
	t.Helper()
	live := &spy{}
	return NewRouter(RouterConfig{
		Allowed: allowed, All: all,
		Live: live, Dry: NewDryRun(zerolog.New(io.Discard)),
		Log: zerolog.New(io.Discard),
	}), live
}

func send(t *testing.T, r *Router, destinations ...string) {
	t.Helper()
	for _, d := range destinations {
		_, err := r.Send(context.Background(), &txn.Request{
			Destination: d, TransactionID: "1", Body: []byte(`{"pdus":[]}`),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// The property this type exists for: only allowlisted destinations reach the
// network, and every other one is inert.
func TestOnlyAllowlistedDestinationsAreSent(t *testing.T) {
	r, live := router(t, []string{"testing.aguiarvieira.pt"}, false)
	send(t, r, "testing.aguiarvieira.pt", "matrix.org", "example.com", "testing.aguiarvieira.pt")

	got := live.destinations()
	if len(got) != 2 {
		t.Fatalf("live sink saw %v, want only the allowlisted destination", got)
	}
	for _, d := range got {
		if d != "testing.aguiarvieira.pt" {
			t.Errorf("real traffic went to %q", d)
		}
	}
}

// The default is deny. An empty allowlist must never be read as "everyone" --
// that is the difference between a misconfiguration and an incident.
func TestEmptyAllowlistSendsNothing(t *testing.T) {
	r, live := router(t, nil, false)
	send(t, r, "matrix.org", "example.com", "testing.aguiarvieira.pt")

	if got := live.destinations(); len(got) != 0 {
		t.Fatalf("an empty allowlist sent to %v", got)
	}
	if !strings.Contains(r.Mode(), "dry-run") {
		t.Errorf("Mode() = %q, should say nothing is being sent", r.Mode())
	}
}

// Sending to everyone is reachable only by asking for it by name.
func TestSendToAllIsExplicit(t *testing.T) {
	r, live := router(t, nil, true)
	send(t, r, "matrix.org", "example.com")

	if got := live.destinations(); len(got) != 2 {
		t.Errorf("send_to_all sent to %v, want both", got)
	}
	if !strings.Contains(r.Mode(), "ALL") {
		t.Errorf("Mode() = %q, should say it is sending to everything", r.Mode())
	}
}

// The startup line is what an operator reads to answer "is this putting traffic
// on the internet?", so it must name the destinations rather than summarise.
func TestModeNamesTheDestinations(t *testing.T) {
	r, _ := router(t, []string{"b.example", "a.example"}, false)
	mode := r.Mode()
	for _, want := range []string{"a.example", "b.example", "SENDING"} {
		if !strings.Contains(mode, want) {
			t.Errorf("Mode() = %q, missing %q", mode, want)
		}
	}
	// Sorted, so the line is stable across restarts and diffable.
	if strings.Index(mode, "a.example") > strings.Index(mode, "b.example") {
		t.Errorf("Mode() = %q, destinations should be sorted", mode)
	}
}

func TestSends(t *testing.T) {
	r, _ := router(t, []string{"yes.example"}, false)
	if !r.Sends("yes.example") {
		t.Error("allowlisted destination reported as not sent")
	}
	if r.Sends("no.example") {
		t.Error("non-allowlisted destination reported as sent")
	}
	// No substring or suffix matching: an allowlist that matched loosely would
	// let a lookalike name through.
	for _, d := range []string{"yes.example.evil.com", "notyes.example", "YES.EXAMPLE"} {
		if r.Sends(d) {
			t.Errorf("%q matched the allowlist entry loosely", d)
		}
	}
}

// A structural guard on the property this design rests on: the HTTP sink holds
// no allowlist of its own, so there is exactly one place where "may we send to
// this server?" is decided.
//
// Read from the AST, not the file text, so the comments in http.go that explain
// the arrangement do not trip it -- the same mistake as in the replication
// package's no-publish guard.
func TestHTTPSinkHasNoAllowlistOfItsOwn(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "http.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	suspicious := []string{"allow", "denylist", "permitted", "sendonly", "sendtoall"}
	ast.Inspect(file, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		lower := strings.ToLower(ident.Name)
		for _, s := range suspicious {
			if strings.Contains(lower, s) {
				t.Errorf("http.go declares or uses %q; the send allowlist belongs "+
					"in Router alone, so there is one place the decision is made",
					ident.Name)
			}
		}
		return true
	})
}
