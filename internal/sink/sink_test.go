package sink

import (
	"context"
	"go/parser"
	"go/token"
	"io"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

func TestDryRunCountsAndSucceeds(t *testing.T) {
	d := NewDryRun(zerolog.New(io.Discard))
	body := []byte(`{"origin":"a.example","origin_server_ts":1,` +
		`"edus":[{"edu_type":"m.receipt","content":{}}],` +
		`"pdus":[{"type":"m.room.message"},{"type":"m.room.message"}]}`)

	res, err := d.Send(context.Background(), &txn.Request{
		TransactionID: "1", Destination: "b.example", Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reporting success is deliberate: the queue advances its cursors on a
	// delivered transaction, and a dry-run that failed would make the shadow
	// retry forever and diverge for a reason unrelated to its decisions.
	if !res.Delivered {
		t.Error("dry-run reported a failure; the queue would retry forever")
	}

	s := d.Stats()
	if s.Transactions != 1 || s.PDUs != 2 || s.EDUs != 1 || s.Bytes != int64(len(body)) {
		t.Errorf("Stats = %+v", s)
	}

	if _, err := d.Send(context.Background(), &txn.Request{Body: []byte(`{"pdus":[]}`)}); err != nil {
		t.Fatal(err)
	}
	if s := d.Stats(); s.Transactions != 2 || s.PDUs != 2 {
		t.Errorf("Stats after a second send = %+v", s)
	}
}

func TestCountUnitsHandlesJunk(t *testing.T) {
	for _, body := range []string{``, `{}`, `not json`, `{"pdus":null}`} {
		if p, e := countUnits([]byte(body)); p != 0 || e != 0 {
			t.Errorf("%q counted %d pdus and %d edus", body, p, e)
		}
	}
}

// The property that makes shadow mode safe: the dry-run path has no way to
// reach the network.
//
// Scoped to sink.go rather than the package, because http.go now lives here
// too and necessarily imports net/http. Go imports are per file, so this still
// says exactly what it means -- the file containing DryRun pulls in nothing
// that can open a socket. What keeps the HTTP sink away from a destination it
// should not touch is Router, which is tested separately.
func TestDryRunFileCannotReachTheNetwork(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sink.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{
		`"net/http"`: true, `"net"`: true, `"crypto/tls"`: true, `"net/url"`: true,
	}
	for _, imp := range file.Imports {
		if forbidden[imp.Path.Value] {
			t.Errorf("sink.go imports %s; the dry-run sink must have no way to "+
				"reach the network (docs/shadow-safety.md)", imp.Path.Value)
		}
	}
}
