package replication

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type recorder struct {
	batches   []batch
	positions []position
	serversUp []string
}

type batch struct {
	stream string
	pos    int64
	rows   []Row
}

type position struct {
	stream, instance string
	pos              int64
}

func (r *recorder) OnRows(stream string, pos int64, rows []Row) {
	r.batches = append(r.batches, batch{stream, pos, rows})
}
func (r *recorder) OnPosition(stream, instance string, pos int64) {
	r.positions = append(r.positions, position{stream, instance, pos})
}
func (r *recorder) OnRemoteServerUp(server string) { r.serversUp = append(r.serversUp, server) }

func newTestSubscriber(t *testing.T) (*Subscriber, *recorder) {
	t.Helper()
	rec := &recorder{}
	s := New(Config{InstanceName: "av-gofederation-worker-1"}, zerolog.New(io.Discard), rec)
	return s, rec
}

// The batching protocol: a "batch" token buffers the row, and the next numeric
// token flushes the buffer plus itself as ONE batch, with every row taking that
// position (commands.py:114). Treating each row as its own batch, or dropping
// the buffered ones, both look like working code.
func TestRDATABatching(t *testing.T) {
	s, rec := newTestSubscriber(t)

	s.handleLine(`RDATA events master batch ["ev",["$a","!r:x","m.room.message","",null,null,null,false,false]]`)
	s.handleLine(`RDATA events master batch ["ev",["$b","!r:x","m.room.message","",null,null,null,false,false]]`)
	if len(rec.batches) != 0 {
		t.Fatalf("a buffered row was delivered early: %v", rec.batches)
	}

	s.handleLine(`RDATA events master 500 ["ev",["$c","!r:x","m.room.message","",null,null,null,false,false]]`)
	if len(rec.batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(rec.batches))
	}
	b := rec.batches[0]
	if b.stream != "events" || b.pos != 500 {
		t.Errorf("batch = %s@%d, want events@500", b.stream, b.pos)
	}
	if len(b.rows) != 3 {
		t.Fatalf("batch has %d rows, want the two buffered plus the flushing one", len(b.rows))
	}
	// Every row in the batch carries the flushing token's position, not its own.
	for i, row := range b.rows {
		if row.Position != 500 {
			t.Errorf("row %d has position %d, want 500", i, row.Position)
		}
	}
	if s.Position("events") != 500 {
		t.Errorf("stream position = %d, want 500", s.Position("events"))
	}

	// The buffer must be empty afterwards, or the next batch inherits these.
	s.handleLine(`RDATA events master 501 ["ev",["$d","!r:x","m.room.message","",null,null,null,false,false]]`)
	if got := len(rec.batches[1].rows); got != 1 {
		t.Errorf("the next batch has %d rows, want 1; the buffer was not cleared", got)
	}
}

// Two streams can be mid-batch at once, so the buffer is per stream.
func TestRDATABatchesAreKeptPerStream(t *testing.T) {
	s, rec := newTestSubscriber(t)

	s.handleLine(`RDATA events master batch ["ev",["$a","!r:x","m.room.message","",null,null,null,false,false]]`)
	s.handleLine(`RDATA to_device master batch ["matrix.org"]`)
	s.handleLine(`RDATA to_device master 10 ["example.com"]`)

	if len(rec.batches) != 1 {
		t.Fatalf("got %d batches, want only to_device's", len(rec.batches))
	}
	if rec.batches[0].stream != "to_device" || len(rec.batches[0].rows) != 2 {
		t.Errorf("to_device batch = %+v", rec.batches[0])
	}

	// The events row is still buffered and flushes on its own token.
	s.handleLine(`RDATA events master 99 ["ev",["$b","!r:x","m.room.message","",null,null,null,false,false]]`)
	if len(rec.batches) != 2 || len(rec.batches[1].rows) != 2 {
		t.Errorf("events batch = %+v", rec.batches[1:])
	}
}

// A dropped subscription leaves half a batch that will never be completed.
// Carrying it into the next connection would merge it into that batch.
func TestPendingRowsAreDiscardedOnDrop(t *testing.T) {
	s, rec := newTestSubscriber(t)
	s.setLive(true)

	s.handleLine(`RDATA events master batch ["ev",["$a","!r:x","m.room.message","",null,null,null,false,false]]`)
	s.setLive(false)
	s.setLive(true)
	s.handleLine(`RDATA events master 7 ["ev",["$b","!r:x","m.room.message","",null,null,null,false,false]]`)

	if len(rec.batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(rec.batches))
	}
	if len(rec.batches[0].rows) != 1 {
		t.Errorf("batch has %d rows; the stale buffered row survived the drop", len(rec.batches[0].rows))
	}
}

func TestPOSITIONAdvancesTheStream(t *testing.T) {
	s, rec := newTestSubscriber(t)

	s.handleLine(`POSITION events master 100 200`)
	if s.Position("events") != 200 {
		t.Errorf("position = %d, want the new token 200, not the previous one", s.Position("events"))
	}
	if len(rec.positions) != 1 || rec.positions[0].pos != 200 {
		t.Errorf("positions = %+v", rec.positions)
	}

	// A POSITION must never move a stream backwards.
	s.handleLine(`POSITION events master 50 60`)
	if s.Position("events") != 200 {
		t.Errorf("position went backwards to %d", s.Position("events"))
	}
}

// We never publish, so a row bearing our own instance name means another worker
// is running under it -- which would double every decision we make.
func TestOurOwnInstanceNameIsIgnored(t *testing.T) {
	s, rec := newTestSubscriber(t)
	s.handleLine(`RDATA events av-gofederation-worker-1 5 ["ev",["$a","!r:x","m.room.message","",null,null,null,false,false]]`)
	s.handleLine(`POSITION events av-gofederation-worker-1 1 5`)
	if len(rec.batches) != 0 || len(rec.positions) != 0 {
		t.Errorf("our own echo was processed: %v %v", rec.batches, rec.positions)
	}
	if s.Position("events") != 0 {
		t.Errorf("our own echo advanced the position to %d", s.Position("events"))
	}
}

func TestRemoteServerUp(t *testing.T) {
	s, rec := newTestSubscriber(t)
	s.handleLine(`REMOTE_SERVER_UP matrix.org`)
	if len(rec.serversUp) != 1 || rec.serversUp[0] != "matrix.org" {
		t.Errorf("serversUp = %v", rec.serversUp)
	}
}

// Seeding is how we get a starting position without publishing REPLICATE. It is
// a lower bound, so it must never pull a live position backwards.
func TestSeedIsALowerBound(t *testing.T) {
	s, _ := newTestSubscriber(t)
	s.Seed(map[string]int64{"events": 100})
	if s.Position("events") != 100 {
		t.Fatalf("seed did not apply: %d", s.Position("events"))
	}
	s.handleLine(`RDATA events master 150 ["ev",["$a","!r:x","m.room.message","",null,null,null,false,false]]`)
	s.Seed(map[string]int64{"events": 120})
	if s.Position("events") != 150 {
		t.Errorf("a stale seed pulled the position back to %d, want 150", s.Position("events"))
	}
}

func TestMalformedLinesAreIgnored(t *testing.T) {
	s, rec := newTestSubscriber(t)
	for _, line := range []string{
		"", "REPLICATE", "RDATA", "RDATA events", "RDATA events master",
		"RDATA events master notanumber []", "POSITION events master 1",
		"UNKNOWN_COMMAND whatever",
	} {
		s.handleLine(line)
	}
	if len(rec.batches) != 0 || len(rec.positions) != 0 {
		t.Errorf("malformed input produced output: %v %v", rec.batches, rec.positions)
	}
}

// The one property the whole package is built around, checked structurally
// because it cannot be checked behaviourally: the failure mode is a call that
// exists, not a call that returns the wrong thing.
//
// A real federation sender publishes REPLICATE on connect and FEDERATION_ACK
// after each transaction. Both would perturb the live cluster we shadow -- the
// first makes every other worker broadcast its positions on our account, the
// second tells the main process it may clear queues we have not actually
// delivered. So no code path here may publish, and a change that adds one has
// to delete this test to do it.
//
// The check reads the AST rather than the file text, so the comments in this
// package that EXPLAIN why we do not publish do not trip it.
func TestPackageNeverPublishes(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Method names on a Redis client that put bytes on the bus.
	forbidden := map[string]bool{
		"Publish": true, "SPublish": true, "Eval": true, "EvalSha": true, "Do": true,
	}

	files := 0
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			files++
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if forbidden[sel.Sel.Name] {
					t.Errorf("%s calls %s(); this package must never write to the "+
						"replication bus (docs/shadow-safety.md)",
						filepath.Base(name), sel.Sel.Name)
				}
				return true
			})

			// A command name in a string literal would be the other half of a
			// publish, wherever the call itself lived.
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				for _, bad := range []string{"REPLICATE", "FEDERATION_ACK"} {
					if strings.Contains(lit.Value, bad) {
						t.Errorf("%s has %q in a string literal; this package must never "+
							"send that command", filepath.Base(name), bad)
					}
				}
				return true
			})
		}
	}
	if files == 0 {
		t.Fatal("no source files were parsed; the guard is not doing anything")
	}
}
