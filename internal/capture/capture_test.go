package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func rec(source Source, dest, txnID string, pdus []string, edus ...EDU) Record {
	raw := make([]json.RawMessage, 0, len(pdus))
	for _, p := range pdus {
		raw = append(raw, json.RawMessage(p))
	}
	body, err := json.Marshal(Transaction{
		Origin: "a.example", OriginServerTS: 1, PDUs: raw, EDUs: edus,
	})
	if err != nil {
		panic(err)
	}
	return Record{
		Time: time.Now(), Source: source, Destination: dest,
		Origin: "a.example", TxnID: txnID, Body: body,
	}
}

func pdu(id, body string) string {
	return fmt.Sprintf(`{"event_id":%q,"type":"m.room.message","content":{"body":%q}}`, id, body)
}

func TestCompareAgreesOnIdenticalPDUs(t *testing.T) {
	// Deliberately DIFFERENT framing: Synapse sent one transaction of two,
	// we sent two of one. That is not a disagreement, and the whole design
	// rests on not reporting it as one.
	syn := []Record{rec(SourceSynapse, "b.example", "1", []string{pdu("$a", "one"), pdu("$b", "two")})}
	ours := []Record{
		rec(SourceWorker, "b.example", "9", []string{pdu("$a", "one")}),
		rec(SourceWorker, "b.example", "10", []string{pdu("$b", "two")}),
	}

	d, err := Compare("b.example", syn, ours)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Agreed() {
		t.Errorf("different framing was reported as disagreement: %+v", d)
	}
	if d.PDUsBoth != 2 {
		t.Errorf("PDUsBoth = %d, want 2", d.PDUsBoth)
	}
	if d.SynapseTransactions != 1 || d.WorkerTransactions != 2 {
		t.Errorf("transaction counts = %d/%d, want 1/2 recorded for context",
			d.SynapseTransactions, d.WorkerTransactions)
	}
}

// The finding that matters most: an event both sent, with different bytes.
func TestCompareDetectsDifferingBytes(t *testing.T) {
	syn := []Record{rec(SourceSynapse, "b.example", "1", []string{pdu("$a", "one")})}
	ours := []Record{rec(SourceWorker, "b.example", "1", []string{pdu("$a", "ONE")})}

	d, _ := Compare("b.example", syn, ours)
	if len(d.PDUsDiffering) != 1 || d.PDUsDiffering[0].EventID != "$a" {
		t.Fatalf("PDUsDiffering = %+v", d.PDUsDiffering)
	}
	if d.Agreed() {
		t.Error("differing bytes reported as agreement")
	}
}

// Key ORDER is not a difference: both sides are canonicalised before comparing,
// so a report is about content rather than encoding.
func TestCompareIgnoresKeyOrder(t *testing.T) {
	syn := []Record{rec(SourceSynapse, "b.example", "1",
		[]string{`{"event_id":"$a","type":"m.room.message","content":{"body":"x"}}`})}
	ours := []Record{rec(SourceWorker, "b.example", "1",
		[]string{`{"content":{"body":"x"},"type":"m.room.message","event_id":"$a"}`})}

	d, _ := Compare("b.example", syn, ours)
	if !d.Agreed() {
		t.Errorf("key order was reported as a difference: %+v", d.PDUsDiffering)
	}
}

func TestCompareDetectsMissingAndExtra(t *testing.T) {
	syn := []Record{rec(SourceSynapse, "b.example", "1",
		[]string{pdu("$a", "one"), pdu("$missed", "two")})}
	ours := []Record{rec(SourceWorker, "b.example", "1",
		[]string{pdu("$a", "one"), pdu("$phantom", "three")})}

	d, _ := Compare("b.example", syn, ours)
	if len(d.PDUsOnlySynapse) != 1 || d.PDUsOnlySynapse[0].ID != "$missed" {
		t.Errorf("PDUsOnlySynapse = %v", d.PDUsOnlySynapse)
	}
	if len(d.PDUsOnlyWorker) != 1 || d.PDUsOnlyWorker[0].ID != "$phantom" {
		t.Errorf("PDUsOnlyWorker = %v", d.PDUsOnlyWorker)
	}
}

func TestCompareFiltersByDestination(t *testing.T) {
	syn := []Record{
		rec(SourceSynapse, "b.example", "1", []string{pdu("$a", "one")}),
		rec(SourceSynapse, "other.example", "2", []string{pdu("$elsewhere", "x")}),
	}
	ours := []Record{rec(SourceWorker, "b.example", "1", []string{pdu("$a", "one")})}

	d, _ := Compare("b.example", syn, ours)
	if !d.Agreed() {
		t.Errorf("another destination's traffic leaked into the comparison: %+v", d)
	}
}

// EDU content is compared only where it is deterministic. Presence and typing
// are snapshots of a moving thing; comparing them between senders that ran at
// different instants tests the clock, not the code.
func TestEDUComparabilityIsPerType(t *testing.T) {
	syn := []Record{rec(SourceSynapse, "b.example", "1", nil,
		EDU{Type: "m.direct_to_device", Content: json.RawMessage(`{"message_id":"m1"}`)},
		EDU{Type: "m.presence", Content: json.RawMessage(`{"push":[{"user_id":"@a:a.example"}]}`)},
	)}
	ours := []Record{rec(SourceWorker, "b.example", "1", nil,
		EDU{Type: "m.direct_to_device", Content: json.RawMessage(`{"message_id":"m1"}`)},
		EDU{Type: "m.presence", Content: json.RawMessage(`{"push":[{"user_id":"@b:a.example"}]}`)},
	)}

	d, _ := Compare("b.example", syn, ours)
	byType := map[string]EDUDiff{}
	for _, e := range d.EDUs {
		byType[e.Type] = e
	}

	dtd := byType["m.direct_to_device"]
	if !dtd.Comparable || dtd.ContentBoth != 1 {
		t.Errorf("m.direct_to_device = %+v, want comparable and matched", dtd)
	}
	pres := byType["m.presence"]
	if pres.Comparable {
		t.Error("m.presence content was compared; it is a snapshot and differs legitimately")
	}
	if pres.Synapse != 1 || pres.Worker != 1 {
		t.Errorf("m.presence counts = %d/%d", pres.Synapse, pres.Worker)
	}
	// EDUs never decide the verdict while the device-list body is incomplete.
	if !d.Agreed() {
		t.Error("an EDU difference failed the PDU verdict")
	}
}

// The same content twice is not the same as once.
func TestEDUContentIsAMultiset(t *testing.T) {
	same := EDU{Type: "m.direct_to_device", Content: json.RawMessage(`{"message_id":"m1"}`)}
	syn := []Record{rec(SourceSynapse, "b.example", "1", nil, same, same)}
	ours := []Record{rec(SourceWorker, "b.example", "1", nil, same)}

	d, _ := Compare("b.example", syn, ours)
	for _, e := range d.EDUs {
		if e.Type != "m.direct_to_device" {
			continue
		}
		if e.ContentBoth != 1 || len(e.ContentOnlySynapse) != 1 {
			t.Errorf("duplicate content collapsed: %+v", e)
		}
	}
}

func TestWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.jsonl")
	w, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	in := rec(SourceWorker, "b.example", "42", []string{pdu("$a", "one")})
	if err := w.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	out, skipped, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 || len(out) != 1 {
		t.Fatalf("read %d records, %d skipped", len(out), skipped)
	}
	if out[0].TxnID != "42" || out[0].Destination != "b.example" {
		t.Errorf("round trip lost fields: %+v", out[0])
	}
	// The body must survive verbatim; the whole comparison is about bytes.
	if string(out[0].Body) != string(in.Body) {
		t.Errorf("body changed:\n in: %s\nout: %s", in.Body, out[0].Body)
	}
}

func TestWriterFiltersDestinations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.jsonl")
	w, err := Open(Config{Path: path, Destinations: []string{"wanted.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if !w.Captures("wanted.example") || w.Captures("other.example") {
		t.Fatal("Captures does not respect the destination list")
	}
	_ = w.Write(rec(SourceWorker, "wanted.example", "1", nil))
	_ = w.Write(rec(SourceWorker, "other.example", "2", nil))

	out, _, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Destination != "wanted.example" {
		t.Errorf("captured %d records: %+v", len(out), out)
	}
}

// A capture is only useful if it is complete, so rotation must keep the
// configured number of generations and no more.
func TestWriterRotatesAndKeepsGenerations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.jsonl")
	w, err := Open(Config{Path: path, MaxBytes: 400, Generations: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if err := w.Write(rec(SourceWorker, "b.example", fmt.Sprint(i),
			[]string{pdu("$e", strings.Repeat("x", 40))})); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	if info, err := os.Stat(path); err != nil || info.Size() > 400 {
		t.Errorf("current file: %v, size over the bound", err)
	}
	for _, g := range []string{".1", ".2"} {
		if _, err := os.Stat(path + g); err != nil {
			t.Errorf("generation %s missing: %v", g, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Error("a third generation was kept; only two were configured")
	}
}

// A truncated last line, from a process killed mid-write, is normal. Refusing
// to read the rest of the evidence over it would be perverse.
func TestReadSkipsMalformedLinesRatherThanFailing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.jsonl")
	good, _ := json.Marshal(rec(SourceWorker, "b.example", "1", []string{pdu("$a", "x")}))
	body := string(good) + "\n" + `{"time":"broken` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	out, skipped, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || skipped != 1 {
		t.Errorf("read %d records, %d skipped, want 1 and 1", len(out), skipped)
	}
}

// This deployment's default room version is 12, so almost no PDU carries an
// event_id. The identity has to come from the content, and it must be short
// enough to read: an earlier version used the whole canonical event as the key
// and printed a 30 KB server ACL into the terminal for a one-field difference.
func TestPDUsWithoutEventIDGetAShortContentIdentity(t *testing.T) {
	v12 := `{"type":"m.room.message","room_id":"!r:a.example",` +
		`"sender":"@u:a.example","origin_server_ts":1700000000000,` +
		`"content":{"body":"` + strings.Repeat("x", 5000) + `"}}`

	syn := []Record{rec(SourceSynapse, "b.example", "1", []string{v12})}
	ours := []Record{rec(SourceWorker, "b.example", "1", []string{v12})}

	d, err := Compare("b.example", syn, ours)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Agreed() {
		t.Fatalf("identical content-identified PDUs disagreed: %+v", d)
	}
	if d.PDUsIdentifiedByContent != 1 {
		t.Errorf("PDUsIdentifiedByContent = %d, want 1", d.PDUsIdentifiedByContent)
	}

	// Now change it, and check the report stays readable.
	changed := strings.Replace(v12, `"body":"`+strings.Repeat("x", 5000), `"body":"changed`, 1)
	ours = []Record{rec(SourceWorker, "b.example", "1", []string{changed})}
	d, _ = Compare("b.example", syn, ours)

	if len(d.PDUsOnlySynapse) != 1 || len(d.PDUsOnlyWorker) != 1 {
		t.Fatalf("a content change should appear as one missing and one extra: %+v", d)
	}
	for _, ref := range append(d.PDUsOnlySynapse, d.PDUsOnlyWorker...) {
		if !strings.HasPrefix(ref.ID, "sha:") {
			t.Errorf("ID = %q, want a content hash", ref.ID)
		}
		if len(ref.Display) > 200 {
			t.Errorf("Display is %d chars; the report must stay readable", len(ref.Display))
		}
		// It still has to be findable in the database.
		for _, want := range []string{"m.room.message", "!r:a.example", "@u:a.example", "1700000000000"} {
			if !strings.Contains(ref.Display, want) {
				t.Errorf("Display %q does not mention %q", ref.Display, want)
			}
		}
	}
}

// A zero in the worker column means two very different things, and a column of
// numbers cannot tell them apart: "none happened in this window" or "this is
// not built yet". The second is exactly the gap that goes unnoticed for weeks
// because the report looked fine.
func TestUnimplementedEDUTypesAreMarked(t *testing.T) {
	syn := []Record{rec(SourceSynapse, "b.example", "1", nil,
		EDU{Type: "m.typing", Content: json.RawMessage(`{"typing":true}`)},
		EDU{Type: "m.direct_to_device", Content: json.RawMessage(`{"message_id":"m1"}`)},
	)}
	ours := []Record{rec(SourceWorker, "b.example", "1", nil,
		EDU{Type: "m.direct_to_device", Content: json.RawMessage(`{"message_id":"m1"}`)},
	)}

	d, err := Compare("b.example", syn, ours)
	if err != nil {
		t.Fatal(err)
	}
	byType := map[string]EDUDiff{}
	for _, e := range d.EDUs {
		byType[e.Type] = e
	}

	if byType["m.typing"].Implemented {
		t.Error("m.typing is reported as implemented; this worker does not send typing")
	}
	if !byType["m.direct_to_device"].Implemented {
		t.Error("m.direct_to_device is reported as unimplemented; it is sent")
	}
	// An unimplemented type still must not fail the PDU verdict.
	if !d.Agreed() {
		t.Error("an unimplemented EDU type failed the PDU verdict")
	}
}
