package difflog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writer(t *testing.T, dir string, sampleEvery int) *Writer {
	t.Helper()
	w, err := Open(Config{Dir: dir, Instance: "w1", SampleEvery: sampleEvery, MaxSampleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// The whole point of the package: totals outlive the process, because a
// promotion gate measured in weeks cannot be judged from counters that reset on
// every deploy.
func TestTotalsSurviveRestart(t *testing.T) {
	dir := t.TempDir()

	w := writer(t, dir, 0)
	w.RecordRoute("$a", []string{"b.example", "c.example"}, []string{"b.example"}, true)
	w.RecordSkip("remote origin")
	w.RecordTransaction(3, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := writer(t, dir, 0)
	defer w2.Close()
	got := w2.Totals()

	if got.EventsExamined != 2 || got.EventsRouted != 1 {
		t.Errorf("examined=%d routed=%d, want 2 and 1", got.EventsExamined, got.EventsRouted)
	}
	if got.DestinationsResolved != 2 || got.DestinationsOurs != 1 {
		t.Errorf("resolved=%d ours=%d", got.DestinationsResolved, got.DestinationsOurs)
	}
	if got.ApproximateRoutes != 1 {
		t.Errorf("approximate = %d", got.ApproximateRoutes)
	}
	if got.EventsSkipped["remote origin"] != 1 {
		t.Errorf("skipped = %v", got.EventsSkipped)
	}
	if got.Transactions != 1 || got.PDUs != 3 || got.EDUs != 1 {
		t.Errorf("transactions=%d pdus=%d edus=%d", got.Transactions, got.PDUs, got.EDUs)
	}
	// Restarts is what lets a suspiciously good match rate be checked against
	// how long the worker has actually been up.
	if got.Restarts != 2 {
		t.Errorf("Restarts = %d, want 2", got.Restarts)
	}
}

// Losing the history is bad; refusing to start over it is worse. The loss is
// visible as Since jumping forward.
func TestCorruptStatsFileDoesNotStopStartup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "w1.stats.json"), []byte("{ not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := Open(Config{Dir: dir, Instance: "w1"})
	if err != nil {
		t.Fatalf("Open refused to start over a corrupt stats file: %v", err)
	}
	defer w.Close()
	if got := w.Totals(); got.EventsExamined != 0 || got.Since.IsZero() {
		t.Errorf("totals were not reset cleanly: %+v", got)
	}
}

// Several shadows can share a volume without overwriting each other.
func TestInstancesGetSeparateStatsFiles(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(Config{Dir: dir, Instance: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(Config{Dir: dir, Instance: "w2"})
	if err != nil {
		t.Fatal(err)
	}
	a.RecordSkip("x")
	a.RecordSkip("x")
	b.RecordSkip("x")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	a2, _ := Open(Config{Dir: dir, Instance: "w1"})
	defer a2.Close()
	b2, _ := Open(Config{Dir: dir, Instance: "w2"})
	defer b2.Close()
	if a2.Totals().EventsExamined != 2 {
		t.Errorf("w1 examined = %d, want 2", a2.Totals().EventsExamined)
	}
	if b2.Totals().EventsExamined != 1 {
		t.Errorf("w2 examined = %d, want 1", b2.Totals().EventsExamined)
	}
}

func TestSamplingWritesOneInN(t *testing.T) {
	dir := t.TempDir()
	w := writer(t, dir, 5)
	for i := 0; i < 20; i++ {
		w.RecordRoute("$e", []string{"b.example"}, []string{"b.example"}, false)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join(dir, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var r Route
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("sample is not valid JSON: %v", err)
		}
		if r.EventID != "$e" {
			t.Errorf("sample = %+v", r)
		}
		lines++
	}
	if lines != 4 {
		t.Errorf("wrote %d samples for 20 routes at one in 5, want 4", lines)
	}
	// Totals count everything, not just the sampled ones.
	if got := w.Totals().EventsExamined; got != 20 {
		t.Errorf("examined = %d, want all 20", got)
	}
}

func TestSamplingDisabledWritesNoFile(t *testing.T) {
	dir := t.TempDir()
	w := writer(t, dir, 0)
	w.RecordRoute("$e", []string{"b.example"}, []string{"b.example"}, false)
	w.Close()
	if _, err := os.Stat(filepath.Join(dir, "samples.jsonl")); !os.IsNotExist(err) {
		t.Error("a sample file was created with sampling disabled")
	}
}

// An unbounded archive of samples would be a disk-filling liability on a worker
// whose whole point is to be harmless.
func TestSampleFileRotates(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, Instance: "w1", SampleEvery: 1, MaxSampleBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		w.RecordRoute("$event-with-a-longish-id", []string{"b.example", "c.example"},
			[]string{"b.example"}, false)
	}
	w.Close()

	cur, err := os.Stat(filepath.Join(dir, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if cur.Size() > 512 {
		t.Errorf("current sample file is %d bytes, over the 512 limit", cur.Size())
	}
	if _, err := os.Stat(filepath.Join(dir, "samples.jsonl.1")); err != nil {
		t.Errorf("no rotated file was kept: %v", err)
	}
	// Exactly one generation is kept, deliberately.
	if _, err := os.Stat(filepath.Join(dir, "samples.jsonl.2")); !os.IsNotExist(err) {
		t.Error("a second rotated generation exists; only one should be kept")
	}
}

// A crash mid-write must leave the previous totals, not a truncated file.
func TestFlushIsAtomic(t *testing.T) {
	dir := t.TempDir()
	w := writer(t, dir, 0)
	w.RecordSkip("x")
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "w1.stats.json.tmp")); !os.IsNotExist(err) {
		t.Error("the temporary file was left behind")
	}
	b, err := os.ReadFile(filepath.Join(dir, "w1.stats.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got Totals
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("the stats file is not valid JSON: %v", err)
	}
	w.Close()
}

func TestOpenRequiresADirectory(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("expected an error with no directory configured")
	}
}
