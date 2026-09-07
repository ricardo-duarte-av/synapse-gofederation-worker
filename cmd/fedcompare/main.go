// Command fedcompare diffs what Synapse actually sent against what this worker
// would have sent, for one destination.
//
// The two inputs come from cmd/fedrecorder (ground truth, captured on the wire
// in front of a test homeserver) and from the worker's own
// shadow.capture_destinations.
//
// It deliberately does NOT compare transaction framing. Which PDUs share a
// transaction depends on what was queued when a sender's loop happened to run,
// so two correct senders differ there; comparing it would report timing as
// error. What is compared is what carries meaning: which events reached the
// destination, with what bytes, and which EDUs went with them.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/capture"
)

var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	var (
		synapsePath = flag.String("synapse", "", "capture file from cmd/fedrecorder")
		workerPath  = flag.String("worker", "", "capture file from the worker")
		destination = flag.String("destination", "", "limit to one destination (recommended)")
		since       = flag.String("since", "", "ignore records before this RFC3339 time (default: the overlap of the two captures)")
		until       = flag.String("until", "", "ignore records after this RFC3339 time (default: the overlap of the two captures)")
		byEventTime = flag.Bool("by-event-time", false,
			"window on the events' own origin_server_ts rather than on when each side processed them; "+
				"needed when either capture is a replay of history rather than a live follow")
		asJSON      = flag.Bool("json", false, "emit the diff as JSON")
		showVersion = flag.Bool("version", false, "print build information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("fedcompare %s (%s, built %s)\n", tag, commit, buildTime)
		return
	}
	if *synapsePath == "" || *workerPath == "" {
		fmt.Fprintln(os.Stderr, "fedcompare: -synapse and -worker are required")
		os.Exit(2)
	}

	code, err := run(*synapsePath, *workerPath, *destination, *since, *until, *byEventTime, *asJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fedcompare: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(synapsePath, workerPath, destination, since, until string, byEventTime, asJSON bool) (int, error) {
	synapse, synSkipped, err := capture.ReadFile(synapsePath)
	if err != nil {
		return 0, err
	}
	worker, workSkipped, err := capture.ReadFile(workerPath)
	if err != nil {
		return 0, err
	}

	// Default to the period both captures could have observed. The recorder's
	// file is cumulative while the worker's covers only when the worker ran, so
	// comparing the whole of each reports everything outside that period as
	// MISSING -- true, and useless.
	window := capture.Overlap(synapse, worker)
	if byEventTime {
		window = capture.OverlapByEventTime(synapse, worker)
	}
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return 0, fmt.Errorf("parsing -since: %w", err)
		}
		window.Since = t
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return 0, fmt.Errorf("parsing -until: %w", err)
		}
		window.Until = t
	}

	diff, err := capture.Compare(destination, window, synapse, worker)
	if err != nil {
		return 0, err
	}

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(diff); err != nil {
			return 0, err
		}
	} else {
		report(diff, synSkipped, workSkipped)
	}

	// Three outcomes, not two. A comparison that looked at nothing has
	// established nothing, and calling that a pass is how a broken rig goes
	// unnoticed: the output is green and nobody asks what it was green about.
	if diff.Inconclusive() {
		return 2, nil
	}
	// A non-zero exit otherwise only for PDU disagreement. EDUs are excluded
	// from the verdict while the m.device_list_update body is knowingly
	// incomplete -- a red light that is permanently on stops being read.
	if !diff.Agreed() {
		return 1, nil
	}
	return 0, nil
}

func report(d capture.Diff, synSkipped, workSkipped int) {
	if d.Destination != "" {
		fmt.Printf("destination: %s\n", d.Destination)
	}
	if d.Window.Since.IsZero() && d.Window.Until.IsZero() {
		fmt.Printf("window: unbounded -- one of the captures has no timestamps\n")
	} else {
		basis := "processed"
		if d.Window.ByEventTime {
			basis = "event timestamps"
		}
		fmt.Printf("window: %s .. %s  (by %s; the period BOTH captures cover)\n",
			d.Window.Since.Format(time.RFC3339), d.Window.Until.Format(time.RFC3339), basis)
	}
	fmt.Printf("transactions: synapse=%d worker=%d  (framing is not comparable; see the docs)\n",
		d.SynapseTransactions, d.WorkerTransactions)
	if synSkipped > 0 || workSkipped > 0 {
		fmt.Printf("unparseable lines skipped: synapse=%d worker=%d\n", synSkipped, workSkipped)
	}

	fmt.Printf("\nPDUs\n")
	fmt.Printf("  delivered by both : %d\n", d.PDUsBoth)
	fmt.Printf("  only synapse sent : %d\n", len(d.PDUsOnlySynapse))
	fmt.Printf("  only we sent      : %d\n", len(d.PDUsOnlyWorker))
	fmt.Printf("  bytes differ      : %d\n", len(d.PDUsDiffering))

	if d.PDUsIdentifiedByContent > 0 {
		fmt.Printf("  (%d PDUs carried no event_id and were matched on a content hash,\n"+
			"   so a MISSING and an EXTRA may be one event whose content differs)\n",
			d.PDUsIdentifiedByContent)
	}
	for i, ref := range d.PDUsOnlySynapse {
		if i >= 10 {
			fmt.Printf("    ... and %d more\n", len(d.PDUsOnlySynapse)-10)
			break
		}
		fmt.Printf("    MISSING %s\n", ref.Display)
	}
	for i, ref := range d.PDUsOnlyWorker {
		if i >= 10 {
			fmt.Printf("    ... and %d more\n", len(d.PDUsOnlyWorker)-10)
			break
		}
		fmt.Printf("    EXTRA   %s\n", ref.Display)
	}
	for i, p := range d.PDUsDiffering {
		if i >= 3 {
			fmt.Printf("    ... and %d more\n", len(d.PDUsDiffering)-3)
			break
		}
		fmt.Printf("    DIFFERS %s\n      synapse: %s\n      ours:    %s\n",
			p.EventID, truncate(p.Synapse), truncate(p.Worker))
	}

	fmt.Printf("\nEDUs\n")
	if len(d.EDUs) == 0 {
		fmt.Printf("  none on either side\n")
	}
	for _, e := range d.EDUs {
		fmt.Printf("  %-24s synapse=%-4d worker=%-4d", e.Type, e.Synapse, e.Worker)
		switch {
		case !e.Implemented:
			// Said first and said plainly. A zero here is not "none this
			// window", it is "not built", and the two look identical in a
			// column of numbers.
			fmt.Printf("  NOT IMPLEMENTED by this worker\n")
		case !e.Comparable:
			fmt.Printf("  (content not comparable: %s)\n", e.Note)
		default:
			fmt.Printf("  content: both=%d only-synapse=%d only-worker=%d\n",
				e.ContentBoth, len(e.ContentOnlySynapse), len(e.ContentOnlyWorker))
			if e.Note != "" {
				fmt.Printf("      note: %s\n", e.Note)
			}
		}
	}

	fmt.Printf("\n")
	switch {
	case d.Inconclusive():
		fmt.Printf("INCONCLUSIVE: %d PDUs were compared.\n", d.Compared)
		if d.Window.Empty() {
			fmt.Printf("The two captures cover disjoint periods, so there is nothing\n" +
				"they both observed. If one of them is a replay of history rather than\n" +
				"a live follow, window on the events instead: -by-event-time\n")
		}
	case d.Agreed():
		fmt.Printf("PDUs agree (%d compared).\n", d.Compared)
	default:
		fmt.Printf("PDUs DISAGREE (%d compared).\n", d.Compared)
	}
}

func truncate(s string) string {
	const max = 200
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
