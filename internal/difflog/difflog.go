// Package difflog records what the shadow decided, durably.
//
// The promotion gate -- the decision to stop shadowing and start sending -- is
// measured in weeks, so these counters are persisted and restored at startup
// rather than living only in Prometheus. Counters that reset on every deploy
// cannot answer "has this agreed with the real sender for a month?", which is
// the only question that matters before flipping shadow.enabled.
//
// Two things are recorded and they answer different questions:
//
//   - The TOTALS say how much was decided and how much of it was approximate.
//     They are cheap, always on, and are what the gate is read from.
//   - The SAMPLES are individual routing decisions, written to a rotating file
//     so they can be diffed against the real sender's logs by hand. They are
//     rate-limited, because at this server's volume logging every decision
//     would produce more data than the events themselves.
package difflog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Totals is the durable summary.
type Totals struct {
	// Since is when counting started, preserved across restarts so the gate
	// can be read as a rate over a known window.
	Since time.Time `json:"since"`
	// Restarts counts process starts, so a suspiciously good match rate can be
	// checked against how long the worker has actually been up.
	Restarts int64 `json:"restarts"`

	// Events examined, routed, and skipped by reason.
	EventsExamined int64            `json:"events_examined"`
	EventsRouted   int64            `json:"events_routed"`
	EventsSkipped  map[string]int64 `json:"events_skipped"`

	// Destinations resolved in total and in our shard. The ratio between them
	// should be roughly 1/len(federation_sender_instances); a drift says the
	// shard function or the instance list has changed under us.
	DestinationsResolved int64 `json:"destinations_resolved"`
	DestinationsOurs     int64 `json:"destinations_ours"`

	// ApproximateRoutes is the count of events routed from current state
	// rather than state before the event. This is the known difference from
	// Synapse's algorithm, and sizing it is the main thing the shadow is for.
	ApproximateRoutes int64 `json:"approximate_routes"`
	// ApproximateBy breaks that down by cause, so "we have not implemented
	// state resolution for forked DAGs" can be told apart from "that prev
	// event was an outlier".
	ApproximateBy map[string]int64 `json:"approximate_by"`
	// RescindedInvites counts the rare rule firing, because its absence from a
	// diff would otherwise be indistinguishable from it never happening.
	RescindedInvites int64 `json:"rescinded_invites"`

	// Transactions and their contents.
	Transactions int64 `json:"transactions"`
	PDUs         int64 `json:"pdus"`
	EDUs         int64 `json:"edus"`
}

// Writer accumulates totals and writes samples.
type Writer struct {
	dir      string
	instance string

	mu     sync.Mutex
	totals Totals
	// dirty is set by every update and cleared by a flush, so a quiet period
	// costs no writes.
	dirty bool

	samples     *sampleFile
	sampleEvery int
	sampleCount int64
}

// Config builds a Writer.
type Config struct {
	// Dir holds stats.json and the sample files.
	Dir string
	// Instance is the sender we shadow, so several shadows can share a volume.
	Instance string
	// SampleEvery writes one routing decision in N to the sample file. Zero
	// disables sampling entirely; the totals are always kept.
	SampleEvery int
	// MaxSampleBytes rotates the sample file when it grows past this.
	MaxSampleBytes int64
}

// Open loads any existing totals and prepares the sample file.
func Open(cfg Config) (*Writer, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("difflog: no directory configured")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("difflog: %w", err)
	}

	w := &Writer{
		dir:         cfg.Dir,
		instance:    cfg.Instance,
		sampleEvery: cfg.SampleEvery,
		totals:      Totals{Since: time.Now(), EventsSkipped: map[string]int64{}},
	}

	// Restoring is the whole point of the package, so a corrupt file must not
	// stop the worker: losing the history is bad, refusing to start over it is
	// worse. The loss is reported by the Since field jumping forward.
	if err := w.load(); err != nil {
		w.totals = Totals{Since: time.Now(), EventsSkipped: map[string]int64{},
			ApproximateBy: map[string]int64{}}
	}
	w.totals.Restarts++

	if cfg.SampleEvery > 0 {
		s, err := openSampleFile(filepath.Join(cfg.Dir, "samples.jsonl"), cfg.MaxSampleBytes)
		if err != nil {
			return nil, err
		}
		w.samples = s
	}
	return w, w.flushLocked()
}

func (w *Writer) statsPath() string {
	name := "stats.json"
	if w.instance != "" {
		name = w.instance + ".stats.json"
	}
	return filepath.Join(w.dir, name)
}

func (w *Writer) load() error {
	b, err := os.ReadFile(w.statsPath())
	if err != nil {
		return err
	}
	var t Totals
	if err := json.Unmarshal(b, &t); err != nil {
		return err
	}
	if t.EventsSkipped == nil {
		t.EventsSkipped = map[string]int64{}
	}
	if t.ApproximateBy == nil {
		t.ApproximateBy = map[string]int64{}
	}
	if t.Since.IsZero() {
		t.Since = time.Now()
	}
	w.totals = t
	return nil
}

// Totals returns a snapshot.
func (w *Writer) Totals() Totals {
	w.mu.Lock()
	defer w.mu.Unlock()
	t := w.totals
	skipped := make(map[string]int64, len(t.EventsSkipped))
	for k, v := range t.EventsSkipped {
		skipped[k] = v
	}
	t.EventsSkipped = skipped
	by := make(map[string]int64, len(t.ApproximateBy))
	for k, v := range t.ApproximateBy {
		by[k] = v
	}
	t.ApproximateBy = by
	return t
}

// RecordSkip counts an event that was not federated.
func (w *Writer) RecordSkip(reason string) {
	w.mu.Lock()
	w.totals.EventsExamined++
	w.totals.EventsSkipped[reason]++
	w.dirty = true
	w.mu.Unlock()
}

// Route is one routing decision, as recorded in the sample file.
type Route struct {
	Time    time.Time `json:"time"`
	EventID string    `json:"event_id"`
	// All is every destination in the room; Ours is our shard's share.
	All  []string `json:"all"`
	Ours []string `json:"ours"`
	// Approximate says the answer came from current state, and Cause says why.
	Approximate bool   `json:"approximate"`
	Cause       string `json:"cause,omitempty"`
}

// RecordRoute counts a routing decision and may sample it.
func (w *Writer) RecordRoute(eventID string, all, ours []string, approximate bool, cause string) {
	w.mu.Lock()
	w.totals.EventsExamined++
	w.totals.DestinationsResolved += int64(len(all))
	w.totals.DestinationsOurs += int64(len(ours))
	if len(ours) > 0 {
		w.totals.EventsRouted++
	}
	if approximate {
		w.totals.ApproximateRoutes++
		if w.totals.ApproximateBy == nil {
			w.totals.ApproximateBy = map[string]int64{}
		}
		w.totals.ApproximateBy[cause]++
	}
	w.dirty = true

	n := w.sampleCount
	w.sampleCount++
	sampler := w.samples
	every := w.sampleEvery
	w.mu.Unlock()

	if sampler != nil && every > 0 && n%int64(every) == 0 {
		// Written outside the lock: a sample is a file write and the totals
		// are hot.
		_ = sampler.write(Route{
			Time: time.Now(), EventID: eventID, All: all, Ours: ours,
			Approximate: approximate, Cause: cause,
		})
	}
}

// RecordRescindedInvite counts the rare destination-adding rule firing.
func (w *Writer) RecordRescindedInvite() {
	w.mu.Lock()
	w.totals.RescindedInvites++
	w.dirty = true
	w.mu.Unlock()
}

// RecordTransaction counts an assembled transaction.
func (w *Writer) RecordTransaction(pdus, edus int) {
	w.mu.Lock()
	w.totals.Transactions++
	w.totals.PDUs += int64(pdus)
	w.totals.EDUs += int64(edus)
	w.dirty = true
	w.mu.Unlock()
}

// Flush writes the totals to disk if anything changed.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.dirty {
		return nil
	}
	return w.flushLocked()
}

// flushLocked writes via a temporary file and a rename, so a crash mid-write
// leaves the previous totals rather than a truncated file. Weeks of history is
// exactly the thing not to lose to a power cut.
func (w *Writer) flushLocked() error {
	b, err := json.MarshalIndent(w.totals, "", "  ")
	if err != nil {
		return fmt.Errorf("difflog: %w", err)
	}
	tmp := w.statsPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("difflog: %w", err)
	}
	if err := os.Rename(tmp, w.statsPath()); err != nil {
		return fmt.Errorf("difflog: %w", err)
	}
	w.dirty = false
	return nil
}

// Close flushes and releases the sample file.
func (w *Writer) Close() error {
	err := w.Flush()
	w.mu.Lock()
	s := w.samples
	w.samples = nil
	w.mu.Unlock()
	if s != nil {
		if cerr := s.close(); err == nil {
			err = cerr
		}
	}
	return err
}
