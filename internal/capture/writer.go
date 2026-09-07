package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Writer appends records to a rotating JSONL file.
//
// Unlike the difflog's sampler this keeps EVERY record. A capture is only worth
// having if it is complete: the comparison asks "did Synapse send something we
// did not", and a sampled file cannot answer it -- an absence would be
// indistinguishable from a record that was simply not written.
//
// That makes the size bound matter. A capture points at ONE low-volume test
// destination, so the volume is small by construction, but the bound is here so
// a misconfiguration that captures a busy destination fills a file rather than
// a disk.
type Writer struct {
	path        string
	maxBytes    int64
	generations int
	// destinations limits what is captured. Empty means capture everything,
	// which is only sensible for the proxy in front of a test server.
	destinations map[string]bool

	mu      sync.Mutex
	f       *os.File
	written int64
	dropped int64
	count   int64
}

// Config builds a Writer.
type Config struct {
	// Path is the JSONL file. Rotated files get .1, .2 ... suffixes.
	Path string
	// MaxBytes rotates the file when it would grow past this. Zero takes a
	// default.
	MaxBytes int64
	// Generations is how many rotated files to keep. Zero takes a default.
	Generations int
	// Destinations limits capture to these servers. Empty captures all.
	Destinations []string
}

// Open prepares the capture file.
func Open(cfg Config) (*Writer, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("capture: no path configured")
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 256 << 20
	}
	if cfg.Generations <= 0 {
		cfg.Generations = 3
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("capture: %w", err)
	}

	w := &Writer{
		path: cfg.Path, maxBytes: cfg.MaxBytes, generations: cfg.Generations,
	}
	if len(cfg.Destinations) > 0 {
		w.destinations = make(map[string]bool, len(cfg.Destinations))
		for _, d := range cfg.Destinations {
			w.destinations[d] = true
		}
	}
	if err := w.reopen(); err != nil {
		return nil, err
	}
	return w, nil
}

// Captures reports whether a destination is being recorded.
//
// Exported so a caller can skip the work of building a record it would then
// discard -- which for the worker means not serialising a transaction body it
// has no use for.
func (w *Writer) Captures(destination string) bool {
	return w.destinations == nil || w.destinations[destination]
}

func (w *Writer) reopen() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("capture: opening %s: %w", w.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("capture: %w", err)
	}
	w.f, w.written = f, info.Size()
	return nil
}

// Write appends a record.
func (w *Writer) Write(r Record) error {
	if !w.Captures(r.Destination) {
		return nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	b = append(b, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		w.dropped++
		return nil
	}
	if w.written+int64(len(b)) > w.maxBytes {
		if err := w.rotateLocked(); err != nil {
			w.dropped++
			return err
		}
	}
	n, err := w.f.Write(b)
	w.written += int64(n)
	if err != nil {
		w.dropped++
		return fmt.Errorf("capture: %w", err)
	}
	w.count++
	return nil
}

// rotateLocked shifts the generations along, oldest discarded.
func (w *Writer) rotateLocked() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	// The oldest generation falls off the end.
	_ = os.Remove(fmt.Sprintf("%s.%d", w.path, w.generations))
	// Then shift the rest along, oldest FIRST, so no file is overwritten
	// before it has itself been moved.
	for i := w.generations - 1; i >= 1; i-- {
		_ = os.Rename(
			fmt.Sprintf("%s.%d", w.path, i),
			fmt.Sprintf("%s.%d", w.path, i+1))
	}
	if err := os.Rename(w.path, w.path+".1"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("capture: rotating: %w", err)
	}
	return w.reopen()
}

// Stats reports what has been written, for logging and metrics.
func (w *Writer) Stats() (written, dropped int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count, w.dropped
}

// Close releases the file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
