// Package sink is where a signed transaction goes.
//
// There are two implementations and the difference between them is the
// difference between shadowing and sending. DryRun logs and counts; HTTP would
// open a connection to a remote homeserver. The split is an interface rather
// than an `if shadow {}` inside one sender on purpose: in dry-run the HTTP
// implementation is never constructed, so there is no client in the process
// that a bug could reach. See docs/shadow-safety.md.
package sink

import (
	"context"
	"sync/atomic"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Result is what a destination learns from an attempt.
type Result struct {
	// Delivered is true when the remote accepted the transaction. In dry-run
	// it is always true: the queue must behave as if the send worked, or the
	// shadow would diverge from the real sender for a reason that has nothing
	// to do with either one's logic.
	Delivered bool
	// StatusCode is the HTTP status, or 0 in dry-run.
	StatusCode int
	// PDUErrors maps event id to the error a remote reported for it. Synapse
	// logs these and never retries them (transaction_manager.py:197).
	PDUErrors map[string]string
}

// Capturer records complete transactions for later comparison against what
// Synapse actually sent. See internal/capture.
type Capturer interface {
	// Captures reports whether a destination is recorded, so the caller can
	// skip building a record it would discard.
	Captures(destination string) bool
	// Capture records one request.
	Capture(req *txn.Request) error
}

// Sink accepts signed transactions.
type Sink interface {
	// Send delivers a transaction, or pretends to.
	Send(ctx context.Context, req *txn.Request) (Result, error)
	// Mode names the implementation for startup logging, so which one is
	// running is never in doubt.
	Mode() string
}

// DryRun logs transactions instead of sending them.
type DryRun struct {
	log zerolog.Logger
	// onSent reports each transaction to whoever is counting. A hook rather
	// than the sink importing the metrics package, so this package stays
	// testable without a Prometheus registry -- and so the same numbers reach
	// both the metrics and the persisted shadow record from one place.
	onSent func(pdus, edus, bytes int)
	// capture records the full request for named destinations. Separate from
	// onSent because it is about bytes rather than counts, and because it is
	// off for every destination but the handful being compared.
	capture Capturer

	transactions atomic.Int64
	pdus         atomic.Int64
	edus         atomic.Int64
	bytes        atomic.Int64
}

// NewDryRun builds a dry-run sink.
func NewDryRun(log zerolog.Logger) *DryRun {
	return &DryRun{log: log}
}

// SetOnSent registers a callback invoked for every transaction.
func (d *DryRun) SetOnSent(f func(pdus, edus, bytes int)) { d.onSent = f }

// SetCapture registers a full-request recorder.
func (d *DryRun) SetCapture(c Capturer) { d.capture = c }

// Mode identifies this sink.
func (d *DryRun) Mode() string { return "dry-run (shadow; nothing is sent)" }

// Send records the transaction and reports success.
//
// Reporting success is the right answer, not a convenient one. The queue
// advances its cursors on a delivered transaction, and a dry-run that reported
// failure would make the shadow retry forever and diverge from the real sender
// for a reason unrelated to either one's decisions -- which is precisely what
// the comparison is supposed to isolate.
func (d *DryRun) Send(_ context.Context, req *txn.Request) (Result, error) {
	pdus, edus := countUnits(req.Body)
	d.transactions.Add(1)
	d.pdus.Add(int64(pdus))
	d.edus.Add(int64(edus))
	d.bytes.Add(int64(len(req.Body)))

	if d.onSent != nil {
		d.onSent(pdus, edus, len(req.Body))
	}
	// Recorded before the log line, and never allowed to fail the send: in
	// shadow mode there is nothing to fail, but the same code runs when this
	// worker sends for real and a capture error must not become a delivery
	// error.
	if d.capture != nil && d.capture.Captures(req.Destination) {
		if err := d.capture.Capture(req); err != nil {
			d.log.Error().Err(err).
				Str("destination", req.Destination).
				Msg("failed to record transaction for comparison")
		}
	}

	d.log.Debug().
		Str("destination", req.Destination).
		Str("txn_id", req.TransactionID).
		Int("pdus", pdus).
		Int("edus", edus).
		Int("bytes", len(req.Body)).
		Msg("would send transaction")

	return Result{Delivered: true}, nil
}

// Stats are the running totals, for metrics and the difflog.
type Stats struct {
	Transactions int64
	PDUs         int64
	EDUs         int64
	Bytes        int64
}

// Stats returns what this sink has seen.
func (d *DryRun) Stats() Stats {
	return Stats{
		Transactions: d.transactions.Load(),
		PDUs:         d.pdus.Load(),
		EDUs:         d.edus.Load(),
		Bytes:        d.bytes.Load(),
	}
}
