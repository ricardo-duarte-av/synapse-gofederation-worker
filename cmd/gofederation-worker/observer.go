package main

import (
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/difflog"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
)

// observer feeds the metrics and the persisted shadow record.
//
// The pipeline reports its decisions through this rather than incrementing
// counters itself, so that what is measured can change without touching the
// code being measured -- and so the pipeline has no opinion about whether it is
// shadowing.
type observer struct {
	// diff is nil when not shadowing; the metrics are always kept.
	diff *difflog.Writer
}

func (o *observer) OnEventSkipped(_ string, reason destinations.SkipReason) {
	metrics.EventsProcessed.Inc()
	metrics.EventsSkipped.WithLabelValues(string(reason)).Inc()
	if o.diff != nil {
		o.diff.RecordSkip(string(reason))
	}
}

func (o *observer) OnEventRouted(eventID string, all, ours []string, fallback destinations.FallbackReason) {
	metrics.EventsProcessed.Inc()
	if len(ours) > 0 {
		metrics.EventsRouted.Inc()
		metrics.DestinationsPerEvent.Observe(float64(len(ours)))
	}
	// Labelled by cause, because the two fallbacks mean different things: a
	// forked DAG is work we have not done, while a missing state group is an
	// outlier we could not have resolved anyway.
	approximate := fallback != ""
	if approximate {
		metrics.ApproximateRoutes.WithLabelValues(string(fallback)).Inc()
	}
	if o.diff != nil {
		o.diff.RecordRoute(eventID, all, ours, approximate, string(fallback))
	}
}

func (o *observer) OnBatch(_, _ int, _, _ int64, took time.Duration) {
	metrics.BatchDuration.Observe(took.Seconds())
}

// countTransaction feeds the metrics and the shadow record for every assembled
// transaction, whether it was really sent or only logged.
//
// Shared by both sinks so the numbers mean the same thing in either mode: a
// count that changed meaning when the worker went live would make every
// dashboard built on it wrong at exactly the moment it mattered.
func (w *worker) countTransaction(pdus, edus, bytes int) {
	metrics.Transactions.Inc()
	metrics.TransactionPDUs.Add(float64(pdus))
	metrics.TransactionEDUs.Add(float64(edus))
	metrics.TransactionBytes.Add(float64(bytes))
	if w.diff != nil {
		w.diff.RecordTransaction(pdus, edus)
	}
}
