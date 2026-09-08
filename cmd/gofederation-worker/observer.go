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

// OnStage records how long one stage of the pipeline took.
//
// Separate from the end-to-end duration because the question this worker exists
// to answer is not only "is it faster?" but "where did the time go?" -- a
// goroutine fan-out that merely moves a bottleneck from one stage to another is
// not an improvement, and one aggregate number cannot tell the two apart.
func (o *observer) OnStage(stage string, took time.Duration) {
	metrics.StageDuration.WithLabelValues(stage).Observe(took.Seconds())
	if stage == "fanout" {
		metrics.FanOutDuration.Observe(took.Seconds())
	}
}

// OnEventLag records how old an event was when routing finished.
//
// The headline number, and the one that is directly comparable with Synapse's
// synapse_event_processing_lag. A zero received_ts means Synapse never recorded
// one, which is "cannot measure" rather than "arrived at the epoch" -- recording
// it would put a decades-long lag in the histogram and wreck every quantile.
func (o *observer) OnEventLag(receivedTS int64, at time.Time) {
	if receivedTS <= 0 {
		return
	}
	lag := at.Sub(time.UnixMilli(receivedTS))
	if lag < 0 {
		// Clock skew between the database and this process. Discarded rather
		// than clamped to zero, which would quietly bias the low quantiles.
		return
	}
	metrics.EventProcessingLag.Observe(lag.Seconds())
}

// countEDUTypes records a transaction's EDU breakdown.
//
// Synapse counts its own EDUs by type, and without the same breakdown the two
// are not comparable in the way that matters: presence was 99.6% of what the
// Python senders on this homeserver sent, so a difference in the TOTAL says
// almost nothing about whether any particular EDU is being routed at all.
func countEDUTypes(byType map[string]int) {
	for t, n := range byType {
		metrics.TransactionEDUsByType.WithLabelValues(t).Add(float64(n))
	}
}
