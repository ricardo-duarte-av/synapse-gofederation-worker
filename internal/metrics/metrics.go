// Package metrics holds the Prometheus collectors.
//
// The names are prefixed gofed_ and deliberately mirror the Synapse metrics
// they should be compared against: while shadowing, the question being asked is
// always "does this number match the real sender's?", and a metric with no
// counterpart cannot answer it.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// BuildInfo carries the version as labels on a constant 1, the usual way
	// to make a build queryable.
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofed_build_info",
		Help: "Build information for the running worker.",
	}, []string{"tag", "commit", "build_time"})

	// ShadowMode is 1 in dry-run and 0 when really sending. Worth a metric of
	// its own: it is the single most important fact about a running instance,
	// and "is this thing actually sending?" should be answerable from a
	// dashboard rather than by reading a config file on a host.
	ShadowMode = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_shadow_mode",
		Help: "1 when the worker is shadowing (nothing is sent), 0 when sending for real.",
	})

	// DatabaseReadOnly is 1 when the Synapse connection cannot write.
	DatabaseReadOnly = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_database_read_only",
		Help: "1 when the Synapse database role is restricted to reads.",
	})

	// ReplicationLive is 1 while the Redis subscription is healthy. Positions
	// are only trustworthy while it is.
	ReplicationLive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_replication_live",
		Help: "1 while the replication subscription is connected.",
	})

	// ReplicationRows counts rows by stream, which is how you find out that a
	// stream you thought you handled is not arriving.
	ReplicationRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_replication_rows_total",
		Help: "Replication rows received, by stream.",
	}, []string{"stream"})

	// StreamPosition mirrors Synapse's event_processing_positions. Compared
	// against the shadowed sender's, the gap between them is the lag.
	StreamPosition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gofed_stream_position",
		Help: "The worker's position in each stream.",
	}, []string{"stream"})

	// EventsProcessed and EventsSkipped account for every event the pickup
	// loop saw. Skips are labelled by reason so a filter that has started
	// rejecting everything is visible as a shape change, not just a drop in
	// throughput.
	EventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_events_processed_total",
		Help: "Events examined by the pickup loop.",
	})

	// EventsSkipped counts events that will not be federated, by reason.
	EventsSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_events_skipped_total",
		Help: "Events not federated, by reason.",
	}, []string{"reason"})

	// EventsRouted counts events that produced at least one destination in our
	// shard.
	EventsRouted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_events_routed_total",
		Help: "Events routed to at least one destination in this worker's shard.",
	})

	// DestinationsPerEvent is the fan-out. This is the number that justifies
	// the whole design, so it is a histogram rather than an average.
	DestinationsPerEvent = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gofed_destinations_per_event",
		Help:    "Destinations in this worker's shard per routed event.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
	})

	// ApproximateRoutes counts events routed from CURRENT room state rather
	// than the state before the event.
	//
	// The most important number in the shadow, because it is the one known
	// difference from Synapse's algorithm rather than a bug we are hoping is
	// not there. See internal/destinations.
	ApproximateRoutes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_approximate_routes_total",
		Help: "Events whose destinations came from current state rather than state before the event.",
	})

	// BatchDuration is how long one pickup pass took.
	BatchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gofed_batch_duration_seconds",
		Help:    "Duration of one event pickup pass.",
		Buckets: prometheus.DefBuckets,
	})

	// Queue depth, sampled rather than tracked, so a leak in the queues shows
	// up even if the counters that should have caught it are the buggy part.
	QueuedDestinations = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_queued_destinations",
		Help: "Destinations with something waiting to be sent.",
	})

	// QueuedPDUs is the total number of events waiting across all queues.
	QueuedPDUs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_queued_pdus",
		Help: "PDUs waiting across all destination queues.",
	})

	// QueuedEDUs is the total number of EDUs waiting across all queues.
	QueuedEDUs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_queued_edus",
		Help: "EDUs waiting across all destination queues.",
	})

	// KnownDestinations is how many queues exist, which grows to the number of
	// destinations in our shard and then stops.
	KnownDestinations = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_known_destinations",
		Help: "Destinations with a queue, whether or not anything is waiting.",
	})

	// Transactions and their contents, the direct counterpart to Synapse's
	// federation transaction metrics.
	Transactions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_transactions_total",
		Help: "Transactions assembled (and, outside shadow mode, sent).",
	})

	// TransactionPDUs counts PDUs placed in transactions.
	TransactionPDUs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_transaction_pdus_total",
		Help: "PDUs placed in transactions.",
	})

	// TransactionEDUs counts EDUs placed in transactions.
	TransactionEDUs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_transaction_edus_total",
		Help: "EDUs placed in transactions.",
	})

	// TransactionBytes is the serialised size, useful for sizing the real
	// thing before it is switched on.
	TransactionBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_transaction_bytes_total",
		Help: "Bytes of transaction body assembled.",
	})
)

// All is every collector, for registration.
func All() []prometheus.Collector {
	return []prometheus.Collector{
		BuildInfo, ShadowMode, DatabaseReadOnly,
		ReplicationLive, ReplicationRows, StreamPosition,
		EventsProcessed, EventsSkipped, EventsRouted, DestinationsPerEvent,
		ApproximateRoutes, BatchDuration,
		QueuedDestinations, QueuedPDUs, QueuedEDUs, KnownDestinations,
		Transactions, TransactionPDUs, TransactionEDUs, TransactionBytes,
	}
}

// MustRegister registers every collector with the default registry.
func MustRegister() {
	prometheus.MustRegister(All()...)
}
