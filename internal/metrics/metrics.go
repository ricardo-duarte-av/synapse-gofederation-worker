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

	// StateGroupCacheEntries is how full the joined-host cache is.
	//
	// The question "should the cache be bigger?" is only answerable with this:
	// while it sits below resolution.host_cache_entries the cache has never
	// evicted anything, every miss is a first sight of a state group, and
	// raising the limit cannot change the hit rate by a single lookup.
	StateGroupCacheEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_state_group_cache_entries",
		Help: "State groups currently held in the joined-host cache.",
	})

	// StateGroupCache counts joined-host lookups by result.
	//
	// The hit rate is the whole value of the cache: state groups are immutable,
	// so a miss is a recursive walk of the state group edges -- measured in the
	// hundreds of milliseconds on this deployment, and seconds at worst -- while
	// a hit is a map read. A falling hit rate means rooms are changing
	// membership faster than the cache holds them, or that it is too small.
	StateGroupCache = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_state_group_cache_total",
		Help: "Joined-host lookups by state group, by cache result.",
	}, []string{"result"})

	// EDUsDropped counts ephemeral EDUs abandoned because their destination is
	// in a long outage.
	//
	// Worth a counter of its own because the alternative is invisible: a queue
	// that stops growing looks identical to a queue that is draining, and this
	// is the only place read receipts are knowingly thrown away.
	EDUsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_edus_dropped_total",
		Help: "Ephemeral EDUs dropped because the destination is in a long outage.",
	}, []string{"destination"})

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
	// than the state before the event, by why.
	//
	// The most important number in the shadow, because it is the one known
	// difference from Synapse's algorithm rather than a bug we are hoping is
	// not there. Labelled by cause: a forked DAG is work we have not done, an
	// outlier prev event is one we could not have resolved anyway. See
	// internal/destinations.
	ApproximateRoutes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_approximate_routes_total",
		Help: "Events routed from current state rather than state before the event, by cause.",
	}, []string{"cause"})

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

	// PresenceStatesSent counts individual user presence states put on the wire.
	//
	// Distinct from the EDU count, and the distinction is the only way to tell
	// two very different things apart. One m.presence EDU carries up to 50
	// states, so a sender that coalesces aggressively delivers the same
	// information in far fewer EDUs than one that does not -- which looks
	// identical, from the EDU counter alone, to a sender that is silently
	// dropping presence.
	//
	// The Python senders this worker replaced sent 15.257 presence EDUs per
	// second against 15.35 transactions per second: one destination, one
	// transaction, one EDU, essentially never batched. This worker sends 3.5
	// EDUs per second from a HIGHER input rate, and whether that is coalescing
	// or loss cannot be read from either counter alone.
	PresenceStatesSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gofed_presence_states_sent_total",
		Help: "User presence states placed in transactions, across all m.presence EDUs.",
	})

	// TransactionEDUsByType counts EDUs placed in transactions, by edu_type.
	//
	// The counterpart to Synapse's
	// synapse_federation_client_sent_edus_by_type_total, and it exists because
	// the unlabelled total could not answer the first real question asked of
	// it. Comparing this worker against the Python senders it replaced, the
	// EDU rate was 4.4x lower -- and with one number there was no way to tell
	// whether that was less presence, which is a tuning difference, or no
	// typing at all, which is a feature silently missing. Synapse's own
	// breakdown showed presence to be 99.6% of its traffic; ours had no
	// breakdown to show.
	TransactionEDUsByType = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_transaction_edus_by_type_total",
		Help: "EDUs placed in transactions, by EDU type.",
	}, []string{"type"})

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
		ReplicationLive, ReplicationRows, StreamPosition, EDUsDropped, StateGroupCache, StateGroupCacheEntries,
		EventsProcessed, EventsSkipped, EventsRouted, DestinationsPerEvent,
		ApproximateRoutes, BatchDuration,
		QueuedDestinations, QueuedPDUs, QueuedEDUs, KnownDestinations,
		Transactions, TransactionPDUs, TransactionEDUs, TransactionEDUsByType,
		PresenceStatesSent, TransactionBytes,
		EventProcessingLag, StageDuration, FanOutDuration, SendDuration,
		InFlightSends, DestinationGoroutines, WriteOps, DestinationsBackingOff,
	}
}

// MustRegister registers every collector with the default registry.
func MustRegister() {
	prometheus.MustRegister(All()...)
}

// Performance metrics.
//
// The reason this worker exists is that Synapse fans one event out to a
// thousand destinations on a single reactor. Claiming an improvement needs
// numbers that are comparable with Synapse's own, so these deliberately mirror
// the shape of the metrics Synapse exposes rather than inventing a private
// vocabulary:
//
//	gofed_event_processing_lag_seconds  <-> synapse_event_processing_lag
//	gofed_events_processed_total        <-> synapse_federation_client_sent_pdu_destinations
//
// Read them alongside the Python senders' on the same dashboard; a number with
// no counterpart cannot answer "is this faster?".
var (
	// EventProcessingLag is the age of an event when we finish routing it:
	// now minus its received_ts.
	//
	// The headline number. It is what a user would feel as "my message took a
	// while to reach the other server", and it is directly comparable with
	// Synapse's synapse_event_processing_lag for the same stream.
	//
	// Only meaningful while FOLLOWING the stream. During a replay -- a restart
	// with a backlog, or a cursor rewound by hand -- every event is hours old
	// by construction and the histogram says nothing about how fast the worker
	// is. Synapse's own metric has the same property. Read it alongside
	// gofed_stage_duration_seconds, which is unaffected.
	EventProcessingLag = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "gofed_event_processing_lag_seconds",
		Help: "Age of an event when routing finished: now minus received_ts.",
		// Bucketed for a healthy server in the tens of milliseconds and a
		// struggling one in the tens of seconds, since both ends matter.
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})

	// StageDuration times each stage of the pipeline separately.
	//
	// One number for "how long did it take" cannot say WHERE the time went,
	// and the whole question here is whether the goroutine fan-out actually
	// moves the bottleneck. Labelled by stage: pickup, resolve, shard, queue,
	// build, send.
	StageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gofed_stage_duration_seconds",
		Help: "Time spent in each stage of the pipeline.",
		// The top of this range is not padding. The stage that dominates is
		// destination resolution, a recursive walk of the state group edges,
		// and it was measured directly against this deployment's database at
		// 10.8ms, 108ms, 375ms and 6.3 SECONDS for the deepest chain. A set
		// ending at 5s put the worst case in +Inf, where it has no value at
		// all, and jumped .5 -> 1 -> 5, so a quantile landing up there was
		// interpolated across a 4-second-wide bucket and reported with a
		// precision it did not have.
		Buckets: []float64{
			.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5,
			1, 2.5, 5, 10, 30,
		},
	}, []string{"stage"})

	// FanOutDuration is how long one event takes to reach every one of its
	// destinations' queues.
	//
	// The number the design is a bet on: Synapse does this on one reactor and
	// we do it across goroutines, so this is where an improvement should show
	// up if there is one.
	FanOutDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gofed_fanout_duration_seconds",
		Help:    "Time to fan one event out to every destination queue in this shard.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5},
	})

	// SendDuration is one HTTP transaction, labelled by outcome so a fast
	// failure is not mistaken for a fast success.
	SendDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gofed_send_duration_seconds",
		Help:    "Duration of one outbound transaction, by outcome.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"outcome"})

	// InFlightSends is how many transactions are on the wire right now,
	// against the concurrency bound.
	InFlightSends = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_in_flight_sends",
		Help: "Transactions currently being sent.",
	})

	// Goroutines is our own count rather than the Go runtime's total, so the
	// fan-out can be seen without the runtime's own noise.
	DestinationGoroutines = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_destination_goroutines",
		Help: "Destination transmission loops currently running.",
	})

	// DestinationsBackingOff is how many destinations are currently refusing to
	// be dialled.
	//
	// A homeserver's destination list is mostly a graveyard, so this number is
	// large and that is normal. What matters is its SHAPE: a step change means
	// a network problem at our end rather than at theirs.
	DestinationsBackingOff = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gofed_destinations_backing_off",
		Help: "Destinations currently in a retry backoff.",
	})

	// WriteOps counts the bookkeeping a primary sender performs, by operation.
	// Zero in shadow mode, and that is worth being able to see.
	WriteOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gofed_write_operations_total",
		Help: "Writes to Synapse's tables, by operation. Always zero in shadow mode.",
	}, []string{"operation", "outcome"})
)
