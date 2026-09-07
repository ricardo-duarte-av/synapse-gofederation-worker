package difflog

import "github.com/prometheus/client_golang/prometheus"

// Collector exposes the persisted totals to Prometheus.
//
// The values are read from the Writer on each scrape rather than mirrored into
// separate counters, so Prometheus and stats.json can never disagree. Because
// the underlying totals are restored from disk at startup, these survive
// restarts -- which is the point: a promotion gate measured in weeks cannot be
// judged from counters that reset on every deploy.
type Collector struct {
	w *Writer

	since       *prometheus.Desc
	restarts    *prometheus.Desc
	examined    *prometheus.Desc
	routed      *prometheus.Desc
	skipped     *prometheus.Desc
	resolved    *prometheus.Desc
	ours        *prometheus.Desc
	approximate *prometheus.Desc
	rescinded   *prometheus.Desc
	txns        *prometheus.Desc
	pdus        *prometheus.Desc
	edus        *prometheus.Desc
}

// NewCollector builds a Collector reading from w.
func NewCollector(w *Writer) *Collector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc("gofed_shadow_"+name, help, labels, nil)
	}
	return &Collector{
		w:           w,
		since:       d("since_seconds", "Unix time at which the persisted totals started accumulating."),
		restarts:    d("restarts_total", "Process starts since the totals began."),
		examined:    d("events_examined_total", "Events examined, across restarts."),
		routed:      d("events_routed_total", "Events routed to at least one destination in our shard."),
		skipped:     d("events_skipped_total", "Events not federated, by reason.", "reason"),
		resolved:    d("destinations_resolved_total", "Destinations resolved across all events."),
		ours:        d("destinations_ours_total", "Resolved destinations belonging to our shard."),
		approximate: d("approximate_routes_total", "Events routed from current state rather than state before the event."),
		rescinded:   d("rescinded_invites_total", "Events where the rescinded-invite rule added a destination."),
		txns:        d("transactions_total", "Transactions assembled."),
		pdus:        d("pdus_total", "PDUs placed in transactions."),
		edus:        d("edus_total", "EDUs placed in transactions."),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.since, c.restarts, c.examined, c.routed, c.skipped,
		c.resolved, c.ours, c.approximate, c.rescinded, c.txns, c.pdus, c.edus,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	t := c.w.Totals()
	counter := func(d *prometheus.Desc, v int64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}
	ch <- prometheus.MustNewConstMetric(c.since, prometheus.GaugeValue, float64(t.Since.Unix()))
	counter(c.restarts, t.Restarts)
	counter(c.examined, t.EventsExamined)
	counter(c.routed, t.EventsRouted)
	for reason, n := range t.EventsSkipped {
		counter(c.skipped, n, reason)
	}
	counter(c.resolved, t.DestinationsResolved)
	counter(c.ours, t.DestinationsOurs)
	counter(c.approximate, t.ApproximateRoutes)
	counter(c.rescinded, t.RescindedInvites)
	counter(c.txns, t.Transactions)
	counter(c.pdus, t.PDUs)
	counter(c.edus, t.EDUs)
}
