package sender

import (
	"context"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// catchupRetryInterval is Synapse's CATCHUP_RETRY_INTERVAL, one hour
// (per_destination_queue.py:75).
//
// It is the slack given to the retry filter, so a destination about to come
// back is included in this batch rather than deferred to the next poll. Without
// it a recovering server waits an extra full interval for no reason.
const catchupRetryInterval = time.Hour

// handleEvent decides where one event goes and queues it. It reports whether
// the event was routed anywhere.
func (s *Sender) handleEvent(ctx context.Context, e store.Event) (bool, error) {
	started := time.Now()
	meta := destinations.ParseMetadata(e.InternalMetadata)

	if ok, reason := destinations.Eligible(e.Sender, s.cfg.ServerName, meta, e.RejectionReason); !ok {
		if s.cfg.Observer != nil {
			s.cfg.Observer.OnEventSkipped(e.EventID, reason)
		}
		return false, nil
	}

	// The auth events are loaded lazily: only a membership leave whose sender
	// is not its subject needs them, which is a small fraction of a small
	// fraction of traffic.
	authEvents := func() ([][]byte, error) {
		return s.loadAuthEvents(ctx, e)
	}

	resolveStart := time.Now()
	res, err := s.cfg.Resolver.Resolve(ctx, e.JSON, authEvents)
	if err != nil {
		return false, err
	}
	s.stage("resolve", resolveStart)
	if len(res.Destinations) == 0 {
		if s.cfg.Observer != nil {
			s.cfg.Observer.OnEventSkipped(e.EventID, destinations.SkipNoDestinations)
		}
		return false, nil
	}

	// The shard filter. Everything before this is work both senders do; this is
	// what makes the event ours or theirs.
	ours := make([]string, 0, len(res.Destinations))
	for _, d := range res.Destinations {
		if !s.cfg.ShouldHandle(d) {
			continue
		}
		// Synapse discards the server we are forwarding on behalf of: it
		// already has the event, which is why it asked us to send it
		// (federation/sender/__init__.py:709).
		if d == meta.SendOnBehalfOf {
			continue
		}
		ours = append(ours, d)
	}

	if s.cfg.Observer != nil {
		s.cfg.Observer.OnEventRouted(e.EventID, res.Destinations, ours, res.Fallback)
	}
	if len(ours) == 0 {
		return false, nil
	}

	// The retry filter comes last, after the shard filter, so we only ask the
	// database about destinations that are actually ours.
	//
	// Synapse writes destination_rooms BEFORE this filter, so a server that is
	// down still has the event recorded as owed to it. We cannot write that
	// table, which means our catch-up is driven by our own cursors instead --
	// see docs/shadow-safety.md and internal/state.
	retryStart := time.Now()
	timings, err := s.cfg.Store.GetDestinationRetryTimings(ctx, ours)
	if err != nil {
		return false, err
	}
	s.stage("retry_filter", retryStart)
	due := store.FilterDestinationsByRetryLimiter(ours, timings, time.Now(), catchupRetryInterval)

	// Serialise as Synapse does before queueing, so a PDU it would drop is
	// never queued anywhere. The depth filter removes the event entirely
	// rather than trimming it: one unencodable PDU makes the whole transaction
	// unparseable, taking every other PDU in it down too.
	body, ok := txn.SerialisePDU(e.JSON)
	if !ok {
		if s.cfg.Observer != nil {
			s.cfg.Observer.OnEventSkipped(e.EventID, destinations.SkipUnserialisable)
		}
		return false, nil
	}

	// Record the decision BEFORE the retry filter and before queueing, exactly
	// where Synapse writes destination_rooms (federation/sender/__init__.py:828).
	// Recording after the retry filter would make us disagree with Synapse for
	// every destination that happened to be backing off -- a difference in
	// bookkeeping that would look like a difference in routing.
	routes := make([]state.RoutedRoom, 0, len(ours))
	for _, d := range ours {
		routes = append(routes, state.RoutedRoom{
			Destination: d, RoomID: e.RoomID, StreamOrdering: e.StreamOrdering,
		})
	}
	if err := s.cfg.Cursors.RecordRoutes(ctx, routes); err != nil {
		return false, err
	}
	// In primary mode the same decision goes where Synapse keeps it, because
	// catch-up reads that table and nothing else is writing it.
	if s.cfg.RecordRoutes != nil {
		if err := s.cfg.RecordRoutes(ctx, ours, e.RoomID, e.StreamOrdering); err != nil {
			return false, err
		}
	}

	// The fan-out itself, timed separately: it is the thing this worker's
	// design is a bet on, so "did it help?" has to be answerable directly
	// rather than inferred from end-to-end latency.
	fanOut := time.Now()
	p := queue.PDU{EventID: e.EventID, StreamOrdering: e.StreamOrdering, JSON: body}
	for _, d := range due {
		q := s.cfg.Queues.Get(d)
		q.EnqueuePDU(p)
		s.cfg.Queues.Wake(q)
	}
	if s.cfg.Observer != nil {
		s.cfg.Observer.OnStage("fanout", time.Since(fanOut))
		s.cfg.Observer.OnStage("event_total", time.Since(started))
		s.cfg.Observer.OnEventLag(e.ReceivedTS, time.Now())
	}
	return len(due) > 0, nil
}

// stage reports a stage's duration, if anyone is listening.
func (s *Sender) stage(name string, since time.Time) {
	if s.cfg.Observer != nil {
		s.cfg.Observer.OnStage(name, time.Since(since))
	}
}
