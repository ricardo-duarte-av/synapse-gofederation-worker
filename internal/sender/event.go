package sender

import (
	"context"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
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

	res, err := s.cfg.Resolver.Resolve(ctx, e.JSON, authEvents)
	if err != nil {
		return false, err
	}
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
	timings, err := s.cfg.Store.GetDestinationRetryTimings(ctx, ours)
	if err != nil {
		return false, err
	}
	due := store.FilterDestinationsByRetryLimiter(ours, timings, time.Now(), catchupRetryInterval)

	p := queue.PDU{EventID: e.EventID, StreamOrdering: e.StreamOrdering, JSON: e.JSON}
	for _, d := range due {
		q := s.cfg.Queues.Get(d)
		q.EnqueuePDU(p)
		s.cfg.Queues.Wake(ctx, q)
	}
	return len(due) > 0, nil
}
