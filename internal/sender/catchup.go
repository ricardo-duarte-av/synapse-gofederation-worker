package sender

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Synapse's catch-up constants (federation/sender/__init__.py:222,227 and
// per_destination_queue.py:497).
const (
	// catchUpPageSize is how many owed rooms one pass reads. Synapse's 50.
	catchUpPageSize = 50
	// wakeupInterval is how often the sweeper looks for destinations that are
	// behind. Synapse's WAKEUP_RETRY_PERIOD.
	wakeupInterval = time.Minute
	// betweenDestinations paces the sweep. Waking thousands of stale
	// destinations at once would put the entire graveyard on the wire
	// simultaneously, which is the stampede the backoff exists to prevent.
	betweenDestinations = 5 * time.Second
	// destinationPage is how many destinations one sweep query returns.
	destinationPage = 25
	// catchUpConcurrency bounds how many destinations are being brought
	// forward at once.
	//
	// The sweep DISPATCHES rather than performs, which is the shape Synapse
	// has: wake_destination starts a background process and returns, so the
	// waker never waits on a destination (federation/sender/__init__.py:1164).
	// Running catch-up inline instead means one destination with a large
	// backlog -- or one that hangs until client_timeout, which on this
	// deployment is 180s with twenty retries behind it -- stalls every other
	// destination behind it for as long as it takes.
	catchUpConcurrency = 32
)

// CatchUpStore is the database access catch-up needs.
type CatchUpStore interface {
	GetDestinationLastSuccessfulStreamOrdering(ctx context.Context, destination string) (int64, bool, error)
	GetCatchUpRoomEventIDs(ctx context.Context, destination string, lastSuccessful int64, limit int) ([]string, error)
	GetCatchUpOutstandingDestinations(ctx context.Context, after string, nowMS int64, limit int) ([]string, error)
	GetEvents(ctx context.Context, ids []string) ([]store.Event, error)
}

// CatchUp replays what a destination missed while this sender could not reach
// it.
//
// It is the only part of the sender that reads from durable state rather than
// from the live stream, and that is the point: the live stream only ever
// carries what is happening NOW. Everything a destination missed while it was
// down exists solely as rows in destination_rooms, and without something that
// goes looking for them they are never sent -- the events are not lost, they
// are simply never delivered, which from the far end is the same thing.
type CatchUp struct {
	store  CatchUpStore
	queues *queue.Manager
	log    zerolog.Logger

	shouldHandle func(destination string) bool
	due          func(destination string) bool
}

// CatchUpConfig builds a CatchUp.
type CatchUpConfig struct {
	Store        CatchUpStore
	Queues       *queue.Manager
	Log          zerolog.Logger
	ShouldHandle func(destination string) bool
	Due          func(destination string) bool
}

// NewCatchUp builds the catch-up worker.
func NewCatchUp(cfg CatchUpConfig) *CatchUp {
	return &CatchUp{
		store: cfg.Store, queues: cfg.Queues, log: cfg.Log,
		shouldHandle: cfg.ShouldHandle, due: cfg.Due,
	}
}

// Run sweeps for destinations that are behind, until the context ends.
func (c *CatchUp) Run(ctx context.Context) error {
	// Swept once at startup rather than only on the tick, because the moment a
	// sender comes back is exactly when the most destinations are behind.
	c.sweep(ctx)

	ticker := time.NewTicker(wakeupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.sweep(ctx)
		}
	}
}

// sweep walks every destination that is owed something and starts it catching
// up.
//
// It dispatches and does not wait. A sweep that performed each catch-up in turn
// would be as slow as its slowest destination, and on a list where half the
// entries are servers that no longer exist that is not a tail case.
func (c *CatchUp) sweep(ctx context.Context) {
	var wg sync.WaitGroup
	slots := make(chan struct{}, catchUpConcurrency)
	defer wg.Wait()

	after := ""
	swept := 0
	for ctx.Err() == nil {
		destinations, err := c.store.GetCatchUpOutstandingDestinations(
			ctx, after, time.Now().UnixMilli(), destinationPage)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to list destinations needing catch-up")
			return
		}
		if len(destinations) == 0 {
			break
		}
		after = destinations[len(destinations)-1]

		for _, d := range destinations {
			if !c.shouldHandle(d) {
				continue
			}
			if c.due != nil && !c.due(d) {
				continue
			}
			// Bounded, so a sweep of thousands cannot open thousands of
			// connections; and acquired before the goroutine so the pacing
			// below still applies when every slot is busy.
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func(destination string) {
				defer wg.Done()
				defer func() { <-slots }()
				if err := c.Destination(ctx, destination); err != nil {
					c.log.Warn().Err(err).Str("destination", destination).
						Msg("catch-up failed")
				}
			}(d)
			swept++

			// Paced deliberately, on DISPATCH. Without it a restart puts every
			// stale destination on the wire in the same instant.
			select {
			case <-ctx.Done():
				return
			case <-time.After(betweenDestinations):
			}
		}
	}
	if swept > 0 {
		c.log.Info().Int("destinations", swept).Msg("catch-up sweep finished")
	}
}

// Destination brings one destination forward.
//
// This is Synapse's _catch_up_transmission_loop (per_destination_queue.py:473)
// with one deliberate omission, noted below.
func (c *CatchUp) Destination(ctx context.Context, destination string) error {
	// Self-guarding, so the exported method is safe to call from anywhere.
	// The sweep already filters on this, but a catch-up that dialled a
	// destination the backoff had ruled out would defeat the backoff for
	// exactly the destinations it exists to protect -- the ones that owe the
	// most because they have been unreachable the longest.
	if c.due != nil && !c.due(destination) {
		return nil
	}

	d := c.queues.Get(destination)

	// The cursor to catch up FROM. A destination with no recorded value has
	// never had a successful transaction, so there is nothing to replay and
	// catch-up ends immediately rather than resending the room's history.
	lastSuccessful, known, err := c.store.GetDestinationLastSuccessfulStreamOrdering(ctx, destination)
	if err != nil {
		return err
	}
	if !known {
		d.FinishCatchUp()
		return nil
	}
	d.SetLastSuccessfulStreamOrdering(lastSuccessful)

	for ctx.Err() == nil {
		// Re-checked between pages. A destination can fall into backoff while
		// its own backlog is being replayed -- a failed send does exactly that
		// -- and a long backlog would otherwise keep dialling it for as long as
		// the pages last.
		if c.due != nil && !c.due(destination) {
			return nil
		}

		eventIDs, err := c.store.GetCatchUpRoomEventIDs(ctx, destination, lastSuccessful, catchUpPageSize)
		if err != nil {
			return err
		}
		if len(eventIDs) == 0 {
			// Nothing owed. Before declaring the destination up to date, check
			// whether an event arrived and was dropped between the last page
			// and now -- otherwise it belongs to neither path and is never
			// sent (per_destination_queue.py:508).
			if skipped := d.TakeCatchUpSkipped(); skipped > lastSuccessful {
				continue
			}
			d.FinishCatchUp()
			c.log.Debug().Str("destination", destination).
				Int64("up_to", lastSuccessful).Msg("caught up")
			return nil
		}

		events, err := c.store.GetEvents(ctx, eventIDs)
		if err != nil {
			return err
		}
		for _, e := range events {
			// Synapse substitutes the room's CURRENT forward extremities when
			// the owed event is no longer one, so the destination arrives at
			// the present in a single step instead of backfilling to it.
			//
			// Not done here on purpose: those extremities were never routed to
			// this destination, so sending them would need
			// filter_events_for_server -- history visibility and erased
			// senders -- and skipping that filter could disclose an event the
			// destination was not entitled to. The owed event WAS routed to it,
			// so sending exactly that is safe, and the far side backfills.
			body, ok := txn.SerialisePDU(e.JSON)
			if !ok {
				// Unencodable, so it could never be delivered. Step over it
				// rather than stalling the whole destination on one bad event.
				lastSuccessful = e.StreamOrdering
				continue
			}
			if err := d.SendCatchUp(ctx, queue.PDU{
				EventID: e.EventID, StreamOrdering: e.StreamOrdering, JSON: body,
			}); err != nil {
				return err
			}
			// Advanced to the ORIGINAL event's ordering: this is a cursor into
			// destination_rooms, not a record of what was delivered.
			lastSuccessful = e.StreamOrdering
		}
	}
	return ctx.Err()
}
