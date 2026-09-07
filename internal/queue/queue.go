// Package queue holds the per-destination sending queues.
//
// This is the part the worker exists for. Synapse runs one PerDestinationQueue
// coroutine per destination on a single reactor; here each destination is a
// goroutine, and a thousand of them fanning out from one event is what the Go
// runtime is good at.
//
// The behaviour is Synapse's, though, and two of its rules are load-bearing:
//
//   - Exactly one in-flight transaction per destination, ever. Two would put
//     PDUs on the wire out of order.
//   - Nothing is dequeued until the send has succeeded. Synapse gets this from
//     __aexit__ returning early on an exception (per_destination_queue.py:851);
//     here it is explicit.
package queue

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Limits are the transaction sizes, defaulting to Synapse's.
type Limits struct {
	// MaxPDUs is Synapse's hardcoded 50 (per_destination_queue.py:838).
	MaxPDUs int
	// MaxEDUs is MAX_EDUS_PER_TRANSACTION, 100 (api/constants.py:56).
	MaxEDUs int
}

// EDU is an ephemeral unit waiting to go to a destination, with the
// bookkeeping that becomes due once it has actually been delivered.
//
// Synapse tracks the same thing on its queue (_device_stream_id and
// _device_list_id) and acts on it in __aexit__, only when the transaction
// succeeded. The marks travel WITH the unit rather than on the queue, because
// a transaction takes a prefix of what is queued: a mark held on the queue
// would be committed for units that were not in the batch that succeeded.
type EDU struct {
	Unit txn.EDU
	// ToDeviceUpTo is the device_federation_outbox stream id this unit
	// consumed, or zero. Once delivered, those rows may be deleted -- that
	// deletion is the real sender's cursor.
	ToDeviceUpTo int64
	// DeviceListUpTo is the device_lists_outbound_pokes stream id this unit
	// consumed, or zero.
	DeviceListUpTo int64
}

// PDU is an event waiting to go to a destination.
type PDU struct {
	EventID string
	// StreamOrdering is the catch-up cursor's unit, advanced only after a
	// successful transaction.
	StreamOrdering int64
	// JSON is the serialised event, ready to drop into a transaction.
	JSON []byte
}

// Destination is one remote server's queue.
//
// Its lock covers the pending slices only. The transmission loop deliberately
// runs outside it: a send can take as long as a remote server takes to answer,
// and holding the lock across that would block every producer trying to enqueue
// for this destination -- which, for a busy server, is most of them.
type Destination struct {
	name   string
	limits Limits
	signer *txn.Signer
	ids    *txn.IDGenerator
	sink   sink.Sink
	log    zerolog.Logger

	mu          sync.Mutex
	pendingPDUs []PDU
	pendingEDUs []EDU
	// running is the "exactly one in-flight transaction" mutex. Synapse calls
	// it transmission_loop_running.
	running bool
	// newData is the re-check-before-exiting flag. It must be cleared at the
	// TOP of each loop iteration, not the bottom: something enqueued while a
	// transaction was in flight has to be noticed, and clearing it after the
	// send would swallow exactly those (per_destination_queue.py:368).
	newData bool

	// lastSuccessfulStreamOrdering is our copy of the destinations table's
	// column. We never write that table, so this is the in-memory truth and
	// internal/state is where it is persisted.
	lastSuccessfulStreamOrdering int64

	// onSuccess is called after a delivered transaction, with the highest
	// stream ordering in it, so the caller can persist the cursor.
	onSuccess func(destination string, streamOrdering int64)
	// onEDUsSent is called after a delivered transaction carrying marked EDUs,
	// with the highest consumed stream id of each kind.
	onEDUsSent func(destination string, toDeviceUpTo, deviceListUpTo int64)
	// onOutcome is called after every attempt, delivered or not, so a primary
	// can keep the destination's persistent backoff. Called for failures too,
	// which is the half that matters: without it a dead server is retried on
	// every event forever.
	onOutcome func(destination string, delivered bool)
	// due gates the loop. Synapse checks its retry limiter at the TOP of the
	// transmission loop and abandons the run entirely when the destination is
	// backing off (per_destination_queue.py:351). Without that gate the
	// backoff only slows down how often a dead server is enqueued for, not how
	// often it is actually dialled.
	due func(destination string) bool
}

// Config builds a Destination.
type Config struct {
	Name       string
	Limits     Limits
	Signer     *txn.Signer
	IDs        *txn.IDGenerator
	Sink       sink.Sink
	Log        zerolog.Logger
	OnSuccess  func(destination string, streamOrdering int64)
	OnEDUsSent func(destination string, toDeviceUpTo, deviceListUpTo int64)
	OnOutcome  func(destination string, delivered bool)
	Due        func(destination string) bool
}

// NewDestination builds a queue for one remote server.
func NewDestination(cfg Config) *Destination {
	limits := cfg.Limits
	if limits.MaxPDUs <= 0 {
		limits.MaxPDUs = 50
	}
	if limits.MaxEDUs <= 0 {
		limits.MaxEDUs = 100
	}
	return &Destination{
		name:       cfg.Name,
		limits:     limits,
		signer:     cfg.Signer,
		ids:        cfg.IDs,
		sink:       cfg.Sink,
		log:        cfg.Log.With().Str("destination", cfg.Name).Logger(),
		onSuccess:  cfg.OnSuccess,
		onEDUsSent: cfg.OnEDUsSent,
		onOutcome:  cfg.OnOutcome,
		due:        cfg.Due,
	}
}

// Name is the remote server's name.
func (d *Destination) Name() string { return d.name }

// EnqueuePDU adds an event to the queue.
func (d *Destination) EnqueuePDU(p PDU) {
	d.mu.Lock()
	d.pendingPDUs = append(d.pendingPDUs, p)
	d.newData = true
	d.mu.Unlock()
}

// EnqueueEDU adds an ephemeral unit to the queue.
func (d *Destination) EnqueueEDU(e txn.EDU) {
	d.EnqueueMarkedEDU(EDU{Unit: e})
}

// EnqueueMarkedEDU adds a unit whose delivery makes bookkeeping due.
func (d *Destination) EnqueueMarkedEDU(e EDU) {
	d.mu.Lock()
	d.pendingEDUs = append(d.pendingEDUs, e)
	d.newData = true
	d.mu.Unlock()
}

// Pending reports how much is waiting, for metrics and tests.
func (d *Destination) Pending() (pdus, edus int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pendingPDUs), len(d.pendingEDUs)
}

// TryStart claims the right to run the transmission loop.
//
// It returns false when a loop is already in flight, having set newData so that
// loop picks up whatever was just enqueued. This is Synapse's
// attempt_new_transaction (per_destination_queue.py:306) and it is what keeps
// "one transaction at a time per destination" true without a queue of pending
// wakeups.
//
// Split from Run so the caller decides where the loop runs. The Manager needs
// that: it holds a concurrency slot for the loop's whole lifetime, which is
// impossible if starting the loop also spawns the goroutine.
func (d *Destination) TryStart() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		d.newData = true
		return false
	}
	d.running = true
	return true
}

// Attempt starts the loop in its own goroutine if one is not already running.
//
// The unbounded form, used by tests and by callers with no concurrency budget
// to manage. Production goes through Manager.Wake.
func (d *Destination) Attempt(ctx context.Context) {
	if d.TryStart() {
		go d.Run(ctx)
	}
}

// Run is the transmission loop. The caller must have won TryStart.
func (d *Destination) Run(ctx context.Context) {
	defer func() {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
	}()

	for ctx.Err() == nil {
		// The backoff gate, checked before every transaction rather than only
		// when the event was routed. A destination can go into backoff while
		// work is already queued for it, and a dead server should be dialled
		// on the interval, not on the traffic.
		//
		// The queued units stay queued: they are what catch-up and the next
		// attempt will send. Only the attempt is abandoned.
		if d.due != nil && !d.due(d.name) {
			d.log.Debug().Msg("destination is backing off; not attempting")
			return
		}

		d.mu.Lock()
		// Cleared at the TOP, so anything enqueued during the send below is
		// seen by the next iteration rather than lost.
		d.newData = false
		pdus, edus := d.takeLocked()
		d.mu.Unlock()

		if len(pdus) == 0 && len(edus) == 0 {
			// Nothing to send. If something arrived between the check and
			// here, go round again; otherwise the loop is done.
			d.mu.Lock()
			again := d.newData
			d.mu.Unlock()
			if again {
				continue
			}
			return
		}

		err := d.send(ctx, pdus, edus)
		if d.onOutcome != nil {
			d.onOutcome(d.name, err == nil)
		}
		if err != nil {
			// The units stay queued: nothing was dequeued, because takeLocked
			// only copies. Synapse gets the same result from __aexit__ bailing
			// on an exception. The loop exits and a later Attempt retries.
			d.log.Warn().Err(err).
				Int("pdus", len(pdus)).Int("edus", len(edus)).
				Msg("transaction failed; units remain queued")
			return
		}

		d.dequeue(len(pdus), len(edus))

		// Bookkeeping for the EDUs that were actually in the delivered
		// transaction, and only then. Synapse does the same in __aexit__,
		// which it skips entirely when the send raised.
		if d.onEDUsSent != nil {
			var toDevice, deviceList int64
			for _, e := range edus {
				if e.ToDeviceUpTo > toDevice {
					toDevice = e.ToDeviceUpTo
				}
				if e.DeviceListUpTo > deviceList {
					deviceList = e.DeviceListUpTo
				}
			}
			if toDevice > 0 || deviceList > 0 {
				d.onEDUsSent(d.name, toDevice, deviceList)
			}
		}

		if len(pdus) > 0 {
			last := pdus[len(pdus)-1].StreamOrdering
			d.mu.Lock()
			if last > d.lastSuccessfulStreamOrdering {
				d.lastSuccessfulStreamOrdering = last
			}
			d.mu.Unlock()
			if d.onSuccess != nil {
				d.onSuccess(d.name, last)
			}
		}
	}
}

// takeLocked copies the next transaction's worth of units WITHOUT removing
// them.
//
// Not removing is the point. If the send fails, the units must still be queued,
// and a take-then-restore would reorder them against anything enqueued in the
// meantime. Only dequeue, after a confirmed delivery, actually removes.
func (d *Destination) takeLocked() ([]PDU, []EDU) {
	pdus := d.pendingPDUs
	if len(pdus) > d.limits.MaxPDUs {
		pdus = pdus[:d.limits.MaxPDUs]
	}
	edus := d.pendingEDUs
	if len(edus) > d.limits.MaxEDUs {
		edus = edus[:d.limits.MaxEDUs]
	}
	return pdus, edus
}

func (d *Destination) dequeue(pdus, edus int) {
	d.mu.Lock()
	d.pendingPDUs = d.pendingPDUs[pdus:]
	d.pendingEDUs = d.pendingEDUs[edus:]
	d.mu.Unlock()
}

func (d *Destination) send(ctx context.Context, pdus []PDU, edus []EDU) error {
	t := txn.Transaction{OriginServerTS: time.Now().UnixMilli()}
	for _, p := range pdus {
		t.PDUs = append(t.PDUs, p.JSON)
	}
	for _, e := range edus {
		t.EDUs = append(t.EDUs, e.Unit)
	}

	req, err := d.signer.Build(d.ids.Next(), d.name, t)
	if err != nil {
		return err
	}
	res, err := d.sink.Send(ctx, req)
	if err != nil {
		return err
	}
	if !res.Delivered {
		return errNotDelivered
	}
	for eventID, msg := range res.PDUErrors {
		// Synapse logs these and never retries them
		// (transaction_manager.py:197): the remote has made a decision about
		// that event, and sending it again would get the same answer.
		d.log.Warn().Str("event_id", eventID).Str("error", msg).
			Msg("remote rejected a PDU")
	}
	return nil
}

// LastSuccessfulStreamOrdering is our copy of the destinations table column.
func (d *Destination) LastSuccessfulStreamOrdering() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastSuccessfulStreamOrdering
}

// SetLastSuccessfulStreamOrdering seeds the cursor at startup.
func (d *Destination) SetLastSuccessfulStreamOrdering(v int64) {
	d.mu.Lock()
	if v > d.lastSuccessfulStreamOrdering {
		d.lastSuccessfulStreamOrdering = v
	}
	d.mu.Unlock()
}

// Release gives up a claim taken by TryStart without running the loop.
//
// Only for a caller that won TryStart and then decided not to Run. Without it
// that destination would be marked as sending forever and would never send
// again -- a deadlock of one server, which is precisely the kind of thing that
// goes unnoticed on a homeserver talking to thousands.
func (d *Destination) Release() {
	d.mu.Lock()
	d.running = false
	d.mu.Unlock()
}

// PeekEDUs returns a copy of the queued EDU units.
//
// For metrics and tests. A copy rather than the slice itself, because the
// transmission loop takes it without removing and a caller holding the live
// slice could observe it being re-sliced under them.
func (d *Destination) PeekEDUs() []txn.EDU {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]txn.EDU, 0, len(d.pendingEDUs))
	for _, e := range d.pendingEDUs {
		out = append(out, e.Unit)
	}
	return out
}
