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
	"errors"
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
	// Key, when set, makes this unit replace any pending unit of the same type
	// with the same key. Typing uses it: two updates about one fact, of which
	// only the latest is worth delivering.
	Key string
	// Receipts holds a merged m.receipt EDU, room -> type -> user. Encoded at
	// transaction time rather than on arrival, since receipts keep merging in
	// while the EDU waits.
	Receipts map[string]map[string]map[string]receiptEntry
	// Presence holds a merged m.presence EDU, user -> that user's state. Like
	// Receipts, and for the same reason: one EDU carries up to fifty users
	// (per_destination_queue.py:79) and sending one EDU per user spends a
	// transaction on each.
	Presence map[string]PresenceEntry
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

	// catchingUp starts TRUE for every destination, as it does in Synapse
	// (per_destination_queue.py:137).
	//
	// The reason is not obvious and it is the whole design: a sender that has
	// been down does not know what it missed, and the live stream will only
	// ever tell it about NEW events. What it missed is recorded in
	// destination_rooms, and the only way to find it is to look. So every
	// destination begins by assuming it is behind and proving otherwise.
	catchingUp bool
	// catchupLastSkipped is the newest stream ordering dropped while catching
	// up. It exists to close a race: an event can arrive after catch-up has
	// read its last page and before it decides it is finished, and without
	// this that event would be dropped by catch-up and never seen again by the
	// live path either.
	catchupLastSkipped int64

	// lastSuccessfulStreamOrdering is our copy of the destinations table's
	// column, and lastSuccessfulKnown says whether it has been read yet.
	//
	// The distinction is load-bearing. A destination with NO recorded value has
	// never had a successful transaction, so there is no point from which to
	// catch up and Synapse leaves catch-up immediately rather than replaying
	// history for it (per_destination_queue.py:485).
	lastSuccessfulStreamOrdering int64
	lastSuccessfulKnown          bool

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
	// longBackoff reports a destination in a real outage rather than a blip.
	longBackoff func(destination string) bool
	// onEDUsDropped reports what a long outage cost.
	onEDUsDropped func(destination string, n int)
	// onRateLimited reports a destination asking us to slow down, and returns
	// how long the limiter decided to wait, so the queue can schedule its own
	// retry for that moment.
	onRateLimited func(destination string, after time.Duration) time.Duration
	// outageLogged keeps one outage to one log line. Guarded by the loop, which
	// is single-threaded per destination.
	outageLogged bool

	batch BatchConfig
	wake  func()
	// heldSince is when the presence currently waiting was first held back.
	// Zero when nothing is being held.
	heldSince time.Time
	// holdTimer wakes the loop when a hold expires, so a destination with
	// nothing but held presence still sends. Without it the loop only ever runs
	// when something new arrives, and the last few states before a quiet period
	// would wait for the next one indefinitely.
	holdTimer *time.Timer
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
	// LongBackoff reports whether a destination's backoff has grown past
	// CatchUpRetryInterval -- i.e. that it will not be tried again for at least
	// an hour. See the drop in Run.
	LongBackoff func(destination string) bool
	// OnEDUsDropped reports ephemeral EDUs abandoned because the destination
	// is in a long outage, so the count is visible rather than inferred from a
	// queue that stopped growing.
	OnEDUsDropped func(destination string, n int)
	// OnRateLimited reports a destination that answered 429, with the delay it
	// asked for if it gave one.
	//
	// Separate from OnOutcome because a 429 is not a failed destination. The
	// server received the request, understood it and declined it: it is up, and
	// the answer is to slow down rather than to write it off. Reporting it as a
	// failure grows a backoff built for dead servers, which on this deployment
	// multiplies to a year.
	OnRateLimited func(destination string, after time.Duration) time.Duration
	// Batch holds presence back so it can accumulate. Zero disables it.
	Batch BatchConfig
	// Wake re-runs this destination's loop, used when a hold expires. Set by
	// the Manager, which owns the concurrency bound.
	Wake func()
}

// BatchConfig is how long presence may be held to accumulate.
//
// PRESENCE ONLY. Typing and receipts are never held: typing is a statement
// about this instant and a remote expires it after a minute, and a read receipt
// arriving late is the thing a read receipt is for. Both are also natural flush
// triggers -- when one is queued the whole transaction goes at once, and any
// presence waiting rides along for free.
//
// The point is not our own throughput. A transaction carrying one presence
// state costs the RECEIVING server an HTTP request, a canonical JSON encoding
// and an ed25519 verification, all of it the same cost as carrying fifty. At
// 96% of this worker's transactions being a single EDU, that is load we are
// putting on everyone else's servers for no reason.
type BatchConfig struct {
	// MaxStates flushes once this many presence states are waiting. Synapse's
	// per-EDU bound is 50 (per_destination_queue.py:79), so more than that in
	// one transaction means more than one EDU.
	MaxStates int
	// MaxWait bounds how long the OLDEST held state waits. Chosen over "time
	// since the last transmission", which sounds equivalent and is not: it
	// makes the delay depend on when the previous send happened rather than
	// bounding the staleness of the data being held.
	MaxWait time.Duration
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
		name: cfg.Name,
		// Starts TRUE, as in Synapse. A sender that has been down cannot know
		// what it missed from the live stream alone, so every destination
		// begins by assuming it is behind and proves otherwise.
		catchingUp:    true,
		limits:        limits,
		signer:        cfg.Signer,
		ids:           cfg.IDs,
		sink:          cfg.Sink,
		longBackoff:   cfg.LongBackoff,
		onEDUsDropped: cfg.OnEDUsDropped,
		onRateLimited: cfg.OnRateLimited,
		log:           cfg.Log.With().Str("destination", cfg.Name).Logger(),
		onSuccess:     cfg.OnSuccess,
		onEDUsSent:    cfg.OnEDUsSent,
		onOutcome:     cfg.OnOutcome,
		due:           cfg.Due,
		batch:         cfg.Batch,
		wake:          cfg.Wake,
	}
}

// Name is the remote server's name.
func (d *Destination) Name() string { return d.name }

// EnqueuePDU adds an event to the queue.
//
// While catching up the event is DROPPED rather than queued, and only its
// stream ordering is remembered. That looks like losing it and is the opposite:
// the durable record is destination_rooms, which was written before this, and
// catch-up will find it there in stream order. Queueing it as well would send
// it out of order, ahead of everything the destination missed while it was
// down (per_destination_queue.py:206).
func (d *Destination) EnqueuePDU(p PDU) {
	d.mu.Lock()
	if d.catchingUp && d.lastSuccessfulKnown {
		if p.StreamOrdering > d.catchupLastSkipped {
			d.catchupLastSkipped = p.StreamOrdering
		}
		d.mu.Unlock()
		return
	}
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

// ErrBusy means the destination is already sending, so the caller must come
// back later rather than send alongside.
//
// NOT a delivery failure, and callers must not treat it as one. Recording it as
// a failure would grow the destination's backoff because of our own scheduling,
// and on this deployment the backoff is capped at a year.
var ErrBusy = errors.New("queue: destination is already sending")

// claim takes the one-transaction-at-a-time claim without marking new data.
//
// Distinct from TryStart, which sets newData on failure so the running loop
// goes round again. A caller that merely wants to know whether it may send --
// catch-up -- has no new data to announce, and saying otherwise would make the
// running loop do an extra empty pass.
func (d *Destination) claim() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		return false
	}
	d.running = true
	return true
}

// releaseClaim drops the claim and re-wakes the loop if work arrived while it
// was held.
//
// The re-wake is not optional. Wake sets newData and gives up when the
// destination is claimed, relying on the running LOOP to go round again -- and
// catch-up is not that loop. Without this, live traffic queued during a
// catch-up send would sit until something else happened to wake the
// destination.
func (d *Destination) releaseClaim() {
	d.mu.Lock()
	pending := d.newData
	d.running = false
	d.mu.Unlock()

	if pending && d.wake != nil {
		d.wake()
	}
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
			// A destination we will not try again for at least an hour is in a
			// real outage, and holding its ephemeral EDUs until it returns is
			// both useless and unbounded: presence and receipts for a dead
			// server accumulate for as long as it stays dead. Synapse drops
			// them at exactly this point and for exactly this reason --
			// "otherwise they will rack up indefinitely"
			// (per_destination_queue.py:423).
			//
			// Only the EDUs we can afford to lose go: typing, receipts and
			// presence say something about NOW, and a presence update delivered
			// after an hour is not stale, it is wrong. The ones carrying a
			// durable mark are left alone -- they are a cursor into a table,
			// and dropping them would advance nothing but lose the poke.
			//
			// The PDUs go too, via catch-up, which is Synapse's
			// _start_catching_up: they are recoverable from destination_rooms,
			// so holding them in memory buys nothing.
			if d.longBackoff != nil && d.longBackoff(d.name) {
				if n := d.dropEphemeralEDUs(); n > 0 {
					// Once per outage, not once per EDU. Every enqueue wakes
					// this loop, so a busy room drops one EDU at a time and an
					// unconditional log prints a line per receipt per dead
					// server, forever -- thousands of lines saying the same
					// thing about the same outage. The count that matters is
					// gofed_edus_dropped_total, which loses nothing by the log
					// being quiet.
					if !d.outageLogged {
						d.outageLogged = true
						d.log.Info().Int("edus", n).
							Msg("destination is in a long outage; dropping its ephemeral EDUs " +
								"and catching up later (further drops for it are not logged)")
					}
					if d.onEDUsDropped != nil {
						d.onEDUsDropped(d.name, n)
					}
				}
				d.RestartCatchUp()
			}
			d.log.Debug().Msg("destination is backing off; not attempting")
			return
		}

		d.mu.Lock()
		// Past the gate, so the outage is over as far as this loop knows; the
		// next one gets its own line.
		d.outageLogged = false
		// Cleared at the TOP, so anything enqueued during the send below is
		// seen by the next iteration rather than lost.
		d.newData = false

		// Hold presence back, if there is nothing else to send and it has not
		// waited long enough. Everything else -- a PDU, typing, a receipt, a
		// device EDU -- flushes the whole queue immediately and takes any held
		// presence with it.
		if wait := d.holdForLocked(time.Now()); wait > 0 {
			d.scheduleFlushLocked(wait)
			d.mu.Unlock()
			return
		}
		d.heldSince = time.Time{}

		batch, err := d.takeLocked()
		d.mu.Unlock()
		if err != nil {
			// An EDU that cannot be encoded. It stays queued, as a failed send
			// would, and the loop exits for a later Attempt to retry.
			d.log.Error().Err(err).Msg("failed to encode a queued EDU")
			return
		}
		pdus, edus := batch.pdus, batch.edus

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

		err = d.send(ctx, batch)
		wait, limited := d.reportOutcome(err)
		if err != nil {
			// The units stay queued either way: nothing was dequeued, because
			// takeLocked only copies. Synapse gets the same result from
			// __aexit__ bailing on an exception.
			if limited {
				// Not a failure, and it must not be logged as one. The remote
				// is up and has asked us to slow down, so the only useful
				// response is to wait -- especially for "still processing
				// another transaction from this origin", which says our own
				// previous transaction is still being worked on.
				//
				// Schedule the retry rather than waiting for the next event to
				// wake us. Without this a quiet destination holds its units
				// until traffic happens to arrive, which is arbitrarily long
				// and looks exactly like delivery working.
				d.mu.Lock()
				d.scheduleFlushLocked(wait)
				d.mu.Unlock()
				d.log.Info().
					Int("pdus", len(pdus)).Int("edus", len(edus)).
					Dur("retry_in", wait).
					Msg("rate limited; units stay queued and the queue will retry")
				return
			}
			d.log.Warn().Err(err).
				Int("pdus", len(pdus)).Int("edus", len(edus)).
				Msg("transaction failed; units remain queued")
			return
		}

		d.dequeue(len(pdus), len(edus))

		// Bookkeeping for the EDUs that were actually in the delivered
		// transaction, and only then. Synapse does the same in __aexit__,
		// which it skips entirely when the send raised.
		if d.onEDUsSent != nil && (batch.toDeviceUpTo > 0 || batch.deviceListUpTo > 0) {
			d.onEDUsSent(d.name, batch.toDeviceUpTo, batch.deviceListUpTo)
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

// taken is one transaction's worth of work, fully detached from the queue.
//
// Detached is the load-bearing word. A transaction is built and sent WITHOUT
// the lock -- it waits on a remote server, so holding the lock would stall
// every enqueue to that destination for the length of a network round trip --
// and the two pending EDU shapes are both mutated IN PLACE while it waits:
// a keyed EDU is clobbered by the next update for the same key, and a receipt
// EDU has receipts merged into its nested maps. Handing the sender a slice that
// still aliases the queue means it reads those bytes as they are being
// rewritten. The keyed case is a data race; the receipt case is a concurrent
// map read and write, which is a fatal runtime error rather than merely a wrong
// answer.
//
// So everything the send and the bookkeeping after it need is copied out here,
// under the lock, and the EDUs are materialised while they still cannot change.
type taken struct {
	pdus []PDU
	// edus are already encoded, so nothing downstream can observe a merge in
	// progress.
	edus []txn.EDU
	// toDeviceUpTo and deviceListUpTo are the maxima over the taken EDUs,
	// computed here so the post-send bookkeeping needs no second look at the
	// queue.
	toDeviceUpTo   int64
	deviceListUpTo int64
}

// takeLocked copies the next transaction's worth of units WITHOUT removing
// them.
//
// Not removing is the point. If the send fails, the units must still be queued,
// and a take-then-restore would reorder them against anything enqueued in the
// meantime. Only dequeue, after a confirmed delivery, actually removes.
func (d *Destination) takeLocked() (taken, error) {
	pdus := d.pendingPDUs
	if len(pdus) > d.limits.MaxPDUs {
		pdus = pdus[:d.limits.MaxPDUs]
	}
	edus := d.pendingEDUs
	if len(edus) > d.limits.MaxEDUs {
		edus = edus[:d.limits.MaxEDUs]
	}

	t := taken{pdus: append([]PDU(nil), pdus...)}
	for _, e := range edus {
		// Materialising here rather than in send is what makes a merged receipt
		// EDU safe: it walks maps the enqueue path writes to.
		unit, err := e.materialise()
		if err != nil {
			return taken{}, err
		}
		t.edus = append(t.edus, unit)
		if e.ToDeviceUpTo > t.toDeviceUpTo {
			t.toDeviceUpTo = e.ToDeviceUpTo
		}
		if e.DeviceListUpTo > t.deviceListUpTo {
			t.deviceListUpTo = e.DeviceListUpTo
		}
	}
	return t, nil
}

// CatchUpRetryInterval is the backoff past which a destination is treated as
// being in a real outage rather than a blip: Synapse's CATCHUP_RETRY_INTERVAL
// (per_destination_queue.py:75).
const CatchUpRetryInterval = time.Hour

// dropEphemeralEDUs discards the queued EDUs that say something about NOW.
//
// Typing, receipts and presence: the ones Synapse names as affordable to lose
// (per_destination_queue.py:427). An EDU carrying a to-device or device-list
// mark is kept, because that mark is a cursor into a table and the row it
// stands for is still owed.
func (d *Destination) dropEphemeralEDUs() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	kept := d.pendingEDUs[:0]
	dropped := 0
	for _, e := range d.pendingEDUs {
		if e.ToDeviceUpTo == 0 && e.DeviceListUpTo == 0 {
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	// Reslicing in place would leave the dropped entries reachable through the
	// backing array; a fresh slice lets them be collected, which is the point.
	d.pendingEDUs = append([]EDU(nil), kept...)
	return dropped
}

// holdForLocked reports how much longer presence should be held, or zero to
// send now.
//
// Anything that is not presence sends immediately, which is what keeps typing
// and receipts out of the batching without a second code path for them: they
// are in the same queue, so their presence returns zero and the whole
// transaction goes.
func (d *Destination) holdForLocked(now time.Time) time.Duration {
	if d.batch.MaxWait <= 0 || d.batch.MaxStates <= 0 {
		return 0
	}
	if len(d.pendingPDUs) > 0 || len(d.pendingEDUs) == 0 {
		return 0
	}

	states := 0
	for i := range d.pendingEDUs {
		if d.pendingEDUs[i].Presence == nil {
			// Typing, a receipt, a device EDU: not ours to delay.
			return 0
		}
		states += len(d.pendingEDUs[i].Presence)
	}
	if states >= d.batch.MaxStates {
		return 0
	}

	if d.heldSince.IsZero() {
		d.heldSince = now
	}
	if remaining := d.batch.MaxWait - now.Sub(d.heldSince); remaining > 0 {
		return remaining
	}
	return 0
}

// scheduleFlushLocked arranges for the loop to run again when the hold expires.
//
// One timer at a time: a destination receiving a state every second would
// otherwise stack a timer per arrival, all firing on the same queue.
func (d *Destination) scheduleFlushLocked(wait time.Duration) {
	if d.holdTimer != nil || d.wake == nil {
		return
	}
	d.holdTimer = time.AfterFunc(wait, func() {
		d.mu.Lock()
		d.holdTimer = nil
		d.mu.Unlock()
		d.wake()
	})
}

// reportOutcome tells the caller what happened, distinguishing a rate limit
// from a failure.
//
// A 429 is deliberately NOT reported through onOutcome. The three cases the
// sender has to tell apart are: no answer before the timeout, which means the
// host has problems; an HTTP error that is not a 429, which means the host is
// answering but cannot accept this; and a 429, which means the host is
// perfectly fine and we are being too aggressive. Only the first two are the
// destination's fault, and only they should back it off.
// It returns how long to wait before trying this destination again, and
// whether the error was a rate limit at all. The caller schedules that retry:
// a 429 that is never retried until the next event arrives leaves units queued
// for an unbounded time on a quiet destination.
func (d *Destination) reportOutcome(err error) (time.Duration, bool) {
	if err != nil {
		if after, limited := sink.IsRateLimited(err); limited {
			var wait time.Duration
			if d.onRateLimited != nil {
				wait = d.onRateLimited(d.name, after)
			}
			return wait, true
		}
	}
	if d.onOutcome != nil {
		d.onOutcome(d.name, err == nil)
	}
	return 0, false
}

func (d *Destination) dequeue(pdus, edus int) {
	d.mu.Lock()
	d.pendingPDUs = d.pendingPDUs[pdus:]
	d.pendingEDUs = d.pendingEDUs[edus:]
	d.mu.Unlock()
}

func (d *Destination) send(ctx context.Context, batch taken) error {
	t := txn.Transaction{OriginServerTS: time.Now().UnixMilli()}
	for _, p := range batch.pdus {
		t.PDUs = append(t.PDUs, p.JSON)
	}
	t.EDUs = append(t.EDUs, batch.edus...)

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
	d.lastSuccessfulKnown = true
	d.mu.Unlock()
}

// CatchingUp reports whether this destination is still replaying a backlog.
func (d *Destination) CatchingUp() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.catchingUp
}

// FinishCatchUp marks the destination as up to date.
func (d *Destination) FinishCatchUp() {
	d.mu.Lock()
	d.catchingUp = false
	d.mu.Unlock()
}

// RestartCatchUp puts the destination back into catch-up and discards what is
// queued.
//
// Synapse does this when a destination has been unreachable for longer than an
// hour (per_destination_queue.py:420): whatever is queued is a fragment of what
// the destination now needs, and sending it would deliver a handful of recent
// events ahead of everything older. destination_rooms has the whole story, so
// the queue is dropped and catch-up reads it back in order.
func (d *Destination) RestartCatchUp() {
	d.mu.Lock()
	d.catchingUp = true
	d.pendingPDUs = nil
	d.mu.Unlock()
}

// TakeCatchUpSkipped returns and clears the newest ordering dropped while
// catching up.
func (d *Destination) TakeCatchUpSkipped() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.catchupLastSkipped
	d.catchupLastSkipped = 0
	return v
}

// SendCatchUp delivers one catch-up transaction: a single room's event, no
// EDUs, as Synapse does (per_destination_queue.py:596).
//
// One room per transaction rather than batching, because the destination is
// being brought forward room by room and a failure should cost one room's
// progress rather than fifty.
func (d *Destination) SendCatchUp(ctx context.Context, p PDU) error {
	// The same one-transaction-at-a-time claim the live loop takes. Catch-up
	// runs on its own goroutine, so without this it sends ALONGSIDE the
	// transmission loop -- which puts PDUs on the wire out of order and makes
	// the remote answer 429 "still processing another transaction from this
	// origin". Observed doing exactly that, and the resulting 429s were
	// recorded as delivery failures, so this worker was growing a live server's
	// backoff because it was talking over itself.
	if !d.claim() {
		return ErrBusy
	}
	defer d.releaseClaim()

	// Catch-up carries no EDUs at all, so there is nothing here that another
	// goroutine could be mutating; the batch is built from one event.
	if err := d.send(ctx, taken{pdus: []PDU{p}}); err != nil {
		d.reportOutcome(err)
		return err
	}
	d.reportOutcome(nil)
	d.mu.Lock()
	if p.StreamOrdering > d.lastSuccessfulStreamOrdering {
		d.lastSuccessfulStreamOrdering = p.StreamOrdering
		d.lastSuccessfulKnown = true
	}
	d.mu.Unlock()
	if d.onSuccess != nil {
		d.onSuccess(d.name, p.StreamOrdering)
	}
	return nil
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
	for i := range d.pendingEDUs {
		if unit, err := d.pendingEDUs[i].materialise(); err == nil {
			out = append(out, unit)
		}
	}
	return out
}
