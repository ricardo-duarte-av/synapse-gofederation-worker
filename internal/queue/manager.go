package queue

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Manager owns every destination queue.
//
// Queues are created on demand and never removed. Synapse does the same
// (_get_per_destination_queue), and the reason is the same: a destination's
// catch-up cursor and its position in the queue are state, and dropping the
// queue for an idle server would lose it. On this deployment that is around
// 28,000 small structs, which is nothing; goroutines are only spawned for
// destinations with something to send.
type Manager struct {
	limits Limits
	signer *txn.Signer
	ids    *txn.IDGenerator
	sink   sink.Sink
	log    zerolog.Logger

	// sem bounds how many destinations may be sending at once. The point of
	// this worker is that a thousand destinations cost a thousand goroutines
	// rather than a thousand coroutines on one reactor, but unbounded is still
	// a way to fall over -- and against a slow remote, unbounded means every
	// goroutine parked on the same socket timeout.
	sem chan struct{}

	onSuccess     func(destination string, streamOrdering int64)
	onEDUsSent    func(destination string, toDeviceUpTo, deviceListUpTo int64)
	onOutcome     func(destination string, delivered bool)
	due           func(destination string) bool
	batch         BatchConfig
	longBackoff   func(destination string) bool
	onEDUsDropped func(destination string, n int)
	onRateLimited func(destination string, after time.Duration)

	mu    sync.RWMutex
	dests map[string]*Destination

	// base is the context transmission loops run under.
	//
	// It MUST outlive whatever fed the queue. An earlier version passed the
	// caller's context straight through, and the caller was an errgroup whose
	// context is cancelled the moment its batch finishes -- so a loop spawned
	// near the end of a batch raced that cancellation and, when it lost,
	// returned without sending anything. Nothing logged it, because exiting on
	// a cancelled context is exactly what the loop is supposed to do. Found by
	// watching a real message fail to be delivered.
	base context.Context
}

// ManagerConfig builds a Manager.
type ManagerConfig struct {
	Limits        Limits
	Signer        *txn.Signer
	IDs           *txn.IDGenerator
	Sink          sink.Sink
	Log           zerolog.Logger
	MaxConcurrent int
	OnSuccess     func(destination string, streamOrdering int64)
	OnEDUsSent    func(destination string, toDeviceUpTo, deviceListUpTo int64)
	OnOutcome     func(destination string, delivered bool)
	Due           func(destination string) bool
	// Batch holds presence back so it can accumulate; see queue.BatchConfig.
	Batch BatchConfig
	// LongBackoff reports whether a destination will not be retried for at
	// least CatchUpRetryInterval, which is when its ephemeral EDUs are dropped
	// rather than held forever. See Destination.Run.
	LongBackoff func(destination string) bool
	// OnEDUsDropped reports what such an outage cost.
	OnEDUsDropped func(destination string, n int)
	// OnRateLimited reports a destination asking us to slow down; see
	// Config.OnRateLimited.
	OnRateLimited func(destination string, after time.Duration)
}

// NewManager builds a Manager.
func NewManager(cfg ManagerConfig) *Manager {
	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 2000
	}
	return &Manager{
		limits:        cfg.Limits,
		signer:        cfg.Signer,
		ids:           cfg.IDs,
		sink:          cfg.Sink,
		log:           cfg.Log,
		sem:           make(chan struct{}, maxConcurrent),
		onSuccess:     cfg.OnSuccess,
		onEDUsSent:    cfg.OnEDUsSent,
		onOutcome:     cfg.OnOutcome,
		due:           cfg.Due,
		batch:         cfg.Batch,
		longBackoff:   cfg.LongBackoff,
		onEDUsDropped: cfg.OnEDUsDropped,
		onRateLimited: cfg.OnRateLimited,
		dests:         map[string]*Destination{},
		base:          context.Background(),
	}
}

// Start binds the manager to the worker's lifetime.
//
// Called once, with a context that lives as long as the process. Everything a
// transmission loop does -- waiting for a concurrency slot, building a
// transaction, waiting on a remote server to answer -- outlives the batch that
// queued the work, so the batch's context is the wrong one and its cancellation
// silently drops the send.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	m.base = ctx
	m.mu.Unlock()
}

// Get returns a destination's queue, creating it if needed.
func (m *Manager) Get(name string) *Destination {
	m.mu.RLock()
	d, ok := m.dests[name]
	m.mu.RUnlock()
	if ok {
		return d
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check: another goroutine may have created it while we swapped locks,
	// and two queues for one destination would mean two concurrent
	// transactions to it.
	if d, ok := m.dests[name]; ok {
		return d
	}
	d = NewDestination(Config{
		Batch:         m.batch,
		Name:          name,
		Limits:        m.limits,
		Signer:        m.signer,
		IDs:           m.ids,
		Sink:          m.sink,
		Log:           m.log,
		OnSuccess:     m.onSuccess,
		OnEDUsSent:    m.onEDUsSent,
		OnOutcome:     m.onOutcome,
		Due:           m.due,
		LongBackoff:   m.longBackoff,
		OnEDUsDropped: m.onEDUsDropped,
		OnRateLimited: m.onRateLimited,
	})
	// The destination needs to be able to re-run its own loop when a batching
	// hold expires, and the Manager owns the concurrency bound, so the hook is
	// wired after construction rather than passed in.
	d.wake = func() { m.Wake(d) }
	m.dests[name] = d
	return d
}

// Count is how many destinations have a queue.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.dests)
}

// Names returns every known destination, for metrics and tests.
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.dests))
	for name := range m.dests {
		out = append(out, name)
	}
	return out
}

// Wake starts a destination's transmission loop, respecting the concurrency
// bound.
//
// Wake never blocks its caller. Fanning one event out to a thousand
// destinations must not be held up by whichever of them is slowest, so the
// concurrency slot is acquired inside the spawned goroutine and held for the
// loop's whole lifetime -- which is why the loop is split into TryStart and
// Run. Taking the slot before spawning would bound nothing, since Run is where
// the time goes.
//
// It deliberately takes no context. The loop runs under the manager's base
// context, not the caller's: the caller is a batch, and a batch ends long
// before the servers it queued work for have answered.
//
// A destination that loses TryStart is already sending; that loop will see the
// newData flag TryStart set and go round again, so nothing is dropped and no
// goroutine is spawned to wait for it.
func (m *Manager) Wake(d *Destination) {
	if !d.TryStart() {
		return
	}
	m.mu.RLock()
	ctx := m.base
	m.mu.RUnlock()

	go func() {
		select {
		case m.sem <- struct{}{}:
		case <-ctx.Done():
			// Nothing was sent, so release the claim; otherwise this
			// destination would never send again.
			d.Release()
			return
		}
		defer func() { <-m.sem }()
		d.Run(ctx)
	}()
}

// PendingTotals sums what is queued across every destination, for metrics.
func (m *Manager) PendingTotals() (destinations, pdus, edus int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, d := range m.dests {
		p, e := d.Pending()
		if p > 0 || e > 0 {
			destinations++
		}
		pdus += p
		edus += e
	}
	return destinations, pdus, edus
}
