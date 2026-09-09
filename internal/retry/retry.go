// Package retry is the per-destination backoff a federation sender keeps.
//
// It matters more than it looks. A homeserver's destination list is mostly a
// graveyard: servers that were shut down years ago, domains that expired,
// hosts that will never answer again. On this deployment 27,824 destinations
// are known and a large share of them are permanently gone. Without a backoff
// that GROWS and PERSISTS, every one of them is retried on every event forever
// -- which is wasted work here and unsolicited traffic at the other end.
//
// This is Synapse's RetryDestinationLimiter (util/retryutils.py), including the
// parts that look arbitrary:
//
//   - The interval multiplies on each failure and is capped, so a dead server
//     is eventually tried twice a day rather than continuously.
//   - The jitter is uniform(0.8, 1.4). Without it every destination that failed
//     in the same incident retries in the same instant, and a server coming
//     back up is met by the whole backlog at once.
//   - A success clears the backoff completely rather than decaying it, because
//     one working request is enough evidence that the server is back.
package retry

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Timings is a destination's backoff state, in milliseconds.
type Timings struct {
	// FailureTS is when the CURRENT run of failures started, kept across
	// retries so "how long has this been broken" is answerable.
	FailureTS int64
	// RetryLastTS is when it was last attempted. Zero means never failed.
	RetryLastTS int64
	// RetryInterval is how long to wait after RetryLastTS.
	RetryInterval int64
}

// Due reports whether a destination may be attempted at nowMS.
//
// A destination with no RetryLastTS has never failed and is always due, which
// is Synapse's reading of a falsy retry_last_ts rather than an interpretation
// of it (retryutils.py:61).
func (t Timings) Due(nowMS int64) bool {
	if t.RetryLastTS == 0 {
		return true
	}
	return t.RetryLastTS+t.RetryInterval <= nowMS
}

// Config is the backoff tuning, read from Synapse's `federation:` block rather
// than chosen here -- the two senders should behave the same way about the same
// destinations.
type Config struct {
	MinInterval time.Duration
	Multiplier  float64
	MaxInterval time.Duration
}

// Loader reads persisted timings, and Persister writes them. Both are optional:
// a shadow has no persistence and keeps the backoff in memory for its own life.
type Loader func(ctx context.Context, destinations []string) (map[string]Timings, error)

// Persister writes a destination's timings.
type Persister func(ctx context.Context, destination string, t Timings) error

// Limiter decides whether a destination may be tried, and records what happened.
type Limiter struct {
	cfg     Config
	load    Loader
	persist Persister

	mu sync.RWMutex
	// state is the authority while the process runs. The database is where it
	// survives a restart, not where it is consulted on every send: a limiter
	// that hit the database before each attempt would put a query in front of
	// every transaction to every destination.
	state map[string]Timings
	// loaded marks destinations whose persisted state has been read, so the
	// first attempt after a restart does not treat a backing-off server as new.
	loaded map[string]bool

	// cooldown is when a destination that asked us to slow down may be tried
	// again. Deliberately separate from state: a 429 is not a failure and must
	// not touch the backoff that state holds.
	cooldown map[string]time.Time
	// cooldownFor is how long each destination's current cooldown is, so
	// repeated rate limiting lengthens it without ever compounding into the
	// backoff.
	cooldownFor map[string]time.Duration

	// onUp is called when a destination recovers, so a caller can wake its
	// queue and tell the rest of the cluster.
	onUp func(destination string)
}

// New builds a Limiter.
func New(cfg Config, load Loader, persist Persister) *Limiter {
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 10 * time.Minute
	}
	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 7 * 24 * time.Hour
	}
	return &Limiter{
		cfg: cfg, load: load, persist: persist,
		state: map[string]Timings{}, loaded: map[string]bool{},
		cooldown: map[string]time.Time{}, cooldownFor: map[string]time.Duration{},
	}
}

// SetOnRecovered registers a callback for a destination coming back.
func (l *Limiter) SetOnRecovered(f func(destination string)) { l.onUp = f }

// Due reports whether a destination may be attempted now, reading persisted
// state the first time it is asked about one.
func (l *Limiter) Due(ctx context.Context, destination string) bool {
	return l.DueWithin(ctx, destination, 0)
}

// Rate-limit cooldowns. Nothing like the destination backoff: this is for a
// server that is UP and asking us to be quieter, so it is measured in seconds,
// grows gently, is capped low, and is never persisted -- a cooldown that
// survived a restart would be a backoff by another name.
const (
	rateLimitBaseCooldown = 5 * time.Second
	rateLimitMaxCooldown  = 5 * time.Minute
)

// RateLimited records that a destination answered 429.
//
// It does NOT touch the backoff. The three failure cases a sender must tell
// apart are: no answer before the timeout, which means the host has problems;
// an HTTP error that is not a 429, which means the host cannot accept this; and
// a 429, which means the host is fine and we are being too aggressive. Only the
// first two are the destination's fault. Backing off on a 429 -- which is what
// Synapse does (retryutils.py:258), and what this worker used to do -- takes a
// server that is talking to us and, over a few collisions, writes it off for as
// long as the cap allows. On this deployment that cap is a year.
//
// after is the remote's own hint when it sent one, which is always preferred:
// it knows how busy it is and we do not.
func (l *Limiter) RateLimited(destination string, after time.Duration) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	wait := after
	if wait <= 0 {
		// No hint, so grow our own: repeated rate limiting means the last
		// interval was still too fast.
		wait = l.cooldownFor[destination] * 2
		if wait <= 0 {
			wait = rateLimitBaseCooldown
		}
	}
	if wait > rateLimitMaxCooldown {
		wait = rateLimitMaxCooldown
	}
	l.cooldownFor[destination] = wait
	l.cooldown[destination] = time.Now().Add(wait)
	return wait
}

// DueWithin reports whether a destination is due now or becomes due within the
// given slack.
//
// The slack is Synapse's retry_due_within_ms, which every one of its senders
// passes as CATCHUP_RETRY_INTERVAL -- one hour -- when deciding whether to
// bother building an EDU at all (federation/sender/__init__.py:838,915,994,1075).
// It is not a rounding convenience: without it a destination that comes back in
// two minutes is skipped now and not reconsidered until the next thing happens
// to be routed to it, so a recovering server waits far longer than its own
// backoff asked for.
func (l *Limiter) DueWithin(ctx context.Context, destination string, within time.Duration) bool {
	l.mu.RLock()
	t, known := l.state[destination]
	seeded := l.loaded[destination]
	l.mu.RUnlock()

	if !seeded {
		t = l.seed(ctx, destination)
	} else if !known {
		return l.offCooldown(destination)
	}
	if !t.Due(time.Now().UnixMilli() + within.Milliseconds()) {
		return false
	}
	return l.offCooldown(destination)
}

// offCooldown reports whether a rate-limit cooldown has passed.
//
// The slack that DueWithin applies to the backoff is deliberately NOT applied
// here. That slack exists so a destination about to come back is included in a
// batch rather than deferred; a cooldown is a promise to a server that is
// already up, and reaching for work a few minutes early would break it.
func (l *Limiter) offCooldown(destination string) bool {
	l.mu.RLock()
	until, ok := l.cooldown[destination]
	l.mu.RUnlock()
	return !ok || !time.Now().Before(until)
}

// seed reads a destination's persisted timings once.
//
// Without it a restart forgets every backoff, and the first event after a
// deploy retries thousands of dead servers at once -- which is the exact
// stampede the backoff exists to prevent, delivered on a schedule.
func (l *Limiter) seed(ctx context.Context, destination string) Timings {
	var t Timings
	if l.load != nil {
		if loaded, err := l.load(ctx, []string{destination}); err == nil {
			t = loaded[destination]
		}
		// A read failure is deliberately not fatal and not cached: the
		// destination is treated as due, and the next call tries again.
	}
	l.mu.Lock()
	if _, exists := l.state[destination]; !exists {
		l.state[destination] = t
	} else {
		t = l.state[destination]
	}
	l.loaded[destination] = true
	l.mu.Unlock()
	return t
}

// Timings returns what is currently known about a destination.
func (l *Limiter) Timings(destination string) Timings {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state[destination]
}

// Success clears a destination's backoff.
//
// Cleared outright rather than decayed: one working request is enough evidence
// that the server is back, and a decay would keep a recovered server throttled
// for as long as it had been broken.
func (l *Limiter) Success(ctx context.Context, destination string) {
	l.mu.Lock()
	previous := l.state[destination]
	l.state[destination] = Timings{}
	l.loaded[destination] = true
	// A delivered transaction means the pace is acceptable again.
	delete(l.cooldown, destination)
	delete(l.cooldownFor, destination)
	l.mu.Unlock()

	// Only act on an actual transition. Clearing an already-clear destination
	// on every successful send would write to the database on every
	// transaction and wake queues that were never asleep.
	if previous.RetryInterval == 0 && previous.RetryLastTS == 0 {
		return
	}
	if l.persist != nil {
		_ = l.persist(ctx, destination, Timings{})
	}
	if l.onUp != nil {
		l.onUp(destination)
	}
}

// Failure grows a destination's backoff and returns the new timings.
func (l *Limiter) Failure(ctx context.Context, destination string) Timings {
	now := time.Now().UnixMilli()

	l.mu.Lock()
	t := l.state[destination]
	if t.RetryInterval > 0 {
		// The jitter is applied to the GROWTH, as Synapse does, so two
		// destinations that failed together diverge instead of staying in
		// lockstep through every subsequent retry.
		grown := float64(t.RetryInterval) * l.cfg.Multiplier * (0.8 + rand.Float64()*0.6)
		if max := float64(l.cfg.MaxInterval.Milliseconds()); grown > max {
			grown = max
		}
		t.RetryInterval = int64(grown)
	} else {
		t.RetryInterval = l.cfg.MinInterval.Milliseconds()
	}
	t.RetryLastTS = now
	if t.FailureTS == 0 {
		// The start of this run of failures, not of this attempt.
		t.FailureTS = now
	}
	l.state[destination] = t
	l.loaded[destination] = true
	l.mu.Unlock()

	if l.persist != nil {
		_ = l.persist(ctx, destination, t)
	}
	return t
}

// Recovered clears a destination because somebody else saw it working.
//
// Synapse's REMOTE_SERVER_UP: another worker got a response, so our backoff is
// stale. It is not evidence WE can reach it, but retrying once and failing is
// cheap next to leaving a working server unreachable for hours.
func (l *Limiter) Recovered(ctx context.Context, destination string) {
	l.mu.Lock()
	t := l.state[destination]
	if t.RetryInterval == 0 && t.RetryLastTS == 0 {
		l.mu.Unlock()
		return
	}
	l.state[destination] = Timings{}
	l.loaded[destination] = true
	l.mu.Unlock()

	// Persisted, exactly as Success does. Clearing only in memory would mean a
	// server somebody else has seen working goes back into a long backoff on
	// our next restart, re-read from a row that recorded an outage which is
	// over -- and with this deployment's destination_max_retry_interval of
	// 365d, "next restart" can be a year of not trying.
	if l.persist != nil {
		_ = l.persist(ctx, destination, Timings{})
	}
	if l.onUp != nil {
		l.onUp(destination)
	}
}

// Backoff reports how many destinations are currently in a backoff, for
// metrics. A number that only grows is a homeserver slowly losing the network.
func (l *Limiter) Backoff() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	now := time.Now().UnixMilli()
	n := 0
	for _, t := range l.state {
		if !t.Due(now) {
			n++
		}
	}
	return n
}
