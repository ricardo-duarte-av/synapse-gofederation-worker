package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// HTTP delivers transactions to real homeservers.
//
// Server discovery -- .well-known, then _matrix-fed._tcp SRV, then the legacy
// _matrix._tcp SRV, then port 8448, with the right Host header and TLS name at
// each step -- is mautrix's ServerResolvingTransport rather than ours. It is
// fiddly, security-relevant, and has nothing to do with what this worker is for.
//
// This type has no idea which destinations it is allowed to talk to. That is
// deliberate: the allowlist lives in Router, which simply never hands this an
// unapproved destination, so there is no flag inside here that could be read
// the wrong way. See docs/going-live.md.
type HTTP struct {
	client    *http.Client
	log       zerolog.Logger
	userAgent string

	retries  int
	maxDelay time.Duration
	timeout  time.Duration
	onSent   func(pdus, edus, bytes int)
	// onEDUTypes reports the EDU type breakdown of each transaction, which the
	// unlabelled counts cannot answer questions about. See countEDUTypes.
	onEDUTypes func(byType map[string]int, presenceStates int)
	capture    Capturer

	// observe reports one attempt's duration and outcome, and inFlight tracks
	// concurrency. Hooks rather than an import of the metrics package, so this
	// stays testable without a Prometheus registry.
	observe  func(outcome string, took time.Duration)
	inFlight func(delta int)
}

// HTTPConfig builds an HTTP sink.
type HTTPConfig struct {
	Log zerolog.Logger
	// UserAgent identifies us to remote servers. Ours, not Synapse's: a remote
	// operator looking at their logs should be able to tell which of our
	// senders they are talking to.
	UserAgent string
	// Timeout bounds one attempt. Synapse's federation.client_timeout, 10s on
	// this deployment.
	Timeout time.Duration
	// Retries and MaxDelay mirror Synapse's max_long_retries and
	// max_long_retry_delay for /send (2 and 10s here).
	Retries  int
	MaxDelay time.Duration
}

// NewHTTP builds a sink that really sends.
func NewHTTP(cfg HTTPConfig) *HTTP {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 3
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 60 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "synapse-gofederation-worker"
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	// Ours rather than mautrix's default cache. Both keep resolutions in
	// memory; the difference is the TTL, and mautrix keeps every resolution for
	// 24 hours however it was reached -- including one that fell back to port
	// 8448 because a delegating server's .well-known was briefly down. See
	// resolveCache.
	transport := federation.NewServerResolvingTransport(
		newResolveCache(),
		exhttp.DialerFunc(dialer.DialContext),
		exhttp.ClientSettings{
			// ClientSettings only carries what it has fields for, and applies
			// nothing it was not given, so an empty one leaves a bare
			// http.Transport. Both of these are zero by default there, and zero
			// means "no limit" for each -- see the pool tuning below.
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     90 * time.Second,
		},
	)

	// The connection pool, sized for a federation sender rather than for a
	// client talking to a handful of hosts.
	//
	// http.Transport's zero values are the wrong shape here in opposite
	// directions. MaxIdleConns of 0 means UNLIMITED idle connections, and with
	// an IdleConnTimeout of 0 they are never closed either, so a sender that
	// has spoken to ten thousand destinations holds ten thousand idle TLS
	// connections forever -- file descriptors and per-connection buffers that
	// are never returned. That is not hypothetical: this deployment was holding
	// 2,945 open descriptors after half an hour, climbing.
	//
	// MaxIdleConnsPerHost of 2 (the package default) is right and is set
	// explicitly so it is not mistaken for an oversight: one transaction at a
	// time per destination is a rule this worker already enforces, so a second
	// idle connection to one server is headroom, not throughput.
	transport.Transport.MaxIdleConns = 512
	transport.Transport.MaxIdleConnsPerHost = 2

	return &HTTP{
		client: &http.Client{
			Transport: transport,
			// Redirects are not part of the federation protocol, and following
			// one would send a signed transaction somewhere it was not
			// addressed to -- the signature names the destination.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: cfg.Timeout,
		},
		log:       cfg.Log,
		userAgent: cfg.UserAgent,
		retries:   cfg.Retries,
		maxDelay:  cfg.MaxDelay,
		timeout:   cfg.Timeout,
	}
}

// SetOnSent registers a callback invoked for every delivered transaction.
func (h *HTTP) SetOnSent(f func(pdus, edus, bytes int)) { h.onSent = f }

// SetOnEDUTypes registers a callback for the EDU type breakdown of each
// transaction.
func (h *HTTP) SetOnEDUTypes(f func(byType map[string]int, presenceStates int)) {
	h.onEDUTypes = f
}

// SetCapture registers a full-request recorder.
func (h *HTTP) SetCapture(c Capturer) { h.capture = c }

// SetObservers registers timing callbacks.
//
// Outcome is labelled because a fast failure and a fast success look identical
// in a duration histogram, and a remote that refuses instantly would otherwise
// read as excellent performance.
func (h *HTTP) SetObservers(observe func(outcome string, took time.Duration), inFlight func(delta int)) {
	h.observe, h.inFlight = observe, inFlight
}

// Mode identifies this sink.
func (h *HTTP) Mode() string { return "http (REALLY SENDING)" }

// Send delivers a transaction, retrying the way Synapse does.
func (h *HTTP) Send(ctx context.Context, req *txn.Request) (Result, error) {
	// Captured before the attempt, not after. What we SENT is the fact worth
	// recording; whether it arrived is a separate field, and a transaction
	// that failed to send is exactly the one you want the bytes of.
	if h.capture != nil && h.capture.Captures(req.Destination) {
		if err := h.capture.Capture(req); err != nil {
			h.log.Error().Err(err).Str("destination", req.Destination).
				Msg("failed to record transaction for comparison")
		}
	}

	var lastErr error
	rateLimitedAttempts := 0
	for attempt := 0; attempt <= h.retries; attempt++ {
		if attempt > 0 {
			delay := h.backoff(attempt)
			// A 429 is the remote asking us to stop, so the ordinary backoff is
			// the wrong schedule for it.
			//
			// This worker manufactures these. client_timeout is 10s on this
			// deployment, so a transaction the remote takes longer than that to
			// process becomes a timeout on our side while it is still being
			// worked on -- and the retry is then a SECOND transaction from the
			// same origin, which is exactly what the remote refuses with
			// "still processing another transaction from this origin". Ten more
			// attempts cannot help; only waiting can.
			//
			// So: honour retry_after_ms when the remote sends one, and give up
			// after maxRateLimitedAttempts rather than spending the full budget
			// hammering a server that has already answered. Giving up here
			// costs nothing extra -- the per-destination backoff grows on a 429
			// either way, which is Synapse's policy too (retryutils.py:258,
			// "429 is us being aggressively rate limited, so lets rate limit
			// ourselves") -- and it saves the remote eight requests it has
			// already declined.
			var ok bool
			if delay, ok = rateLimitDelay(delay, lastErr, &rateLimitedAttempts); !ok {
				return Result{}, fmt.Errorf(
					"sink: %s: rate limited, not retrying further: %w",
					req.Destination, lastErr)
			}
			h.log.Debug().
				Str("destination", req.Destination).Str("txn_id", req.TransactionID).
				Int("attempt", attempt).Dur("delay", delay).
				Msg("retrying transaction")
			select {
			case <-ctx.Done():
				return Result{}, ctx.Err()
			case <-time.After(delay):
			}
		}

		res, retryable, err := h.attempt(ctx, req)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !retryable || ctx.Err() != nil {
			return Result{}, err
		}
	}
	return Result{}, fmt.Errorf("sink: %s: giving up after %d attempts: %w",
		req.Destination, h.retries+1, lastErr)
}

// attempt makes one request. retryable says whether trying again could help.
func (h *HTTP) attempt(ctx context.Context, req *txn.Request) (Result, bool, error) {
	// The matrix-federation scheme is what tells the transport to resolve the
	// name rather than treat it as a host.
	url := "matrix-federation://" + req.Destination + req.Path

	// The body must be the exact bytes that were signed. bytes.NewReader over
	// req.Body, never a re-encode.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(req.Body))
	if err != nil {
		return Result{}, false, fmt.Errorf("sink: building request: %w", err)
	}
	httpReq.Header.Set("Authorization", req.AuthHeader)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", h.userAgent)
	httpReq.ContentLength = int64(len(req.Body))

	if h.inFlight != nil {
		h.inFlight(1)
		defer h.inFlight(-1)
	}
	started := time.Now()
	resp, err := h.client.Do(httpReq)
	if err != nil {
		h.observed("error", started)
		// A connection failure is worth retrying: the remote may be
		// restarting, and Synapse retries these too.
		return Result{}, true, fmt.Errorf("sink: %s: %w", req.Destination, err)
	}
	defer resp.Body.Close()

	// Bounded: a remote server's response is not something to trust with our
	// memory.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return Result{}, true, fmt.Errorf("sink: %s: reading response: %w", req.Destination, err)
	}

	if resp.StatusCode != http.StatusOK {
		h.observed("http_"+strconv.Itoa(resp.StatusCode), started)
		// Synapse retries 5xx and 429 and gives up on everything else
		// (matrixfederationclient.py:833). A 400 means the remote has made a
		// decision about this transaction and will make the same one again.
		retryable := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
		err := fmt.Errorf("sink: %s: %s: %s",
			req.Destination, resp.Status, truncate(body, 512))
		if resp.StatusCode == http.StatusTooManyRequests {
			return Result{}, retryable, &rateLimited{
				err:   err,
				after: retryAfter(resp, body),
			}
		}
		return Result{}, retryable, err
	}

	result := Result{Delivered: true, StatusCode: resp.StatusCode}
	// The response maps event id to {} or {"error": ...}. Synapse logs the
	// errors and never retries them: the remote has judged that event.
	if pdus := gjson.GetBytes(body, "pdus"); pdus.IsObject() {
		pdus.ForEach(func(key, value gjson.Result) bool {
			if msg := value.Get("error").String(); msg != "" {
				if result.PDUErrors == nil {
					result.PDUErrors = map[string]string{}
				}
				result.PDUErrors[key.String()] = msg
			}
			return true
		})
	}

	h.observed("ok", started)
	pdus, edus := countUnits(req.Body)
	if h.onEDUTypes != nil {
		h.onEDUTypes(countEDUTypes(req.Body), countPresenceStates(req.Body))
	}
	if h.onSent != nil {
		h.onSent(pdus, edus, len(req.Body))
	}
	h.log.Info().
		Str("destination", req.Destination).Str("txn_id", req.TransactionID).
		Int("pdus", pdus).Int("edus", edus).Int("bytes", len(req.Body)).
		Int("rejected_pdus", len(result.PDUErrors)).
		Msg("sent transaction")
	return result, false, nil
}

// backoff is Synapse's long-retry delay: 4^attempt, capped, with jitter
// (matrixfederationclient.py:833-885).
//
// The jitter is not decoration. Without it every queue that failed at the same
// moment retries at the same moment, and a remote coming back up is met by the
// whole backlog at once.
func (h *HTTP) backoff(attempt int) time.Duration {
	delay := time.Duration(1<<uint(2*attempt)) * time.Second
	if delay > h.maxDelay {
		delay = h.maxDelay
	}
	jitter := 0.8 + rand.Float64()*0.6
	return time.Duration(float64(delay) * jitter)
}

func (h *HTTP) observed(outcome string, since time.Time) {
	if h.observe != nil {
		h.observe(outcome, time.Since(since))
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// rateLimitDelay adjusts the retry schedule for a rate-limited attempt.
//
// Returns the delay to wait and whether to retry at all. For anything that is
// not a 429 it returns the ordinary backoff unchanged, so the schedule for a
// 5xx or a connection failure is untouched.
func rateLimitDelay(base time.Duration, lastErr error, seen *int) (time.Duration, bool) {
	var limited *rateLimited
	if !errors.As(lastErr, &limited) {
		return base, true
	}
	if *seen++; *seen >= maxRateLimitedAttempts {
		return 0, false
	}
	if limited.after > 0 {
		return limited.after, true
	}
	return base, true
}

// RateLimitedError is an error meaning the remote asked us to slow down.
//
// An interface rather than a concrete type so the concept is expressible
// outside this package -- by another sink, or by a test that needs to produce
// one without reaching into this package's internals. A contract only this file
// can satisfy is a contract only this file can be tested against.
type RateLimitedError interface {
	error
	// RetryAfter is how long the remote asked for, or zero if it gave no hint.
	RetryAfter() time.Duration
}

// IsRateLimited reports whether err is a remote asking us to slow down, and how
// long it asked for.
//
// The distinction matters: a 429 means the server is UP and talking to us,
// which is the opposite of what the per-destination backoff is for.
func IsRateLimited(err error) (time.Duration, bool) {
	var limited RateLimitedError
	if errors.As(err, &limited) {
		return limited.RetryAfter(), true
	}
	return 0, false
}

// maxRateLimitedAttempts is how many times a 429 is retried before giving up.
//
// Small on purpose. A remote that answers 429 has received the request and
// declined it; repeating it up to max_long_retries times is eleven requests to
// tell one server the same thing. The per-destination backoff is the mechanism
// that actually waits, and it grows on a 429 regardless.
const maxRateLimitedAttempts = 2

// rateLimited carries a 429's retry-after hint alongside its error.
type rateLimited struct {
	err   error
	after time.Duration
}

func (e *rateLimited) Error() string             { return e.err.Error() }
func (e *rateLimited) RetryAfter() time.Duration { return e.after }
func (e *rateLimited) Unwrap() error             { return e.err }

// retryAfter reads how long the remote asked us to wait.
//
// Two spellings, because both are in the wild: the HTTP Retry-After header in
// seconds, and retry_after_ms in an M_LIMIT_EXCEEDED body, which is the one the
// Matrix spec defines. Either may be absent -- the server that prompted this
// sends "retry_after: None" -- in which case the ordinary backoff stands.
//
// Capped, because this is a number a remote server chooses and a transaction
// must not be parked for an hour because somebody sent a silly one.
func retryAfter(resp *http.Response, body []byte) time.Duration {
	if ms := gjson.GetBytes(body, "retry_after_ms"); ms.Exists() && ms.Int() > 0 {
		return capRetryAfter(time.Duration(ms.Int()) * time.Millisecond)
	}
	if h := resp.Header.Get("Retry-After"); h != "" {
		if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
			return capRetryAfter(time.Duration(secs) * time.Second)
		}
	}
	return 0
}

func capRetryAfter(d time.Duration) time.Duration {
	const max = 60 * time.Second
	if d > max {
		return max
	}
	return d
}
