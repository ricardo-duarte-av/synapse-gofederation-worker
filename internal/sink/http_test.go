package sink

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/federation"
)

// http.Transport's zero values are the wrong shape for a federation sender in
// opposite directions: MaxIdleConns of 0 means UNLIMITED idle connections, and
// IdleConnTimeout of 0 means they are never closed. A sender that has spoken to
// ten thousand destinations then holds ten thousand idle TLS connections for
// the life of the process. Observed: 2,945 open descriptors after half an hour,
// still climbing.
//
// exhttp.ClientSettings applies only the fields it was given, so passing an
// empty one -- which reads like "defaults" -- leaves every one of these unset.
func TestHTTPSinkBoundsItsConnectionPool(t *testing.T) {
	h := NewHTTP(HTTPConfig{Log: zerolog.Nop()})
	rt, ok := h.client.Transport.(*federation.ServerResolvingTransport)
	if !ok {
		t.Fatalf("transport is %T, not the resolving transport", h.client.Transport)
	}

	if rt.Transport.MaxIdleConns == 0 {
		t.Error("MaxIdleConns is 0, which means unlimited idle connections")
	}
	if rt.Transport.IdleConnTimeout == 0 {
		t.Error("IdleConnTimeout is 0, so idle connections are never closed")
	}
	if rt.Transport.MaxIdleConnsPerHost > 4 {
		t.Errorf("MaxIdleConnsPerHost = %d; one transaction at a time per "+
			"destination makes anything larger dead weight across thousands of hosts",
			rt.Transport.MaxIdleConnsPerHost)
	}
	if rt.Transport.TLSHandshakeTimeout == 0 {
		t.Error("TLSHandshakeTimeout is 0, so a stalled handshake is bounded only " +
			"by the whole-request timeout")
	}
}

// A 429 means the remote received the request and declined it. Repeating it up
// to max_long_retries times is eleven requests to tell one server the same
// thing, and it is how this worker manufactured its own rate limiting: with
// client_timeout at 10s, a transaction the remote takes longer to process times
// out on our side while still being worked on, and the retry is then a SECOND
// transaction from the same origin, which is what the remote refuses with
// "still processing another transaction from this origin".
func TestRateLimitedGivesUpQuickly(t *testing.T) {
	limited := &rateLimited{err: errors.New("429"), after: 0}
	seen := 0
	base := 5 * time.Second

	// The first rate-limited retry is allowed.
	if d, ok := rateLimitDelay(base, limited, &seen); !ok || d != base {
		t.Fatalf("first retry: delay=%v ok=%t, want %v true", d, ok, base)
	}
	// The second gives up rather than spending the whole budget.
	if _, ok := rateLimitDelay(base, limited, &seen); ok {
		t.Errorf("kept retrying a server that has already said no %d times", seen)
	}
}

// The schedule for everything else must be untouched: a 5xx or a connection
// failure still gets the full retry budget, which is what Synapse does.
func TestNonRateLimitedRetriesAreUnchanged(t *testing.T) {
	seen := 0
	base := 4 * time.Second
	for i := 0; i < 10; i++ {
		d, ok := rateLimitDelay(base, errors.New("connection refused"), &seen)
		if !ok || d != base {
			t.Fatalf("attempt %d: delay=%v ok=%t, want %v true", i, d, ok, base)
		}
	}
	if seen != 0 {
		t.Errorf("counted %d rate-limited attempts for errors that were not 429s", seen)
	}
}

// When the remote says how long it needs, that wins over our own backoff.
func TestRetryAfterOverridesTheBackoff(t *testing.T) {
	seen := 0
	limited := &rateLimited{err: errors.New("429"), after: 2500 * time.Millisecond}
	d, ok := rateLimitDelay(5*time.Second, limited, &seen)
	if !ok {
		t.Fatal("gave up on the first rate-limited retry")
	}
	if d != 2500*time.Millisecond {
		t.Errorf("delay = %v, want the remote's 2.5s hint", d)
	}
}

// The spec's spelling is retry_after_ms in an M_LIMIT_EXCEEDED body. Honouring
// it is the whole of the netiquette: the remote has said how long it needs.
func TestRetryAfterIsHonoured(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		body   string
		want   time.Duration
	}{
		{"retry_after_ms", "", `{"retry_after_ms":2500}`, 2500 * time.Millisecond},
		{"Retry-After header", "3", `{}`, 3 * time.Second},
		{"body wins over header", "3", `{"retry_after_ms":1000}`, time.Second},
		{"absent", "", `{"errcode":"M_LIMIT_EXCEEDED"}`, 0},
		{"capped", "", `{"retry_after_ms":9999999}`, 60 * time.Second},
		{"nonsense header ignored", "soon", `{}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			if tc.header != "" {
				resp.Header.Set("Retry-After", tc.header)
			}
			if got := retryAfter(resp, []byte(tc.body)); got != tc.want {
				t.Errorf("retryAfter = %v, want %v", got, tc.want)
			}
		})
	}
}
