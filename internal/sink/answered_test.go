package sink

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// stubbed is an HTTP sink whose every request is answered by rt, with retries
// short enough to run the whole Send path.
func stubbed(rt roundTripFunc) *HTTP {
	h := NewHTTP(HTTPConfig{Log: zerolog.Nop(), Retries: 1, MaxDelay: time.Millisecond})
	h.client.Transport = rt
	return h
}

func answer(code int, body string) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: code, Status: http.StatusText(code),
			Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}, Request: r,
		}, nil
	}
}

// Whether the host answered decides what REMOTE_SERVER_UP may do to its
// backoff, so it has to survive the whole Send path -- including the retry
// loop, which wraps the last attempt's error in "giving up after N attempts".
func TestAnsweredTellsARefusalFromSilence(t *testing.T) {
	cases := []struct {
		name string
		rt   roundTripFunc
		want bool
	}{
		// itcalc.eu: Apache in front of /send.
		{"403 from a proxy", answer(http.StatusForbidden, "<h1>Forbidden</h1>"), true},
		// thicket.au: a server that does not route /send.
		{"404 unrecognized", answer(http.StatusNotFound, `{"errcode":"M_UNRECOGNIZED"}`), true},
		// matrix.joshpatra.me: retried, then given up on.
		{"502 after retries", answer(http.StatusBadGateway, "Bad Gateway"), true},
		{"no answer", func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp: i/o timeout")
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := stubbed(c.rt).Send(context.Background(), &txn.Request{
				Destination: "remote.example", Path: "/_matrix/federation/v1/send/1",
				Body: []byte(`{}`),
			})
			if err == nil {
				t.Fatal("the send succeeded")
			}
			if got := Answered(err); got != c.want {
				t.Errorf("Answered(%v) = %t, want %t", err, got, c.want)
			}
		})
	}
}
