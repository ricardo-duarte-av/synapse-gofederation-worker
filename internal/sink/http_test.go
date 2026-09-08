package sink

import (
	"testing"

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
