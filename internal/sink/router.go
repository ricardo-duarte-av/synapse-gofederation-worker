package sink

import (
	"context"
	"sort"
	"strings"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Router decides, per destination, whether a transaction is really sent or only
// logged.
//
// This is how "go live one server at a time" is implemented, and the shape is
// deliberate. The alternative -- a boolean inside the HTTP sender saying whether
// to actually send -- puts the decision next to the code that opens sockets,
// where a wrong answer means real traffic to real servers. Here the HTTP sender
// has no opinion and no allowlist: it is simply never handed a destination that
// is not approved, and every other destination goes to a sink with no network
// dependency at all.
//
// The default is deny. An empty allowlist sends nothing, and turning that into
// "send to everyone" takes an explicit, separately named option.
type Router struct {
	// allowed is the set of destinations that reach the live sink. Nil means
	// none, which is the safe reading of an unset option.
	allowed map[string]bool
	// all is set only by an explicit opt-in, never inferred from an empty
	// allowlist.
	all bool

	live Sink
	dry  Sink
	log  zerolog.Logger
}

// RouterConfig builds a Router.
type RouterConfig struct {
	// Allowed lists destinations that are really sent to.
	Allowed []string
	// All sends to every destination. Must be set explicitly; an empty Allowed
	// list does NOT mean this.
	All bool
	// Live is the sink for approved destinations, Dry for everything else.
	Live Sink
	Dry  Sink
	Log  zerolog.Logger
}

// NewRouter builds a Router.
func NewRouter(cfg RouterConfig) *Router {
	r := &Router{live: cfg.Live, dry: cfg.Dry, all: cfg.All, log: cfg.Log}
	if len(cfg.Allowed) > 0 {
		r.allowed = make(map[string]bool, len(cfg.Allowed))
		for _, d := range cfg.Allowed {
			r.allowed[strings.TrimSpace(d)] = true
		}
	}
	return r
}

// Sends reports whether a destination is really sent to.
func (r *Router) Sends(destination string) bool {
	if r.all {
		return true
	}
	return r.allowed[destination]
}

// Mode describes the routing, for startup logging.
//
// Spelled out rather than summarised, because this single line is what an
// operator reads to answer "is this thing putting traffic on the internet?".
func (r *Router) Mode() string {
	switch {
	case r.all:
		return "http (REALLY SENDING to ALL destinations)"
	case len(r.allowed) == 0:
		return "dry-run (no destination is on the send allowlist)"
	default:
		names := make([]string, 0, len(r.allowed))
		for d := range r.allowed {
			names = append(names, d)
		}
		sort.Strings(names)
		return "REALLY SENDING to " + strings.Join(names, ", ") +
			"; every other destination is dry-run"
	}
}

// Allowed returns the allowlist, sorted, for logging.
func (r *Router) Allowed() []string {
	names := make([]string, 0, len(r.allowed))
	for d := range r.allowed {
		names = append(names, d)
	}
	sort.Strings(names)
	return names
}

// Send routes one transaction.
func (r *Router) Send(ctx context.Context, req *txn.Request) (Result, error) {
	if r.Sends(req.Destination) {
		return r.live.Send(ctx, req)
	}
	return r.dry.Send(ctx, req)
}
