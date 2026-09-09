// Command fedrecorder sits in front of a test homeserver and records the
// federation transactions it receives.
//
// It is the ground truth for the shadow comparison: what Synapse ACTUALLY put
// on the wire, headers and all, rather than what we reconstruct it must have
// sent. Deploy it between the reverse proxy and a real Synapse acting as the
// test destination:
//
//	nginx (TLS, testing.example.com) -> fedrecorder -> test Synapse
//
// A real Synapse rather than a stub receiver on purpose. A stub accepts
// anything, so it would confirm nothing when we eventually send for real; a
// real one validates our signatures and PDU format and rejects us loudly if we
// are wrong, which is the entire point of a canary.
//
// The proxy is deliberately transparent. It records and forwards; it never
// rewrites a request, and a recording failure never fails a request. A test
// homeserver that stops working because its instrumentation broke would be
// worse than no instrumentation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/capture"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/logfmt"
)

var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

// sendPathPrefix is the only path whose body is recorded. Everything else --
// key lookups, joins, state requests -- is forwarded untouched and unrecorded.
const sendPathPrefix = "/_matrix/federation/v1/send/"

// maxBodyBytes bounds a recorded body. A transaction is at most 50 PDUs, each
// capped at 64 KiB by the spec, so this is generous; it exists so a malformed
// or hostile request cannot exhaust memory before it reaches the homeserver
// that would have rejected it.
const maxBodyBytes = 32 << 20

func main() {
	var (
		listen      = flag.String("listen", ":8449", "address to listen on")
		upstream    = flag.String("upstream", "", "base URL of the test homeserver, e.g. http://synapse:8008")
		out         = flag.String("out", "captures/synapse.jsonl", "capture file")
		originOnly  = flag.String("origin", "", "record only transactions from this server (recommended)")
		maxBytes    = flag.Int64("max-bytes", 256<<20, "rotate the capture file past this size")
		generations = flag.Int("generations", 3, "rotated capture files to keep")
		pretty      = flag.Bool("pretty", false, "human-readable logs")
		showVersion = flag.Bool("version", false, "print build information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("fedrecorder %s (%s, built %s)\n", tag, commit, buildTime)
		return
	}
	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "fedrecorder: -upstream is required")
		os.Exit(1)
	}

	if err := run(*listen, *upstream, *out, *originOnly, *maxBytes, *generations, *pretty); err != nil {
		fmt.Fprintf(os.Stderr, "fedrecorder: %v\n", err)
		os.Exit(1)
	}
}

func run(listen, upstream, out, originOnly string, maxBytes int64, generations int, pretty bool) error {
	log := newLogger(pretty)

	target, err := url.Parse(upstream)
	if err != nil {
		return fmt.Errorf("parsing -upstream: %w", err)
	}

	w, err := capture.Open(capture.Config{
		Path: out, MaxBytes: maxBytes, Generations: generations,
	})
	if err != nil {
		return err
	}
	defer w.Close()

	rec := &recorder{
		writer: w, log: log, originOnly: originOnly,
		proxy: newProxy(target, log),
	}

	srv := &http.Server{
		Addr:    listen,
		Handler: rec,
		// Federation transactions can be large and remote senders slow, so the
		// read timeout is generous; the header timeout is not, since a stalled
		// header is never legitimate.
		ReadHeaderTimeout: 20 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Info().
		Str("listen", listen).Str("upstream", upstream).Str("capture", out).
		Str("origin_filter", originOnly).
		Msg("recording federation transactions")

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	written, dropped := w.Stats()
	log.Info().Int64("recorded", written).Int64("dropped", dropped).Msg("stopped")
	return nil
}

func newProxy(target *url.URL, log zerolog.Logger) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)
	p.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, err error) {
		// The upstream being unreachable is the test homeserver's problem, not
		// the sender's; report it as a gateway error so the sender backs off
		// the way it would against any unhealthy server.
		log.Error().Err(err).Msg("upstream unreachable")
		rw.WriteHeader(http.StatusBadGateway)
	}
	return p
}

func newLogger(pretty bool) zerolog.Logger {
	var l zerolog.Logger
	if pretty {
		l = zerolog.New(logfmt.New(os.Stderr))
	} else {
		l = zerolog.New(os.Stderr)
	}
	return l.With().Timestamp().Logger()
}

type recorder struct {
	writer     *capture.Writer
	proxy      *httputil.ReverseProxy
	log        zerolog.Logger
	originOnly string
}

func (rec *recorder) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, sendPathPrefix) {
		rec.proxy.ServeHTTP(rw, r)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, maxBodyBytes))
	if err != nil {
		// Reading failed, so the request cannot be forwarded either -- the
		// bytes are gone. Report it rather than passing on a truncated body.
		rec.log.Error().Err(err).Msg("reading transaction body")
		http.Error(rw, "cannot read request body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// Forward the EXACT bytes that were read. Re-encoding would invalidate the
	// signature the sender computed over them, and the homeserver would reject
	// a transaction that was in fact correct.
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	r.ContentLength = int64(len(body))

	capturing := rec.shouldRecord(body)
	sw := &statusWriter{ResponseWriter: rw, status: http.StatusOK}
	rec.proxy.ServeHTTP(sw, r)

	if capturing {
		rec.record(r, body, sw.status)
	}
}

// shouldRecord applies the origin filter, reading the origin from the BODY.
//
// The body is what was signed. The Authorization header carries an origin too,
// and taking it from there would let an unsigned header decide what we record.
func (rec *recorder) shouldRecord(body []byte) bool {
	if rec.originOnly == "" {
		return true
	}
	return gjson.GetBytes(body, "origin").String() == rec.originOnly
}

func (rec *recorder) record(r *http.Request, body []byte, status int) {
	txnID := strings.TrimPrefix(r.URL.Path, sendPathPrefix)
	if unescaped, err := url.PathUnescape(txnID); err == nil {
		txnID = unescaped
	}

	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}

	err := rec.writer.Write(capture.Record{
		Time:   time.Now().UTC(),
		Source: capture.SourceSynapse,
		// The destination is US: we are the server being sent to. Recorded
		// from the Host header so a capture taken through a differently-named
		// front end still says which name was used.
		Destination: hostOf(r),
		Origin:      gjson.GetBytes(body, "origin").String(),
		TxnID:       txnID,
		Path:        path,
		Auth:        r.Header.Get("Authorization"),
		Body:        body,
		Status:      status,
	})
	if err != nil {
		// Never fail the request over a recording failure: a test homeserver
		// that stops working because its instrumentation broke is worse than
		// no instrumentation.
		rec.log.Error().Err(err).Msg("recording transaction")
		return
	}
	rec.log.Debug().
		Str("origin", gjson.GetBytes(body, "origin").String()).
		Str("txn_id", txnID).
		Int("pdus", len(gjson.GetBytes(body, "pdus").Array())).
		Int("edus", len(gjson.GetBytes(body, "edus").Array())).
		Int("status", status).
		Msg("recorded transaction")
}

func hostOf(r *http.Request) string {
	// X-Forwarded-Host is set by the reverse proxy in front of us and names
	// what the sender actually addressed.
	if h := r.Header.Get("X-Forwarded-Host"); h != "" {
		return h
	}
	if host, _, err := net.SplitHostPort(r.Host); err == nil {
		return host
	}
	return r.Host
}

// statusWriter remembers the status the upstream returned, which is part of the
// evidence: a transaction Synapse sent and the test server REJECTED is a
// different fact from one it accepted.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}
