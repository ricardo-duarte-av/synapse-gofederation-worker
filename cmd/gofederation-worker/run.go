package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/difflog"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
)

func (w *worker) registerMetrics() {
	metrics.MustRegister()
	metrics.BuildInfo.WithLabelValues(tag, commit, buildTime).Set(1)
	if w.cfg.ShadowEnabled() {
		metrics.ShadowMode.Set(1)
	} else {
		metrics.ShadowMode.Set(0)
	}
	// The shadow collector reads the persisted totals on each scrape, so it is
	// only registered when there are totals to read.
	if w.diff != nil {
		prometheus.MustRegister(difflog.NewCollector(w.diff))
	}
}

// run starts every long-lived task and returns when the first one stops.
func (w *worker) run(ctx context.Context) error {
	key, _ := w.cfg.Synapse.SigningKey()
	w.log.Info().
		Str("version", tag).
		Str("server_name", w.cfg.ServerName).
		Str("worker_name", w.cfg.WorkerName).
		Str("shadowing", w.cfg.ShardInstance).
		Strs("senders", w.cfg.Synapse.SenderInstances).
		Str("signing_key", key.ID()).
		Str("redis", w.cfg.RedisAddress).
		Str("channel", w.cfg.RedisChannel).
		Bool("shadow", w.cfg.ShadowEnabled()).
		Str("sink", w.sinkMode()).
		Msg("starting")

	// Said plainly and at info level, because the one thing an operator must
	// never be unsure about is whether this process is putting federation
	// traffic on the internet.
	switch {
	case w.cfg.ShadowEnabled():
		w.log.Info().Msg(
			"SHADOW MODE: transactions will be built and signed but never sent, " +
				"and no Synapse table will be written")
	case w.cfg.Shadow.SendToAll:
		w.log.Warn().Msg(
			"LIVE: sending real federation traffic to EVERY destination in this shard")
	default:
		w.log.Warn().
			Strs("destinations", w.router.Allowed()).
			Msg("LIVE: sending real federation traffic to the allowlisted destinations only; " +
				"every other destination is still dry-run")
	}

	// Bind the queues to the WORKER's lifetime, not to any batch's. A
	// transmission loop waits on remote servers long after the batch that
	// queued its work has finished.
	w.queues.Start(ctx)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		w.sub.Run(gctx)
		return gctx.Err()
	})
	g.Go(func() error { return w.sender.Run(gctx) })
	g.Go(func() error { return w.sampleGauges(gctx) })

	if w.cfg.Metrics.Addr != "" {
		g.Go(func() error { return w.serveMetrics(gctx) })
	}

	err := g.Wait()
	if errors.Is(err, context.Canceled) {
		w.log.Info().Msg("shutting down")
		return nil
	}
	return err
}

// sampleGauges refreshes the values that are cheaper to sample than to track,
// and flushes the shadow record.
//
// Sampling the queue depths rather than incrementing counters means a leak in
// the queues shows up even when the counters that should have caught it are
// themselves the buggy part.
func (w *worker) sampleGauges(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Flush on the way out so the last window is not lost.
			if w.diff != nil {
				_ = w.diff.Flush()
			}
			return ctx.Err()
		case <-ticker.C:
			dests, pdus, edus := w.queues.PendingTotals()
			metrics.QueuedDestinations.Set(float64(dests))
			metrics.QueuedPDUs.Set(float64(pdus))
			metrics.QueuedEDUs.Set(float64(edus))
			metrics.KnownDestinations.Set(float64(w.queues.Count()))
			metrics.DestinationsBackingOff.Set(float64(w.limiter.Backoff()))

			if w.sub.Live() {
				metrics.ReplicationLive.Set(1)
			} else {
				metrics.ReplicationLive.Set(0)
			}
			for stream, pos := range w.sub.Positions() {
				metrics.StreamPosition.WithLabelValues(stream).Set(float64(pos))
			}

			if w.diff != nil {
				if err := w.diff.Flush(); err != nil {
					w.log.Error().Err(err).Msg("failed to write the shadow record")
				}
			}
		}
	}
}

func (w *worker) serveMetrics(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// The healthcheck reports liveness, not readiness: a worker whose Redis
	// subscription has dropped is reconnecting, and restarting it would not
	// help.
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              w.cfg.Metrics.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	w.log.Info().Str("addr", w.cfg.Metrics.Addr).Msg("metrics listening")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return ctx.Err()
}

// sinkMode describes where transactions go, for the startup line.
func (w *worker) sinkMode() string {
	if w.router != nil {
		return w.router.Mode()
	}
	return "dry-run (shadow; nothing is sent)"
}
