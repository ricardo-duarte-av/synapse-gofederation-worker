// Command gofederation-worker is a Synapse federation sender written in Go.
//
// It runs as a SHADOW of an existing federation sender by default: it consumes
// the same replication stream, applies the same sharding, resolves the same
// destinations, assembles and signs the same transactions -- and then logs them
// instead of sending. Nothing is written to any Synapse table and nothing is
// published to Redis. See docs/shadow-safety.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/capture"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/config"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/difflog"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/replication"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/retry"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sender"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Stamped by the build. The same three names as the sibling workers.
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	var (
		configPath  = flag.String("config", "gofederation-worker.yaml", "path to the configuration file")
		check       = flag.Bool("check", false, "validate the configuration and connections, then exit")
		healthcheck = flag.Bool("healthcheck", false, "probe a running worker and exit")
		showVersion = flag.Bool("version", false, "print build information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("gofederation-worker %s (%s, built %s)\n", tag, commit, buildTime)
		return
	}

	if err := run(*configPath, *check, *healthcheck); err != nil {
		fmt.Fprintf(os.Stderr, "gofederation-worker: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string, check, healthcheck bool) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if healthcheck {
		return probe(cfg)
	}

	log := newLogger(cfg)

	scfg, err := synapsecfg.LoadWithOptions(cfg.SynapseConfig,
		synapsecfg.Options{SigningKeyPath: cfg.SigningKeyPath})
	if err != nil {
		return err
	}
	resolved, err := config.Resolve(cfg, scfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w, err := newWorker(ctx, resolved, log)
	if err != nil {
		return err
	}
	defer w.close()

	if check {
		log.Info().Msg("configuration and connections are valid")
		return nil
	}

	return w.run(ctx)
}

func newLogger(cfg *config.Config) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Log.Level)
	if err != nil || cfg.Log.Level == "" {
		level = zerolog.InfoLevel
	}
	var l zerolog.Logger
	if cfg.Log.Pretty {
		l = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		l = zerolog.New(os.Stderr)
	}
	return l.Level(level).With().Timestamp().Logger()
}

// probe is the container healthcheck. The image is distroless and has no curl,
// so the binary probes itself.
func probe(cfg *config.Config) error {
	if cfg.Metrics.Addr == "" {
		// Nothing to probe. A worker with no metrics listener is still a valid
		// configuration, so this is success rather than failure.
		return nil
	}
	addr := cfg.Metrics.Addr
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", resp.Status)
	}
	return nil
}

// worker owns everything with a lifetime.
type worker struct {
	cfg *config.Resolved
	log zerolog.Logger

	db *store.Store
	// writer is nil in shadow mode, which is what guarantees a shadow has no
	// writable handle to Synapse's tables anywhere in the process.
	writer  *store.Writer
	cursors *state.Store
	diff    *difflog.Writer
	capture *capture.WorkerCapture
	// router is set only outside shadow mode; it owns the send allowlist.
	router *sink.Router
	// limiter is the per-destination backoff, shared by the routing filter and
	// the gate in front of the transmission loop. One instance, because two
	// implementations of a backoff would disagree under exactly the conditions
	// that make a backoff matter.
	limiter   *retry.Limiter
	queues    *queue.Manager
	sender    *sender.Sender
	devices   *sender.Devices
	catchup   *sender.CatchUp
	ephemeral *sender.Ephemeral
	sub       *replication.Subscriber
	metrics   *http.Server
}

func newWorker(ctx context.Context, cfg *config.Resolved, log zerolog.Logger) (*worker, error) {
	w := &worker{cfg: cfg, log: log}

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var err error
	if w.db, err = store.Open(openCtx, store.Config{
		DSN:            cfg.DatabaseDSN,
		MaxConns:       cfg.Database.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout(),
	}); err != nil {
		return nil, err
	}

	if cfg.DatabaseFromSynapse {
		log.Info().Str("dsn", cfg.Synapse.Database.Redacted()).
			Msg("connected to Synapse's database as configured in homeserver.yaml")
	}

	if err := w.checkReadOnly(openCtx); err != nil {
		w.close()
		return nil, err
	}

	// The writable pool exists only in primary mode. Constructed here rather
	// than lazily, so a role that cannot actually write is a startup failure
	// rather than a stream of silent errors hours later.
	if cfg.Mode == config.ModePrimary {
		if w.writer, err = store.OpenWriter(openCtx, store.Config{
			DSN:            cfg.WriteDSN,
			MaxConns:       cfg.Database.MaxConns,
			ConnectTimeout: cfg.ConnectTimeout(),
		}); err != nil {
			w.close()
			return nil, err
		}
		if err := w.writer.CanWrite(openCtx); err != nil {
			w.close()
			return nil, err
		}
		log.Info().Msg("primary mode: this worker is the homeserver's federation sender " +
			"and keeps its own bookkeeping in Synapse's tables")
	}

	if w.cursors, err = state.Open(openCtx, state.Config{
		DSN:            cfg.StateDSN,
		Table:          cfg.State.Table,
		RoutesTable:    cfg.State.RoutesTable,
		InstanceName:   cfg.WorkerName,
		MaxConns:       4,
		ConnectTimeout: cfg.ConnectTimeout(),
	}); err != nil {
		w.close()
		return nil, err
	}

	if cfg.ShadowEnabled() {
		if w.diff, err = difflog.Open(difflog.Config{
			Dir:            cfg.Shadow.DiffLogDir,
			Instance:       cfg.WorkerName,
			SampleEvery:    1000,
			MaxSampleBytes: 64 << 20,
		}); err != nil {
			w.close()
			return nil, err
		}
	}

	key, err := cfg.Synapse.SigningKey()
	if err != nil {
		w.close()
		return nil, err
	}
	signer, err := txn.NewSigner(cfg.ServerName,
		fmt.Sprintf("%s %s %s", key.Algorithm, key.Version, encodeSeed(key.Seed)))
	if err != nil {
		w.close()
		return nil, err
	}

	// The switch. In shadow mode the HTTP sink is never constructed, so there
	// is no client in the process that a bug could reach.
	if len(cfg.Shadow.CaptureDestinations) > 0 {
		cw, err := capture.Open(capture.Config{
			Path:         cfg.Shadow.CaptureFile,
			Destinations: cfg.Shadow.CaptureDestinations,
		})
		if err != nil {
			w.close()
			return nil, err
		}
		w.capture = capture.NewWorkerCapture(cw, cfg.ServerName)
		log.Info().
			Strs("destinations", cfg.Shadow.CaptureDestinations).
			Str("file", cfg.Shadow.CaptureFile).
			Msg("recording full transactions for comparison")
	}

	// The dry-run sink always exists: it is where every destination that is not
	// on the send allowlist goes, including in live mode.
	dry := sink.NewDryRun(log)
	dry.SetOnSent(w.countTransaction)
	if w.capture != nil {
		dry.SetCapture(w.capture)
	}

	var out sink.Sink = dry
	if !cfg.ShadowEnabled() {
		live := sink.NewHTTP(sink.HTTPConfig{
			Log:       log,
			UserAgent: "synapse-gofederation-worker/" + tag,
			Timeout:   cfg.Synapse.ClientTimeout,
			Retries:   cfg.Synapse.MaxLongRetries,
			MaxDelay:  cfg.Synapse.MaxLongRetryDelay,
		})
		live.SetOnSent(w.countTransaction)
		live.SetObservers(
			func(outcome string, took time.Duration) {
				metrics.SendDuration.WithLabelValues(outcome).Observe(took.Seconds())
			},
			func(delta int) { metrics.InFlightSends.Add(float64(delta)) },
		)
		if w.capture != nil {
			live.SetCapture(w.capture)
		}
		router := sink.NewRouter(sink.RouterConfig{
			Allowed: cfg.Shadow.SendOnlyTo,
			All:     cfg.Shadow.SendToAll,
			Live:    live,
			Dry:     dry,
			Log:     log,
		})
		w.router = router
		out = router
	}

	// The persistent backoff. Seeded from whichever table holds it -- Synapse's
	// while shadowing, ours once primary -- so a restart does not forget that
	// a quarter of the destination list has been dead for years.
	w.limiter = retry.New(
		retry.Config{
			MinInterval: cfg.Synapse.Retry.MinInterval,
			Multiplier:  cfg.Synapse.Retry.Multiplier,
			MaxInterval: cfg.Synapse.Retry.MaxInterval,
		},
		func(ctx context.Context, dests []string) (map[string]retry.Timings, error) {
			timings, err := w.db.GetDestinationRetryTimings(ctx, dests)
			if err != nil {
				return nil, err
			}
			out := make(map[string]retry.Timings, len(timings))
			for d, t := range timings {
				out[d] = retry.Timings{
					FailureTS: t.FailureTS, RetryLastTS: t.RetryLastTS,
					RetryInterval: t.RetryInterval,
				}
			}
			return out, nil
		},
		w.persistTimings,
	)
	w.limiter.SetOnRecovered(func(destination string) {
		log.Info().Str("destination", destination).Msg("destination recovered; backoff cleared")
	})

	w.queues = queue.NewManager(queue.ManagerConfig{
		Limits: queue.Limits{
			MaxPDUs: cfg.Queue.MaxPDUsPerTransaction,
			MaxEDUs: cfg.Queue.MaxEDUsPerTransaction,
		},
		Signer:        signer,
		IDs:           txn.NewIDGenerator(cfg.TransactionIDPrefix()),
		Sink:          out,
		Log:           log,
		MaxConcurrent: cfg.Queue.MaxConcurrentDestinations,
		OnSuccess:     w.onDelivered,
		OnEDUsSent:    w.onEDUsDelivered,
		OnOutcome:     w.onSendOutcome,
		Due: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.Due(ctx, destination)
		},
	})

	w.sender = sender.New(sender.Config{
		Store:          w.db,
		Cursors:        w.cursors,
		Resolver:       destinations.NewResolver(w.db, cfg.ServerName, cfg.Synapse.DomainWhitelist),
		Queues:         w.queues,
		Observer:       &observer{diff: w.diff},
		Log:            log,
		ServerName:     cfg.ServerName,
		ShouldHandle:   cfg.ShouldHandle,
		BatchLimit:     cfg.Queue.EventBatchLimit,
		RecordRoutes:   w.recordRoutes,
		RecordPosition: w.recordPosition,
	})

	w.devices = sender.NewDevices(sender.DevicesConfig{
		Store:                 w.db,
		Log:                   log,
		Cursors:               w.cursors,
		Queues:                w.queues,
		ShouldHandle:          cfg.ShouldHandle,
		AllowDeviceNameLookup: cfg.Synapse.AllowDeviceNameLookup,
	})

	w.ephemeral = sender.NewEphemeral(sender.EphemeralConfig{
		Store:        w.db,
		Queues:       w.queues,
		Log:          log,
		ServerName:   cfg.ServerName,
		ShouldHandle: cfg.ShouldHandle,
	})

	w.catchup = sender.NewCatchUp(sender.CatchUpConfig{
		Store:        w.db,
		Queues:       w.queues,
		Log:          log,
		ShouldHandle: cfg.ShouldHandle,
		Due: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.Due(ctx, destination)
		},
	})

	w.sub = replication.New(replication.Config{
		Enabled:      cfg.ReplicationEnabled(),
		Address:      cfg.RedisAddress,
		Channel:      cfg.RedisChannel,
		Password:     cfg.Replication.Password,
		DB:           cfg.Replication.DB,
		InstanceName: cfg.WorkerName,
	}, log, &handler{worker: w, log: log})

	w.registerMetrics()
	return w, nil
}

func (w *worker) checkReadOnly(ctx context.Context) error {
	role, err := w.db.CurrentRole(ctx)
	if err != nil {
		return err
	}
	readOnly, err := w.db.IsReadOnly(ctx)
	if err != nil {
		return err
	}
	if readOnly {
		metrics.DatabaseReadOnly.Set(1)
	} else {
		metrics.DatabaseReadOnly.Set(0)
	}

	if readOnly {
		w.log.Info().Str("role", role).Msg("Synapse database connection is read-only")
		return nil
	}
	if w.cfg.Database.RequireReadOnly {
		return fmt.Errorf(
			"database role %q can write to Synapse's tables and database.require_read_only "+
				"is set; see deploy/readonly-role.sql", role)
	}
	// A warning rather than a refusal, because a scratch database is a
	// legitimate way to develop against this. In production, set
	// require_read_only -- which means naming a read-only role in
	// database.dsn, since homeserver.yaml can only ever give us Synapse's own.
	msg := "the Synapse database role can WRITE; production should use a read-only role " +
		"(deploy/readonly-role.sql) and set database.require_read_only"
	if w.cfg.DatabaseFromSynapse {
		msg = "the Synapse database connection comes from homeserver.yaml, so it is " +
			"Synapse's own read-write role; a shadow should name a read-only role in " +
			"database.dsn (deploy/readonly-role.sql)"
	}
	w.log.Warn().Str("role", role).Msg(msg)
	return nil
}

func (w *worker) close() {
	if w.diff != nil {
		if err := w.diff.Close(); err != nil {
			w.log.Error().Err(err).Msg("failed to flush the shadow record")
		}
	}
	if w.capture != nil {
		if err := w.capture.Close(); err != nil {
			w.log.Error().Err(err).Msg("failed to close the transaction capture")
		}
	}
	if w.cursors != nil {
		w.cursors.Close()
	}
	if w.writer != nil {
		w.writer.Close()
	}
	if w.db != nil {
		w.db.Close()
	}
}

func encodeSeed(seed []byte) string {
	return base64RawStd.EncodeToString(seed)
}
