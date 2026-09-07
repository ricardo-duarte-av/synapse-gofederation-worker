// Package config parses gofederation-worker.yaml.
//
// Two rules, both borrowed from the sibling workers because both have caught
// real mistakes:
//
//   - Defaults are set on the struct BEFORE decoding, so an explicit `false` in
//     the document still wins over a default of true.
//   - Unknown fields are a startup error (KnownFields), so a typo in a key is
//     reported rather than silently ignored. For this worker that matters more
//     than usual: `shadow.enabled` is the switch between logging a transaction
//     and sending it to a real homeserver, and a misspelt key there would fail
//     open in the worst possible direction.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole file.
type Config struct {
	// WorkerName is THIS process's own identity, and must differ from every
	// name in federation_sender_instances.
	//
	// It is not the same thing as shadow.instance, and conflating the two is a
	// real bug rather than a tidiness point: the replication bus suppresses a
	// worker's own echo by instance name, so a shadow calling itself by the
	// name of the sender it shadows discards that sender's rows -- which are
	// exactly the ones it needs. It also keeps our cursors from colliding if
	// two shadows of the same sender ever run.
	WorkerName string `yaml:"worker_name"`

	// SynapseConfig points at Synapse's homeserver.yaml. Everything derivable
	// from it -- server_name, the sender instance list, the signing key path,
	// Redis, the retry tuning -- is read from there rather than duplicated
	// here, so it cannot drift. See internal/synapsecfg.
	SynapseConfig string `yaml:"synapse_config"`

	// SigningKeyPath overrides homeserver.yaml's signing_key_path, which names
	// a path inside Synapse's container.
	SigningKeyPath string `yaml:"signing_key_path"`

	Shadow      ShadowConfig      `yaml:"shadow"`
	Database    DatabaseConfig    `yaml:"database"`
	State       StateConfig       `yaml:"state"`
	Replication ReplicationConfig `yaml:"replication"`
	Queue       QueueConfig       `yaml:"queue"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Log         LogConfig         `yaml:"log"`
}

// ShadowConfig controls whether this worker is inert.
type ShadowConfig struct {
	// Enabled true means dry-run: build, sign, log, never send, never write to
	// a Synapse table. This defaults to TRUE and every code path that could
	// reach the network checks it. Turning it off is the promotion decision and
	// is made from the difflog, not from the code looking finished --
	// docs/shadow-safety.md.
	Enabled *bool `yaml:"enabled"`

	// Instance is the federation sender whose SHARD we take -- which
	// destinations are ours. It is not our identity; see WorkerName.
	//
	// It must appear in
	// homeserver.yaml's federation_sender_instances, and startup fails if it
	// does not: a name that is not in the list owns no destinations at all, so
	// the worker would run perfectly and do nothing, which is the hardest kind
	// of misconfiguration to notice.
	Instance string `yaml:"instance"`

	// DiffLogDir holds the persisted comparison record. Counters there survive
	// restarts because the promotion gate is measured in weeks.
	DiffLogDir string `yaml:"difflog_dir"`
}

// DatabaseConfig is the read-only connection to Synapse's database.
type DatabaseConfig struct {
	// DSN is a libpq keyword string, not a URI, matching the sibling workers:
	//   host=/var/sockets user=gofed_ro dbname=synapse-db
	DSN                   string `yaml:"dsn"`
	MaxConns              int32  `yaml:"max_conns"`
	ConnectTimeoutSeconds int    `yaml:"connect_timeout_seconds"`

	// RequireReadOnly makes a role with write grants on Synapse's tables a
	// startup failure rather than a warning. Default false, matching
	// gosync-worker: a scratch database is a legitimate way to develop against
	// this. Set it true in production.
	RequireReadOnly bool `yaml:"require_read_only"`
}

// StateConfig is our own cursor storage -- the only thing this worker writes.
type StateConfig struct {
	// DSN is a separate connection, under a separate role with write access to
	// exactly one table and nothing else. Empty means reuse the read-only
	// database connection, which only works if that role can write, so it is
	// really only for tests.
	DSN string `yaml:"dsn"`
	// Table is the qualified name of our cursor table.
	Table string `yaml:"table"`
}

// ReplicationConfig is the Redis subscription.
type ReplicationConfig struct {
	Enabled *bool `yaml:"enabled"`
	// Address is a unix socket path or host:port. Empty takes whatever
	// homeserver.yaml's redis block says.
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// Channel MUST equal Synapse's server_name; the channel is named after it
	// and there is no prefix setting (replication/tcp/redis.py:399).
	// Subscribing to the wrong channel raises no error and delivers nothing, so
	// the worker would look merely idle. Empty takes server_name.
	Channel string `yaml:"channel"`
}

// QueueConfig bounds the per-destination fan-out.
type QueueConfig struct {
	// MaxConcurrentDestinations caps how many destination goroutines may be
	// building or sending a transaction at once. The point of this worker is
	// that a thousand destinations cost a thousand goroutines rather than a
	// thousand Python coroutines on one reactor, but unbounded is still a way
	// to fall over.
	MaxConcurrentDestinations int `yaml:"max_concurrent_destinations"`

	// MaxPDUsPerTransaction and MaxEDUsPerTransaction default to Synapse's
	// values and should not be changed while shadowing: a different batch size
	// produces different transactions, and the diff against the real sender
	// stops meaning anything.
	MaxPDUsPerTransaction int `yaml:"max_pdus_per_transaction"`
	MaxEDUsPerTransaction int `yaml:"max_edus_per_transaction"`

	// EventBatchLimit is the `limit` of the event pickup query, Synapse's 100.
	EventBatchLimit int `yaml:"event_batch_limit"`
}

// MetricsConfig is the Prometheus listener.
type MetricsConfig struct {
	// Addr is host:port; empty disables the listener.
	Addr string `yaml:"addr"`
}

// LogConfig is zerolog's setup.
type LogConfig struct {
	Level string `yaml:"level"`
	// Pretty selects the console writer. Off in production, where the logs are
	// collected as JSON.
	Pretty bool `yaml:"pretty"`
}

// Synapse's own limits. Defaulting to anything else while shadowing makes the
// comparison meaningless; see docs/synapse-reference.md §3.
const (
	SynapseMaxPDUsPerTransaction = 50
	SynapseMaxEDUsPerTransaction = 100
	SynapseEventBatchLimit       = 100
)

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return Parse(b)
}

// Parse decodes and validates a config document.
func Parse(data []byte) (*Config, error) {
	// Defaults go on the struct before decoding, so an explicit false in the
	// document still wins.
	cfg := &Config{
		Database: DatabaseConfig{MaxConns: 16, ConnectTimeoutSeconds: 10},
		State:    StateConfig{Table: "gofederation.stream_positions"},
		Queue: QueueConfig{
			MaxConcurrentDestinations: 2000,
			MaxPDUsPerTransaction:     SynapseMaxPDUsPerTransaction,
			MaxEDUsPerTransaction:     SynapseMaxEDUsPerTransaction,
			EventBatchLimit:           SynapseEventBatchLimit,
		},
		Metrics: MetricsConfig{Addr: ":9202"},
		Log:     LogConfig{Level: "info"},
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ShadowEnabled reports whether the worker is in dry-run mode.
//
// A pointer with a nil default rather than a bool, so that "the key was absent"
// and "the key said false" are distinguishable -- and absent means shadowing.
// The one direction this must never fail is open.
func (c *Config) ShadowEnabled() bool {
	return c.Shadow.Enabled == nil || *c.Shadow.Enabled
}

// ReplicationEnabled reports whether to follow the replication stream. Defaults
// to true: a federation sender that is not listening is not a federation
// sender.
func (c *Config) ReplicationEnabled() bool {
	return c.Replication.Enabled == nil || *c.Replication.Enabled
}

// ConnectTimeout is the database connect timeout.
func (c *Config) ConnectTimeout() time.Duration {
	return time.Duration(c.Database.ConnectTimeoutSeconds) * time.Second
}

func (c *Config) validate() error {
	if c.WorkerName == "" {
		return fmt.Errorf("config: worker_name is required; it is this process's own " +
			"identity on the replication bus and must differ from every federation sender")
	}
	if c.SynapseConfig == "" {
		return fmt.Errorf("config: synapse_config is required; it is where server_name, " +
			"federation_sender_instances and the signing key come from")
	}
	if c.Shadow.Instance == "" {
		return fmt.Errorf("config: shadow.instance is required; it names the federation " +
			"sender whose shard this worker takes")
	}
	if c.Database.DSN == "" {
		return fmt.Errorf("config: database.dsn is required")
	}
	if c.Database.MaxConns < 1 {
		return fmt.Errorf("config: database.max_conns must be at least 1, got %d", c.Database.MaxConns)
	}
	if c.State.Table == "" {
		return fmt.Errorf("config: state.table cannot be empty")
	}
	if c.Queue.MaxConcurrentDestinations < 1 {
		return fmt.Errorf("config: queue.max_concurrent_destinations must be at least 1, got %d",
			c.Queue.MaxConcurrentDestinations)
	}
	for _, f := range []struct {
		name  string
		value int
	}{
		{"queue.max_pdus_per_transaction", c.Queue.MaxPDUsPerTransaction},
		{"queue.max_edus_per_transaction", c.Queue.MaxEDUsPerTransaction},
		{"queue.event_batch_limit", c.Queue.EventBatchLimit},
	} {
		if f.value < 1 {
			return fmt.Errorf("config: %s must be at least 1, got %d", f.name, f.value)
		}
	}
	if c.ShadowEnabled() && c.Shadow.DiffLogDir == "" {
		return fmt.Errorf("config: shadow.difflog_dir is required while shadowing; " +
			"the comparison record is the only output that matters in dry-run")
	}
	switch c.Log.Level {
	case "trace", "debug", "info", "warn", "error", "fatal", "panic", "":
	default:
		return fmt.Errorf("config: log.level %q is not a zerolog level", c.Log.Level)
	}
	return nil
}
