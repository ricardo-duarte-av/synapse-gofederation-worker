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

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/destinations"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// Mode is how this worker relates to the homeserver it serves.
type Mode string

const (
	// ModeShadow is the default: this worker impersonates the shard of an
	// EXISTING sender, writes nothing to Synapse's tables and sends nothing
	// unless a destination is explicitly allowlisted. worker_name must NOT be
	// one of federation_sender_instances.
	ModeShadow Mode = "shadow"

	// ModePrimary means this worker IS one of the homeserver's configured
	// federation senders. worker_name MUST be in federation_sender_instances,
	// and the read-only guarantee no longer applies: a sender's bookkeeping is
	// a set of deletions, so it needs a role that can write.
	ModePrimary Mode = "primary"
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

	// Mode says whether this worker is a shadow of somebody else's sender or a
	// homeserver's own.
	//
	// It inverts several checks rather than merely enabling a feature, which is
	// why it is a mode and not a set of booleans -- see Resolve.
	Mode Mode `yaml:"mode"`

	// SynapseConfig points at Synapse's homeserver.yaml. Everything derivable
	// from it -- server_name, the sender instance list, the signing key path,
	// Redis, the retry tuning -- is read from there rather than duplicated
	// here, so it cannot drift. See internal/synapsecfg.
	SynapseConfig string `yaml:"synapse_config"`

	// SigningKeyPath overrides homeserver.yaml's signing_key_path, which names
	// a path inside Synapse's container.
	SigningKeyPath string `yaml:"signing_key_path"`

	Shadow      ShadowConfig      `yaml:"shadow"`
	Resolution  ResolutionConfig  `yaml:"resolution"`
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

	// CaptureDestinations names servers whose transactions are recorded in
	// full, for byte-level comparison against what Synapse actually sent to
	// them (see cmd/fedrecorder and docs/verification.md).
	//
	// Meant for ONE test destination we control. Pointing it at a real,
	// busy server would write every transaction to that server to disk, which
	// is a lot of disk and a lot of other people's message content.
	CaptureDestinations []string `yaml:"capture_destinations"`

	// CaptureFile is where those recordings go.
	CaptureFile string `yaml:"capture_file"`

	// SendOnlyTo is the send allowlist: destinations this worker REALLY sends
	// to. Everything else is dry-run, even when shadow.enabled is false.
	//
	// This is how going live happens -- one destination at a time, starting
	// with a test homeserver we control -- rather than all of them at once.
	// An empty list sends to nobody; that is the safe reading and it is not
	// overridable by accident. See SendToAll.
	SendOnlyTo []string `yaml:"send_only_to"`

	// SendToAll makes this worker send to every destination in its shard.
	//
	// The end state, and the one that carries real consequence, so it is a
	// separate option rather than a property of leaving send_only_to empty.
	// Nobody should reach production federation traffic by deleting a line.
	SendToAll bool `yaml:"send_to_all"`
}

// ResolutionConfig tunes working out where an event goes.
type ResolutionConfig struct {
	// HostCacheEntries is how many state groups' joined-host sets to remember.
	//
	// Sized by MEMORY rather than by hit rate, which is the part that surprises.
	// The lookup it caches is keyed by state group, and a state group is
	// immutable, so the cache never evicts for correctness and a bigger one
	// cannot raise the hit rate above its ceiling: events between two state
	// changes share a group, so the best possible rate is 1 - 1/(events per
	// group), and misses beyond that are first sights of a group that no size
	// prevents.
	//
	// What DOES vary between homeservers is what an entry costs. One entry is
	// one hostname per server in the room, and rooms differ by three orders of
	// magnitude -- on this deployment the median room has 2 servers and the
	// largest has 1,308. So the same 4,096 entries is a few megabytes on one
	// homeserver and hundreds on another, which is why this is a setting and
	// not a constant. Watch gofed_state_group_cache_entries: while it sits
	// below the limit, raising it changes nothing at all.
	HostCacheEntries int `yaml:"host_cache_entries"`
}

// DatabaseConfig is the read-only connection to Synapse's database.
type DatabaseConfig struct {
	// DSN is a libpq keyword string, not a URI, matching the sibling workers:
	//   host=/var/sockets user=gofed_ro dbname=synapse-db
	//
	// Empty is the normal case: the connection is then built from
	// homeserver.yaml's `database:` block, so there is one statement of where
	// the homeserver's data lives rather than two that can drift. Set it only
	// to connect differently from Synapse -- under a read-only role, or
	// bypassing a pooler -- and see RequireReadOnly, which the derived
	// connection cannot satisfy because it is Synapse's own read-write role.
	DSN                   string `yaml:"dsn"`
	MaxConns              int32  `yaml:"max_conns"`
	ConnectTimeoutSeconds int    `yaml:"connect_timeout_seconds"`

	// RequireReadOnly makes a role with write grants on Synapse's tables a
	// startup failure rather than a warning. Default false, matching
	// gosync-worker: a scratch database is a legitimate way to develop against
	// this. Set it true in production -- but only in shadow mode, where it is
	// meaningful.
	RequireReadOnly bool `yaml:"require_read_only"`

	// WriteDSN is the connection a PRIMARY sender does its bookkeeping on:
	// deleting consumed to-device rows and device pokes, recording retry
	// timings and catch-up cursors.
	//
	// Kept separate from DSN, under its own role, so the reading path stays
	// provably read-only and a shadow deployment has no writable handle to
	// Synapse's tables anywhere in the process. Refused in shadow mode; in
	// primary mode it may be left empty, which falls back to homeserver.yaml's
	// own connection -- definitionally able to write, since it is what Synapse
	// writes with.
	WriteDSN string `yaml:"write_dsn"`
}

// StateConfig is our own cursor storage -- the only thing this worker writes.
type StateConfig struct {
	// DSN is a separate connection, under a separate role with write access to
	// exactly one table and nothing else. Empty means reuse whatever the
	// Synapse database connection resolved to, which only works if that role
	// can write to our schema.
	DSN string `yaml:"dsn"`
	// Table is the qualified name of our cursor table.
	Table string `yaml:"table"`
	// RoutesTable holds our copy of Synapse's destination_rooms, which is what
	// the routing comparison joins against. Empty disables route recording.
	RoutesTable string `yaml:"routes_table"`

	// RetryTable holds THIS sender's per-destination backoff.
	//
	// Deliberately ours rather than Synapse's `destinations`: Synapse caches
	// those timings in every process and only its own writes invalidate the
	// cache, so a backoff written or cleared from out here would be served
	// stale indefinitely. See internal/state.SetRetryTimings for why
	// publishing the invalidation instead would be worse. Empty keeps the
	// backoff in memory for this process's lifetime, which loses it on every
	// restart.
	RetryTable string `yaml:"retry_table"`
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

	// PresenceBatchStates flushes a destination's held presence once this many
	// user states are waiting. Zero disables batching entirely.
	//
	// Presence only. Typing and receipts are never held -- typing is a
	// statement about this instant and a remote expires it after a minute, and
	// a receipt arriving late defeats its purpose. Both also act as flush
	// triggers, so held presence rides along with them for free.
	PresenceBatchStates int `yaml:"presence_batch_states"`

	// PresenceBatchMaxWaitSeconds bounds how long the OLDEST held presence
	// state waits before it is sent regardless of how few there are.
	//
	// This is a deliberate divergence from Synapse, not a gap being closed.
	// Synapse staggers wakeups by about 20ms (_DestinationWakeupQueue), for a
	// different reason -- avoiding a stampede of TLS handshakes -- so it
	// batches only incidentally. At this deployment's rate of roughly one
	// presence row per destination every few seconds, 20ms accumulates nothing;
	// seconds are needed before a second state ever arrives to merge with.
	//
	// The cost of holding is not ours. A transaction carrying one presence
	// state costs the RECEIVING server a request, an encoding and a signature
	// verification, the same as one carrying fifty.
	PresenceBatchMaxWaitSeconds int `yaml:"presence_batch_max_wait_seconds"`

	// TransactionIDPrefix namespaces our transaction ids away from Synapse's.
	//
	// A receiving Synapse deduplicates on (origin, transaction_id) and returns
	// the cached response for a repeat, so if this worker and one of Synapse's
	// own senders ever pick the same id for the same destination, the second
	// transaction's events are silently discarded. Both seed from
	// milliseconds-since-epoch and increment, so their ranges drift into each
	// other. The prefix removes the possibility.
	//
	// Only safe to set empty once no other sender delivers to any destination
	// this worker handles.
	TransactionIDPrefix *string `yaml:"transaction_id_prefix"`
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
		State: StateConfig{
			Table:       "gofederation.stream_positions",
			RoutesTable: "gofederation.destination_rooms",
			RetryTable:  "gofederation.destination_retry",
		},
		Queue: QueueConfig{
			MaxConcurrentDestinations: 2000,
			MaxPDUsPerTransaction:     SynapseMaxPDUsPerTransaction,
			MaxEDUsPerTransaction:     SynapseMaxEDUsPerTransaction,
			EventBatchLimit:           SynapseEventBatchLimit,
			PresenceBatchStates:       SynapseMaxEDUsPerTransaction / 2,
			// Held long enough to be worth holding. See the field comment for
			// why this is seconds rather than Synapse's milliseconds.
			PresenceBatchMaxWaitSeconds: 30,
		},
		Resolution: ResolutionConfig{HostCacheEntries: destinations.DefaultHostCacheEntries},
		Metrics:    MetricsConfig{Addr: ":9202"},
		Log:        LogConfig{Level: "info"},
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: %w%s", err, hintFor(err))
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// hintFor turns an unknown-key error into an answer where we have one.
//
// KnownFields reports the type it failed to find the key on, which is accurate
// and unhelpful: "field federation not found in type config.ShadowConfig" is
// what you get for putting Synapse's federation tuning in this file, indented
// one level too far. The keys below are all ones that belong in SYNAPSE's
// homeserver.yaml, and every one of them is an easy mistake to make because
// this worker reads them -- just from the other file.
func hintFor(err error) string {
	elsewhere := map[string]string{
		"federation":                     "Synapse's homeserver.yaml",
		"client_timeout":                 "the `federation:` block of Synapse's homeserver.yaml",
		"max_long_retries":               "the `federation:` block of Synapse's homeserver.yaml",
		"max_long_retry_delay":           "the `federation:` block of Synapse's homeserver.yaml",
		"destination_min_retry_interval": "the `federation:` block of Synapse's homeserver.yaml",
		"destination_retry_multiplier":   "the `federation:` block of Synapse's homeserver.yaml",
		"destination_max_retry_interval": "the `federation:` block of Synapse's homeserver.yaml",
		"redis":                          "Synapse's homeserver.yaml",
		"federation_sender_instances":    "Synapse's homeserver.yaml",
		"server_name":                    "Synapse's homeserver.yaml",
	}
	msg := err.Error()
	for key, where := range elsewhere {
		if strings.Contains(msg, "field "+key+" not found") {
			return fmt.Sprintf("\n\nhint: `%s` is not a setting of this file. "+
				"It is read from %s, named by synapse_config, so that this worker "+
				"and Synapse cannot disagree about it.", key, where)
		}
	}
	return ""
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

// TransactionIDPrefix is the prefix for our transaction ids.
//
// A pointer with a nil default, so "the key was absent" and "the key was set to
// empty" are distinguishable: absent takes the safe default, and only an
// explicit empty string turns the namespacing off.
func (c *Config) TransactionIDPrefix() string {
	if c.Queue.TransactionIDPrefix == nil {
		return txn.DefaultIDPrefix
	}
	return *c.Queue.TransactionIDPrefix
}

// PresenceBatchMaxWait is how long held presence may wait.
func (c *Config) PresenceBatchMaxWait() time.Duration {
	return time.Duration(c.Queue.PresenceBatchMaxWaitSeconds) * time.Second
}

// ConnectTimeout is the database connect timeout.
func (c *Config) ConnectTimeout() time.Duration {
	return time.Duration(c.Database.ConnectTimeoutSeconds) * time.Second
}

func (c *Config) validate() error {
	switch c.Mode {
	case "", ModeShadow:
		c.Mode = ModeShadow
	case ModePrimary:
	default:
		return fmt.Errorf("config: mode %q is not %q or %q", c.Mode, ModeShadow, ModePrimary)
	}
	if c.Mode == ModePrimary {
		// database.write_dsn is not required here: an empty one falls back to
		// homeserver.yaml's connection in Resolve, which is the role Synapse
		// itself writes with. Whether anything writable exists at all is
		// checked there, where homeserver.yaml has been read.
		if c.Database.RequireReadOnly {
			return fmt.Errorf("config: database.require_read_only cannot be set in primary " +
				"mode; the worker must write its own bookkeeping")
		}
		if c.ShadowEnabled() {
			return fmt.Errorf("config: a primary sender cannot run with shadow.enabled true; " +
				"it is the homeserver's only sender and dry-running would drop all its traffic")
		}
	}
	if c.Mode == ModeShadow && c.Database.WriteDSN != "" {
		return fmt.Errorf("config: database.write_dsn is set in shadow mode; a shadow must " +
			"have no writable handle to Synapse's tables at all")
	}
	if c.WorkerName == "" {
		return fmt.Errorf("config: worker_name is required; it is this process's own " +
			"identity on the replication bus and must differ from every federation sender")
	}
	if c.SynapseConfig == "" {
		return fmt.Errorf("config: synapse_config is required; it is where server_name, " +
			"federation_sender_instances and the signing key come from")
	}
	if c.Mode == ModeShadow && c.Shadow.Instance == "" {
		return fmt.Errorf("config: shadow.instance is required in shadow mode; it names the " +
			"federation sender whose shard this worker takes")
	}
	if c.Database.MaxConns < 1 {
		return fmt.Errorf("config: database.max_conns must be at least 1, got %d", c.Database.MaxConns)
	}
	if c.State.Table == "" {
		return fmt.Errorf("config: state.table cannot be empty")
	}
	if c.Resolution.HostCacheEntries < 1 {
		return fmt.Errorf("config: resolution.host_cache_entries must be at least 1, got %d; "+
			"omit it for the default of %d", c.Resolution.HostCacheEntries,
			destinations.DefaultHostCacheEntries)
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
	if c.Queue.PresenceBatchStates < 0 || c.Queue.PresenceBatchMaxWaitSeconds < 0 {
		return fmt.Errorf("config: queue.presence_batch_states and "+
			"queue.presence_batch_max_wait_seconds cannot be negative, got %d and %d; "+
			"set either to 0 to send presence as soon as it arrives",
			c.Queue.PresenceBatchStates, c.Queue.PresenceBatchMaxWaitSeconds)
	}
	if !c.ShadowEnabled() {
		// Leaving shadow mode is the one change here with consequences
		// outside this process, so an ambiguous configuration is refused
		// rather than resolved.
		if !c.Shadow.SendToAll && len(c.Shadow.SendOnlyTo) == 0 {
			return fmt.Errorf("config: shadow.enabled is false but neither " +
				"shadow.send_only_to nor shadow.send_to_all is set; refusing to start " +
				"in a state where it is unclear whether this worker sends real " +
				"federation traffic")
		}
		if c.Shadow.SendToAll && len(c.Shadow.SendOnlyTo) > 0 {
			return fmt.Errorf("config: shadow.send_to_all and shadow.send_only_to are " +
				"both set; they contradict each other, so neither is assumed")
		}
	}
	if len(c.Shadow.CaptureDestinations) > 0 && c.Shadow.CaptureFile == "" {
		return fmt.Errorf("config: shadow.capture_file is required when " +
			"shadow.capture_destinations is set")
	}
	if c.Mode == ModeShadow && c.ShadowEnabled() && c.Shadow.DiffLogDir == "" {
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
