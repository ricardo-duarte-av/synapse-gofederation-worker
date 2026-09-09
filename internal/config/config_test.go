package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `
worker_name: av-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  instance: av-federation-sender-worker-1
  difflog_dir: /data/difflog
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`

func TestParseMinimalAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ShadowEnabled() {
		t.Error("shadow must default to enabled; the one direction this can never fail is open")
	}
	if !cfg.ReplicationEnabled() {
		t.Error("replication should default to enabled")
	}
	if cfg.Queue.MaxPDUsPerTransaction != SynapseMaxPDUsPerTransaction ||
		cfg.Queue.MaxEDUsPerTransaction != SynapseMaxEDUsPerTransaction {
		t.Errorf("transaction limits = %d/%d, want Synapse's %d/%d",
			cfg.Queue.MaxPDUsPerTransaction, cfg.Queue.MaxEDUsPerTransaction,
			SynapseMaxPDUsPerTransaction, SynapseMaxEDUsPerTransaction)
	}
	if cfg.Metrics.Addr != ":9202" {
		t.Errorf("Metrics.Addr = %q", cfg.Metrics.Addr)
	}
	if cfg.State.Table == "" {
		t.Error("State.Table has no default")
	}
}

// The defaults-before-decode rule: an explicit false must survive.
func TestExplicitFalseBeatsTheDefault(t *testing.T) {
	cfg, err := Parse([]byte(minimal + "\nreplication:\n  enabled: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicationEnabled() {
		t.Error("replication.enabled: false was overridden by the default")
	}
}

func TestShadowCanBeTurnedOff(t *testing.T) {
	cfg, err := Parse([]byte(`
worker_name: av-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: false
  instance: av-federation-sender-worker-1
  send_only_to: [testing.aguiarvieira.pt]
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShadowEnabled() {
		t.Error("shadow.enabled: false was overridden by the default")
	}
	// difflog_dir is only required while shadowing.
	if cfg.Shadow.DiffLogDir != "" {
		t.Errorf("DiffLogDir = %q, want empty", cfg.Shadow.DiffLogDir)
	}
}

// A typo in a key must not be silently ignored. shadow.enabled is the reason.
func TestUnknownFieldIsAnError(t *testing.T) {
	if _, err := Parse([]byte(minimal + "\nshaodw:\n  enabled: false\n")); err == nil {
		t.Fatal("expected an error for an unknown top-level key")
	}
	if _, err := Parse([]byte(minimal + "\nqueue:\n  max_pdus: 10\n")); err == nil {
		t.Fatal("expected an error for an unknown nested key")
	}
}

func TestRequiredFields(t *testing.T) {
	for name, body := range map[string]string{
		"no worker_name":     "synapse_config: /h.yaml\nshadow:\n  instance: w1\n  difflog_dir: /d\ndatabase:\n  dsn: x\n",
		"no synapse_config":  "worker_name: w\nshadow:\n  instance: w1\n  difflog_dir: /d\ndatabase:\n  dsn: x\n",
		"no shadow.instance": "worker_name: w\nsynapse_config: /h.yaml\nshadow:\n  difflog_dir: /d\ndatabase:\n  dsn: x\n",
		"no difflog while shadowing": "worker_name: w\nsynapse_config: /h.yaml\nshadow:\n  instance: w1\n" +
			"database:\n  dsn: x\n",
	} {
		if _, err := Parse([]byte(body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestRejectsNonsenseNumbers(t *testing.T) {
	for name, body := range map[string]string{
		"zero conns":        "database:\n  max_conns: 0\n",
		"zero destinations": "queue:\n  max_concurrent_destinations: 0\n",
		"zero pdus":         "queue:\n  max_pdus_per_transaction: 0\n",
		"negative edus":     "queue:\n  max_edus_per_transaction: -1\n",
		"zero batch":        "queue:\n  event_batch_limit: 0\n",
	} {
		if _, err := Parse([]byte(minimal + body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestRejectsBadLogLevel(t *testing.T) {
	if _, err := Parse([]byte(minimal + "\nlog:\n  level: verbose\n")); err == nil {
		t.Fatal("expected an error for an unknown log level")
	}
	if _, err := Parse([]byte(minimal + "\nlog:\n  level: debug\n")); err != nil {
		t.Fatalf("debug should be accepted: %v", err)
	}
}

// Leaving shadow mode is the one change here with consequences outside the
// process, so an ambiguous configuration is refused rather than resolved.
func TestLeavingShadowModeRequiresAnExplicitTarget(t *testing.T) {
	live := `
worker_name: av-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: false
  instance: av-federation-sender-worker-1
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`
	if _, err := Parse([]byte(live)); err == nil {
		t.Fatal("shadow.enabled: false was accepted with no send target; " +
			"it must not be unclear whether real traffic is sent")
	}

	// An allowlist is a valid target.
	withAllowlist := strings.Replace(live,
		"  instance: av-federation-sender-worker-1\n",
		"  instance: av-federation-sender-worker-1\n  send_only_to: [testing.aguiarvieira.pt]\n", 1)
	cfg, err := Parse([]byte(withAllowlist))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShadowEnabled() || len(cfg.Shadow.SendOnlyTo) != 1 {
		t.Errorf("got %+v", cfg.Shadow)
	}

	// So is sending to everything, but only when asked for by name.
	withAll := strings.Replace(live,
		"  instance: av-federation-sender-worker-1\n",
		"  instance: av-federation-sender-worker-1\n  send_to_all: true\n", 1)
	if _, err := Parse([]byte(withAll)); err != nil {
		t.Fatal(err)
	}
}

// The two options contradict each other, so neither is assumed.
func TestSendToAllAndAllowlistTogetherIsRefused(t *testing.T) {
	_, err := Parse([]byte(`
worker_name: av-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: false
  instance: av-federation-sender-worker-1
  send_to_all: true
  send_only_to: [testing.aguiarvieira.pt]
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`))
	if err == nil {
		t.Fatal("send_to_all and send_only_to were accepted together")
	}
}

// An allowlist set while still shadowing is harmless and must not be an error:
// it is how a config is staged before the switch is flipped.
func TestAllowlistIsAllowedWhileStillShadowing(t *testing.T) {
	cfg, err := Parse([]byte(`
worker_name: av-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  instance: av-federation-sender-worker-1
  difflog_dir: /data/difflog
  send_only_to: [testing.aguiarvieira.pt]
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ShadowEnabled() {
		t.Error("shadow mode was turned off by setting an allowlist")
	}
	if len(cfg.Shadow.SendOnlyTo) != 1 {
		t.Errorf("SendOnlyTo = %v", cfg.Shadow.SendOnlyTo)
	}
}

// The two modes invert several checks rather than one enabling a feature, and
// getting the inversion wrong is a worker that runs healthily while doing
// nothing.
func TestPrimaryModeRequirements(t *testing.T) {
	base := `
mode: primary
worker_name: testing-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: false
  send_to_all: true
database:
  dsn: "host=/var/sockets user=gofed_ro dbname=synapse-db"
`
	// A primary must be able to write; its bookkeeping is a set of deletions.
	// An absent write_dsn is not refused here, though: it falls back to
	// homeserver.yaml's own connection, which is checked in Resolve where that
	// file has been read. See TestPrimaryWithoutAWriteDSNTakesSynapses.
	if _, err := Parse([]byte(base)); err != nil {
		t.Errorf("primary mode with no write_dsn should parse: %v", err)
	}

	withWrite := base + `  write_dsn: "host=/var/sockets user=gofed_rw dbname=synapse-db"
`
	cfg, err := Parse([]byte(withWrite))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != ModePrimary {
		t.Errorf("Mode = %q", cfg.Mode)
	}

	// require_read_only contradicts primary mode outright.
	if _, err := Parse([]byte(withWrite + "  require_read_only: true\n")); err == nil {
		t.Error("primary mode accepted require_read_only")
	}

	// A primary that dry-runs would drop the homeserver's entire federation
	// traffic, since nothing else is sending it.
	shadowing := `
mode: primary
worker_name: testing-gofederation-worker-1
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: true
  difflog_dir: /d
database:
  dsn: x
  write_dsn: y
`
	if _, err := Parse([]byte(shadowing)); err == nil {
		t.Error("a primary sender was allowed to run in shadow (dry-run) mode")
	}
}

// A shadow must have no writable handle to Synapse's tables anywhere in the
// process, so configuring one is refused rather than ignored.
func TestShadowModeRefusesAWriteDSN(t *testing.T) {
	if _, err := Parse([]byte(minimal + "  write_dsn: \"host=/var/sockets user=rw\"\n")); err == nil {
		t.Error("shadow mode accepted a write_dsn")
	}
}

func TestUnknownModeIsRejected(t *testing.T) {
	if _, err := Parse([]byte(minimal + "\nmode: sortof\n")); err == nil {
		t.Error("an unknown mode was accepted")
	}
}

// The example is what every deployment starts from, so a key that no longer
// parses -- KnownFields makes any stale one a hard error -- should fail here
// rather than at the first `docker compose up`.
func TestTheExampleConfigParses(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "deploy",
		"gofederation-worker.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	// The example must ship inert. Everything else in it is a matter of taste;
	// this is not.
	if !cfg.ShadowEnabled() {
		t.Error("the example config is not in shadow mode")
	}
	// database.dsn is commented out in the example: the connection comes from
	// homeserver.yaml, which is the change this documents.
	if cfg.Database.DSN != "" {
		t.Errorf("database.dsn = %q, want it left to homeserver.yaml", cfg.Database.DSN)
	}
}

// A cache size of zero would be a cache that misses every time while looking
// configured, so it is refused rather than silently defaulted.
func TestHostCacheEntriesMustBePositive(t *testing.T) {
	if _, err := Parse([]byte(minimal + "\nresolution:\n  host_cache_entries: 0\n")); err == nil {
		t.Error("a zero host cache size was accepted")
	}
	cfg, err := Parse([]byte(minimal + "\nresolution:\n  host_cache_entries: 128\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Resolution.HostCacheEntries != 128 {
		t.Errorf("HostCacheEntries = %d", cfg.Resolution.HostCacheEntries)
	}
	// Absent takes the default rather than zero.
	cfg, err = Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Resolution.HostCacheEntries < 1 {
		t.Errorf("absent host_cache_entries gave %d", cfg.Resolution.HostCacheEntries)
	}
}

// "field federation not found in type config.ShadowConfig" is what you get for
// putting Synapse's federation tuning in this file, indented one level too far.
// It is accurate and tells you nothing about what to do, and it happened.
func TestSettingsThatLiveInSynapsesConfigSaySo(t *testing.T) {
	_, err := Parse([]byte(minimal + `
  federation:
    client_timeout: 180s
    max_long_retries: 20
`))
	if err == nil {
		t.Fatal("a federation block in this file was accepted")
	}
	if !strings.Contains(err.Error(), "homeserver.yaml") {
		t.Errorf("error does not say where the setting belongs: %v", err)
	}
	if !strings.Contains(err.Error(), "synapse_config") {
		t.Errorf("error does not name the setting that points at that file: %v", err)
	}
}
