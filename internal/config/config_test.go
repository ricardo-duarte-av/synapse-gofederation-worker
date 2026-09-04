package config

import "testing"

const minimal = `
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
synapse_config: /etc/synapse/homeserver.yaml
shadow:
  enabled: false
  instance: av-federation-sender-worker-1
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
		"no synapse_config":  "shadow:\n  instance: w1\n  difflog_dir: /d\ndatabase:\n  dsn: x\n",
		"no shadow.instance": "synapse_config: /h.yaml\nshadow:\n  difflog_dir: /d\ndatabase:\n  dsn: x\n",
		"no database.dsn":    "synapse_config: /h.yaml\nshadow:\n  instance: w1\n  difflog_dir: /d\n",
		"no difflog while shadowing": "synapse_config: /h.yaml\nshadow:\n  instance: w1\n" +
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
