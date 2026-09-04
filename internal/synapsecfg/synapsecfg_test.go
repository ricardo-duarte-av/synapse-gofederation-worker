package synapsecfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A signing key file in the real format: two lines, as this deployment has.
const twoKeyFile = "ed25519 a_Yofy AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8\n" +
	"ed25519 rAzySt ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8\n"

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "signing.key"), []byte(twoKeyFile), 0o600); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "homeserver.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = `
server_name: "example.com"
signing_key_path: signing.key
`

func TestLoadMinimal(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "example.com" {
		t.Errorf("ServerName = %q", cfg.ServerName)
	}
	// federation_sender_instances absent and send_federation defaulting to
	// true means the main process does the sending.
	if len(cfg.SenderInstances) != 1 || cfg.SenderInstances[0] != MainProcessInstanceName {
		t.Errorf("SenderInstances = %v, want [%s]", cfg.SenderInstances, MainProcessInstanceName)
	}
	if cfg.Retry.MinInterval != DefaultDestinationMinRetryInterval {
		t.Errorf("Retry.MinInterval = %v, want the Synapse default", cfg.Retry.MinInterval)
	}
	// Nil, not empty: no whitelist means send to everyone.
	if cfg.DomainWhitelist != nil {
		t.Errorf("DomainWhitelist = %v, want nil when the key is absent", cfg.DomainWhitelist)
	}
}

func TestSendFederationFalseMeansNoSenders(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+"send_federation: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SenderInstances) != 0 {
		t.Errorf("SenderInstances = %v, want empty", cfg.SenderInstances)
	}
}

// Order is the whole point: it is indexed by the shard hash.
func TestSenderInstancesKeepFileOrder(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
federation_sender_instances:
  - zzz-worker
  - aaa-worker
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SenderInstances) != 2 ||
		cfg.SenderInstances[0] != "zzz-worker" || cfg.SenderInstances[1] != "aaa-worker" {
		t.Errorf("SenderInstances = %v, want [zzz-worker aaa-worker] in file order", cfg.SenderInstances)
	}
}

func TestSenderInstancesAcceptsAString(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+"federation_sender_instances: solo-worker\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SenderInstances) != 1 || cfg.SenderInstances[0] != "solo-worker" {
		t.Errorf("SenderInstances = %v, want [solo-worker]", cfg.SenderInstances)
	}
}

// An explicitly empty list is a real configuration -- federation sending off --
// and must not fall back to the main process.
func TestEmptySenderListIsNotTheDefault(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+"federation_sender_instances: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SenderInstances) != 0 {
		t.Errorf("SenderInstances = %v, want empty", cfg.SenderInstances)
	}
}

func TestInstanceMapMainIsRewrittenToMaster(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
instance_map:
  main:
    path: /var/sockets/main.sock
  worker-1:
    host: 127.0.0.1
    port: 9093
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.InstanceMap["main"]; ok {
		t.Error(`InstanceMap still has "main"; Synapse rewrites it to "master"`)
	}
	m, ok := cfg.InstanceMap[MainProcessInstanceName]
	if !ok {
		t.Fatalf("InstanceMap has no %q entry: %v", MainProcessInstanceName, cfg.InstanceMap)
	}
	if m.Socket != "/var/sockets/main.sock" {
		t.Errorf("main socket = %q", m.Socket)
	}
	if got := cfg.InstanceMap["worker-1"].URL; got != "http://127.0.0.1:9093" {
		t.Errorf("worker-1 URL = %q", got)
	}
}

// A bare number is milliseconds to Synapse. Reading it as seconds would make
// the backoff a thousand times too long.
func TestRetryDurationsFollowSynapseUnits(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
federation:
  destination_min_retry_interval: 1m
  destination_retry_multiplier: 5
  destination_max_retry_interval: 365d
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retry.MinInterval != time.Minute {
		t.Errorf("MinInterval = %v, want 1m", cfg.Retry.MinInterval)
	}
	if cfg.Retry.Multiplier != 5 {
		t.Errorf("Multiplier = %v, want 5", cfg.Retry.Multiplier)
	}
	if cfg.Retry.MaxInterval != 365*24*time.Hour {
		t.Errorf("MaxInterval = %v, want 365d", cfg.Retry.MaxInterval)
	}

	cfg, err = Load(writeConfig(t, minimal+"federation:\n  destination_min_retry_interval: 60\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retry.MinInterval != 60*time.Millisecond {
		t.Errorf("MinInterval = %v, want 60ms (a bare number is milliseconds)", cfg.Retry.MinInterval)
	}
}

// Empty and absent mean opposite things: nobody versus everybody.
func TestEmptyDomainWhitelistIsNotNil(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+"federation_domain_whitelist: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DomainWhitelist == nil {
		t.Fatal("DomainWhitelist is nil for an empty list; that means 'send to everyone'")
	}
	if len(cfg.DomainWhitelist) != 0 {
		t.Errorf("DomainWhitelist = %v, want empty", cfg.DomainWhitelist)
	}
}

func TestRedisUnixSocketAndTCP(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
redis:
  enabled: true
  path: /var/sockets/keydb
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Redis.Enabled || cfg.Redis.Socket != "/var/sockets/keydb" || cfg.Redis.Addr != "" {
		t.Errorf("Redis = %+v", cfg.Redis)
	}

	cfg, err = Load(writeConfig(t, minimal+"redis:\n  enabled: true\n  host: keydb\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "keydb:6379" {
		t.Errorf("Redis.Addr = %q, want the default port applied", cfg.Redis.Addr)
	}
}

func TestSigningKeyFirstIsTheSigningOne(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SigningKeys) != 2 {
		t.Fatalf("loaded %d keys, want 2", len(cfg.SigningKeys))
	}
	k, err := cfg.SigningKey()
	if err != nil {
		t.Fatal(err)
	}
	if k.ID() != "ed25519:a_Yofy" {
		t.Errorf("SigningKey().ID() = %q, want the first key in the file", k.ID())
	}
	if len(k.PrivateKey()) == 0 {
		t.Error("PrivateKey() is empty")
	}
}

func TestMissingServerNameIsAnError(t *testing.T) {
	if _, err := Load(writeConfig(t, "signing_key_path: signing.key\n")); err == nil {
		t.Fatal("expected an error for a config with no server_name")
	}
}

func TestParseSigningKeysRejectsBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"empty":             "\n\n",
		"wrong field count": "ed25519 a_Yofy\n",
		"bad algorithm":     "rsa a_Yofy MFRJ0FCkc1IqiWFGoCRQqzeIhHMDDGRi5jbHqxUvCDA\n",
		"padded base64":     "ed25519 a_Yofy MFRJ0FCkc1IqiWFGoCRQqzeIhHMDDGRi5jbHqxUvCDA=\n",
		"short seed":        "ed25519 a_Yofy c2hvcnQ\n",
	} {
		if _, err := ParseSigningKeys(body); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestParseSigningKeysSkipsBlanksAndComments(t *testing.T) {
	keys, err := ParseSigningKeys("# a comment\n\n" + twoKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("parsed %d keys, want 2", len(keys))
	}
}
