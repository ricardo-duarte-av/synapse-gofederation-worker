package synapsecfg

import (
	"os"
	"path/filepath"
	"strings"
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

func TestDatabaseArgsBecomeALibpqDSN(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
database:
  name: psycopg2
  args:
    user: synapse
    password: "hunter2"
    database: synapse-db
    host: /var/sockets
    cp_min: 5
    cp_max: 10
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Database.Present {
		t.Fatal("database block was not read")
	}
	// cp_min and cp_max are Twisted pool sizes and must not leak into the DSN.
	want := "host=/var/sockets user=synapse password=hunter2 dbname=synapse-db"
	if got := cfg.Database.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
	if got := cfg.Database.Redacted(); got == want || !strings.Contains(got, "xxxxx") {
		t.Errorf("Redacted() = %q, want the password hidden", got)
	}
}

// psycopg2 takes either spelling, and a TCP host needs the port carried over.
func TestDatabaseAcceptsDBNameAndPort(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
database:
  name: psycopg2
  args:
    user: synapse
    dbname: synapse
    host: db.internal
    port: 6432
    sslmode: require
`))
	if err != nil {
		t.Fatal(err)
	}
	want := "host=db.internal port=6432 user=synapse dbname=synapse sslmode=require"
	if got := cfg.Database.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

// A password with a space or a quote in it is exactly what an unescaped
// keyword string gets wrong, and it fails at connect time with a message about
// the wrong keyword.
func TestDatabasePasswordIsQuoted(t *testing.T) {
	d := Database{Present: true, User: "u", DBName: "d", Password: `a b'c\d`}
	want := `user=u password='a b\'c\\d' dbname=d`
	if got := d.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestSqliteIsRefused(t *testing.T) {
	_, err := Load(writeConfig(t, minimal+"database:\n  name: sqlite3\n  args:\n    database: /data/homeserver.db\n"))
	if err == nil {
		t.Error("a sqlite3 database was accepted")
	}
}

func TestNoDatabaseBlockIsNotAnError(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Present || cfg.Database.DSN() != "" {
		t.Errorf("Database = %+v, want absent", cfg.Database)
	}
}

// homeserver.yaml names the key by the path inside SYNAPSE's container. When
// the whole config directory is mounted elsewhere read-only, the key is beside
// the homeserver.yaml we just read, and that is where the fallback looks.
func TestAbsoluteSigningKeyPathFallsBackBesideTheConfig(t *testing.T) {
	p := writeConfig(t, "server_name: \"example.com\"\nsigning_key_path: /data/signing.key\n")
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SigningKeys) != 2 {
		t.Fatalf("SigningKeys = %v", cfg.SigningKeys)
	}
	if cfg.SigningKeys[0].Version != "a_Yofy" {
		t.Errorf("first key = %q, want the one Synapse signs with", cfg.SigningKeys[0].Version)
	}
}

// The fallback only looks for the basename Synapse named, in the directory
// homeserver.yaml came from. A key that is in neither place is an error naming
// both, not a silently different key.
func TestSigningKeyMissingInBothPlacesNamesBoth(t *testing.T) {
	p := writeConfig(t, "server_name: \"example.com\"\nsigning_key_path: /data/other.signing.key\n")
	_, err := Load(p)
	if err == nil {
		t.Fatal("a missing signing key was accepted")
	}
	if !strings.Contains(err.Error(), "/data/other.signing.key") ||
		!strings.Contains(err.Error(), filepath.Dir(p)) {
		t.Errorf("error names only one location: %v", err)
	}
}

// YAML types a quoted port as a string, psycopg2 and redis-py both take one,
// and this deployment's test homeserver writes it that way. An int-typed field
// turned a working Synapse config into a startup failure whose message was
// about YAML types and said nothing about what to do.
func TestQuotedPortsAreAccepted(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal+`
database:
  name: psycopg2
  allow_unsafe_locale: true
  args:
    user: testingsynapse
    password: testingsynapse
    database: testingsynapse
    host: "matrix-postgres"
    port: "5432"
    cp_min: 5
    cp_max: 10
redis:
  enabled: true
  host: testing-redis
  port: "6379"
  dbid: "2"
instance_map:
  main:
    host: localhost
    port: "9093"
`))
	if err != nil {
		t.Fatal(err)
	}
	want := "host=matrix-postgres port=5432 user=testingsynapse " +
		"password=testingsynapse dbname=testingsynapse"
	if got := cfg.Database.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
	if cfg.Redis.Addr != "testing-redis:6379" {
		t.Errorf("Redis.Addr = %q", cfg.Redis.Addr)
	}
	if cfg.Redis.DBID != 2 {
		t.Errorf("Redis.DBID = %d, want 2", cfg.Redis.DBID)
	}
	if got := cfg.InstanceMap[MainProcessInstanceName].URL; got != "http://localhost:9093" {
		t.Errorf("instance_map URL = %q", got)
	}
}

// A port that is not a number at all is still an error, and it says which key.
func TestANonNumericPortIsAnError(t *testing.T) {
	_, err := Load(writeConfig(t, minimal+
		"database:\n  name: psycopg2\n  args:\n    database: d\n    port: \"not-a-port\"\n"))
	if err == nil {
		t.Fatal("a non-numeric port was accepted")
	}
	if !strings.Contains(err.Error(), "database.args.port") {
		t.Errorf("error does not name the key: %v", err)
	}
}

// The override is read literally: it exists to say where the file really is,
// so a fallback beside homeserver.yaml would defeat it. The error names the
// override as the thing to remove, because a path that came from the worker's
// own config looks identical in a log to one that came from Synapse's.
func TestAMissingOverrideNamesItself(t *testing.T) {
	p := writeConfig(t, minimal)
	_, err := LoadWithOptions(p, Options{SigningKeyPath: "/data/signing.key"})
	if err == nil {
		t.Fatal("a missing override was accepted")
	}
	if !strings.Contains(err.Error(), "OVERRIDE") {
		t.Errorf("error does not say the path was an override: %v", err)
	}
	// It must not have quietly found signing.key beside homeserver.yaml, which
	// is exactly what writeConfig puts there.
	if !strings.Contains(err.Error(), "/data/signing.key") {
		t.Errorf("error does not name the path that failed: %v", err)
	}
}
