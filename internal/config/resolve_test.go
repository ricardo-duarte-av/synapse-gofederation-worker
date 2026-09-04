package config

import (
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/synapsecfg"
)

func synapse() *synapsecfg.Config {
	keys, err := synapsecfg.ParseSigningKeys("ed25519 a_Yofy AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8\n")
	if err != nil {
		panic(err)
	}
	return &synapsecfg.Config{
		Path:       "/etc/synapse/homeserver.yaml",
		ServerName: "aguiarvieira.pt",
		SenderInstances: []string{
			"av-federation-sender-worker-1",
			"av-federation-sender-worker-2",
		},
		SigningKeys: keys,
		Redis:       synapsecfg.Redis{Enabled: true, Socket: "/var/sockets/keydb"},
	}
}

func worker(t *testing.T, extra string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(minimal + extra))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestResolveTakesSynapsesValues(t *testing.T) {
	r, err := Resolve(worker(t, ""), synapse())
	if err != nil {
		t.Fatal(err)
	}
	if r.ServerName != "aguiarvieira.pt" {
		t.Errorf("ServerName = %q", r.ServerName)
	}
	if r.RedisAddress != "/var/sockets/keydb" {
		t.Errorf("RedisAddress = %q, want Synapse's socket", r.RedisAddress)
	}
	// The channel is named after server_name; there is no prefix setting.
	if r.RedisChannel != "aguiarvieira.pt" {
		t.Errorf("RedisChannel = %q, want server_name", r.RedisChannel)
	}
}

func TestResolveOverridesWin(t *testing.T) {
	r, err := Resolve(worker(t, "\nreplication:\n  address: localhost:6379\n  channel: scratch\n"), synapse())
	if err != nil {
		t.Fatal(err)
	}
	if r.RedisAddress != "localhost:6379" || r.RedisChannel != "scratch" {
		t.Errorf("overrides ignored: addr=%q channel=%q", r.RedisAddress, r.RedisChannel)
	}
}

// The check this file exists for.
func TestUnknownShadowInstanceIsRefused(t *testing.T) {
	cfg := worker(t, "")
	cfg.Shadow.Instance = "av-federation-sender-worker-9"
	_, err := Resolve(cfg, synapse())
	if err == nil {
		t.Fatal("expected an error for an instance that is not a configured sender")
	}
	// The message must name the valid options, because the whole failure mode
	// is that this is otherwise invisible.
	if !strings.Contains(err.Error(), "av-federation-sender-worker-1") {
		t.Errorf("error does not list the configured senders: %v", err)
	}
}

func TestShouldHandlePartitionsAllDestinations(t *testing.T) {
	scfg := synapse()
	cfg1, cfg2 := worker(t, ""), worker(t, "")
	cfg2.Shadow.Instance = "av-federation-sender-worker-2"

	r1, err := Resolve(cfg1, scfg)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Resolve(cfg2, scfg)
	if err != nil {
		t.Fatal(err)
	}

	// Every destination belongs to exactly one of the two shards. If both or
	// neither claimed one, the shadow would either double-count or drop it.
	for _, d := range []string{"matrix.org", "maunium.net", "t2bot.io", "element.io", "beeper.com"} {
		if r1.ShouldHandle(d) == r2.ShouldHandle(d) {
			t.Errorf("%q is claimed by both shards or by neither", d)
		}
	}
}

func TestReplicationEnabledWithNoAddressIsRefused(t *testing.T) {
	scfg := synapse()
	scfg.Redis = synapsecfg.Redis{}
	if _, err := Resolve(worker(t, ""), scfg); err == nil {
		t.Fatal("expected an error when replication is on and nothing gives an address")
	}
}

func TestIsMine(t *testing.T) {
	r, err := Resolve(worker(t, ""), synapse())
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]bool{
		"@alice:aguiarvieira.pt": true,
		"@bob:matrix.org":        false,
		"!room:aguiarvieira.pt":  true,
		"nocolon":                false,
		// The domain is everything after the FIRST colon, matching Synapse's
		// get_domain_from_id.
		"@a:aguiarvieira.pt:8448": false,
	} {
		if got := r.IsMine(id); got != want {
			t.Errorf("IsMine(%q) = %v, want %v", id, got, want)
		}
	}
}
