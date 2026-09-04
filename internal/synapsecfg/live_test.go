package synapsecfg

import (
	"os"
	"testing"
)

// TestLiveHomeserverConfig parses the deployment's real homeserver.yaml.
//
// Skipped unless the env vars are set, per repo convention, so CI never needs
// the file. Run it against the live config after any change here:
//
//	HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml \
//	SIGNING_KEY=/opt/matrix/synapse/synapse/aguiarvieira.pt.signing.key \
//	go test ./internal/synapsecfg/ -run TestLive -v
func TestLiveHomeserverConfig(t *testing.T) {
	path := os.Getenv("HOMESERVER_YAML")
	if path == "" {
		t.Skip("set HOMESERVER_YAML to run against the real config")
	}
	cfg, err := LoadWithOptions(path, Options{SigningKeyPath: os.Getenv("SIGNING_KEY")})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName == "" {
		t.Error("no server_name")
	}
	if len(cfg.SenderInstances) == 0 {
		t.Error("no federation_sender_instances; this worker has no shard to shadow")
	}
	// Every configured sender should be reachable, or replication calls
	// addressed to it go nowhere. We make no such calls, but a mismatch here
	// is worth surfacing because it is invisible in Synapse's own logs.
	for _, name := range cfg.SenderInstances {
		if _, ok := cfg.InstanceMap[name]; !ok {
			t.Errorf("sender %q is not in instance_map", name)
		}
	}
	if os.Getenv("SIGNING_KEY") != "" {
		k, err := cfg.SigningKey()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("signing key %s (%d keys in file)", k.ID(), len(cfg.SigningKeys))
	}
	t.Logf("server_name=%s senders=%v redis=%+v retry=%+v",
		cfg.ServerName, cfg.SenderInstances, cfg.Redis, cfg.Retry)
}
