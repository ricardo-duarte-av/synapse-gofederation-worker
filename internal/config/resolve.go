package config

import (
	"fmt"
	"strings"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sharding"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/synapsecfg"
)

// Resolved is our config combined with Synapse's, with every value the worker
// actually runs on already decided.
//
// Keeping the merge in one place means the precedence rules are stated once and
// the rest of the worker never has to ask "is this the Synapse value or ours?".
type Resolved struct {
	*Config
	Synapse *synapsecfg.Config

	// ServerName is Synapse's, always. There is no override: signing with the
	// wrong origin produces transactions every remote server rejects.
	ServerName string

	// WorkerName is this process's own identity: what it calls itself on the
	// replication bus and what its cursors are keyed by. Never a real sender's
	// name.
	WorkerName string

	// ShardInstance is the sender whose shard we take, validated to be one of
	// Synapse's configured senders. It decides which destinations are ours and
	// nothing else.
	ShardInstance string

	// Shard answers which destinations are ours.
	Shard *sharding.Config

	// RedisAddress and RedisChannel are what the subscriber connects to.
	RedisAddress string
	RedisChannel string
}

// Resolve merges the worker config with Synapse's and validates the pair.
func Resolve(cfg *Config, scfg *synapsecfg.Config) (*Resolved, error) {
	r := &Resolved{
		Config:        cfg,
		Synapse:       scfg,
		ServerName:    scfg.ServerName,
		WorkerName:    cfg.WorkerName,
		ShardInstance: cfg.Shadow.Instance,
		Shard:         sharding.New(scfg.SenderInstances),
	}

	// The check this whole function exists for. An instance name that is not in
	// federation_sender_instances owns no destinations at all, so the worker
	// would start cleanly, consume replication, and decide that every single
	// event is somebody else's -- running perfectly while doing nothing. That
	// is the hardest failure to notice by watching it, so it is refused here.
	// The worker_name check INVERTS with the mode, and that inversion is the
	// clearest statement of what the two modes are.
	//
	// A shadow must not claim a real sender's name: the replication bus
	// suppresses a worker's own echo by instance name, so it would discard the
	// rows of the very sender it is shadowing and process nothing while looking
	// healthy.
	//
	// A primary must claim its own, because that name is what Synapse shards
	// on.
	inList := contains(scfg.SenderInstances, cfg.WorkerName)
	if cfg.Mode != ModePrimary && inList {
		return nil, fmt.Errorf(
			"config: worker_name %q is one of %s's federation_sender_instances; "+
				"a shadow must have an identity of its own",
			cfg.WorkerName, scfg.Path)
	}

	// In primary mode the shard we take is our own; in shadow mode it is the
	// sender being impersonated.
	if cfg.Mode == ModePrimary {
		r.ShardInstance = cfg.WorkerName
	}

	// The name we shard on has to be one Synapse actually shards to. The
	// consequence of it not being is the same in both modes -- we own no
	// destinations -- but what that means differs enough to be worth saying
	// separately: a shadow quietly compares nothing, a primary leaves the
	// homeserver federating with nobody.
	if !contains(scfg.SenderInstances, r.ShardInstance) {
		known := strings.Join(quoted(scfg.SenderInstances), ", ")
		if cfg.Mode == ModePrimary {
			return nil, fmt.Errorf(
				"config: mode is primary but worker_name %q is not in %s's "+
					"federation_sender_instances (%s); it would own no destinations and the "+
					"homeserver would federate with nobody",
				r.ShardInstance, scfg.Path, known)
		}
		return nil, fmt.Errorf(
			"config: shadow.instance %q is not in %s's federation_sender_instances (%s); "+
				"it would own no destinations and the worker would silently do nothing",
			r.ShardInstance, scfg.Path, known)
	}

	// Redis: ours wins if set, otherwise Synapse's. Synapse's is the normal
	// case -- the whole point of reading homeserver.yaml is not to restate it.
	switch {
	case cfg.Replication.Address != "":
		r.RedisAddress = cfg.Replication.Address
	case scfg.Redis.Socket != "":
		r.RedisAddress = scfg.Redis.Socket
	default:
		r.RedisAddress = scfg.Redis.Addr
	}
	if cfg.ReplicationEnabled() && r.RedisAddress == "" {
		return nil, fmt.Errorf(
			"config: replication is enabled but neither replication.address nor %s's redis block "+
				"gives an address", scfg.Path)
	}

	// The channel is named after server_name and there is no prefix setting
	// (replication/tcp/redis.py:399). Allowing an override is still worth it
	// for testing against a scratch bus, but the default is the only value
	// that works against a real one.
	r.RedisChannel = cfg.Replication.Channel
	if r.RedisChannel == "" {
		r.RedisChannel = scfg.ServerName
	}

	if len(scfg.SigningKeys) == 0 {
		return nil, fmt.Errorf("config: no signing keys were loaded")
	}

	return r, nil
}

// ShouldHandle reports whether this worker owns a destination.
func (r *Resolved) ShouldHandle(destination string) bool {
	return r.Shard.ShouldHandle(r.ShardInstance, destination)
}

// IsMine reports whether a user or room id belongs to this homeserver.
func (r *Resolved) IsMine(id string) bool {
	_, domain, ok := strings.Cut(id, ":")
	return ok && domain == r.ServerName
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

func quoted(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fmt.Sprintf("%q", s)
	}
	return out
}
