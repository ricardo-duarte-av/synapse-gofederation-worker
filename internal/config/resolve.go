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

	// DatabaseDSN, StateDSN and WriteDSN are the connections the worker opens,
	// with the fallbacks already applied: our own dsn if set, otherwise
	// homeserver.yaml's `database:` block. Read these rather than
	// Config.Database.DSN, which is only what the file said.
	DatabaseDSN string
	StateDSN    string
	// WriteDSN is empty in shadow mode, and that is the guarantee: no writable
	// handle to Synapse's tables is constructed anywhere in the process.
	WriteDSN string

	// DatabaseFromSynapse records that DatabaseDSN was derived rather than
	// configured, which changes what a writable role means -- Synapse's own
	// role can write by definition, so require_read_only cannot hold.
	DatabaseFromSynapse bool
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

	if err := resolveDatabases(r, cfg, scfg); err != nil {
		return nil, err
	}

	return r, nil
}

// resolveDatabases decides the three connection strings.
//
// Ours wins where it is set; otherwise the homeserver's, for the same reason as
// everything else read from homeserver.yaml -- the worker reads Synapse's
// database to reproduce Synapse's decisions, and a second copy of where that
// database is can only ever be wrong later.
func resolveDatabases(r *Resolved, cfg *Config, scfg *synapsecfg.Config) error {
	derived := scfg.Database.DSN()

	r.DatabaseDSN = cfg.Database.DSN
	if r.DatabaseDSN == "" {
		if derived == "" {
			return fmt.Errorf(
				"config: database.dsn is unset and %s has no usable database block; "+
					"there is nothing to connect to", scfg.Path)
		}
		r.DatabaseDSN = derived
		r.DatabaseFromSynapse = true
	}

	// The derived connection is Synapse's own role, which writes to these
	// tables for a living. Saying require_read_only over it would be a check
	// that cannot pass, so it is refused at startup rather than at the first
	// query -- and the fix is named, since it is a different role, not a
	// different flag.
	if r.DatabaseFromSynapse && cfg.Database.RequireReadOnly {
		return fmt.Errorf(
			"config: database.require_read_only is set but the connection comes from %s, "+
				"which is Synapse's own read-write role; set database.dsn to a read-only "+
				"role (deploy/readonly-role.sql) or drop the requirement", scfg.Path)
	}

	// Empty state.dsn reuses whatever the Synapse connection resolved to. That
	// only works where that role can write to our schema, which is why the
	// deployments that mean it give state its own role.
	r.StateDSN = cfg.State.DSN
	if r.StateDSN == "" {
		r.StateDSN = r.DatabaseDSN
	}

	// In shadow mode this stays empty, and nothing writable is opened at all.
	if cfg.Mode == ModePrimary {
		r.WriteDSN = cfg.Database.WriteDSN
		if r.WriteDSN == "" {
			if derived == "" {
				return fmt.Errorf(
					"config: mode is primary but database.write_dsn is unset and %s has no "+
						"usable database block; a sender's bookkeeping is a set of deletions "+
						"and cannot be done read-only", scfg.Path)
			}
			r.WriteDSN = derived
		}
	}

	return nil
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
