// Package sharding decides which federation sender instance owns a destination.
//
// This reproduces Synapse's ShardedWorkerHandlingConfig
// (synapse/config/_base.py:1054) exactly, because it has to: the whole point of
// running this worker in shadow mode is to compare its decisions against a real
// sender's, and a sharding function that is merely close would put every
// destination on the wrong shard while still looking like plausible output.
package sharding

import (
	"crypto/sha256"
	"math/big"
)

// Config maps destinations to instances.
type Config struct {
	// Instances is federation_sender_instances in CONFIG ORDER. Sorting it
	// would silently reshard the whole homeserver, so it is never sorted.
	instances []string
}

// New builds a Config from the ordered instance list.
//
// An empty list is allowed and means "some other worker handles this", matching
// should_handle's early return rather than _get_instance's exception: Synapse
// treats an unconfigured shard as somebody else's business, not an error.
func New(instances []string) *Config {
	cp := make([]string, len(instances))
	copy(cp, instances)
	return &Config{instances: cp}
}

// Instances returns the instance list, in order.
func (c *Config) Instances() []string {
	out := make([]string, len(c.instances))
	copy(out, c.instances)
	return out
}

// ShouldHandle reports whether instanceName is responsible for key.
func (c *Config) ShouldHandle(instanceName, key string) bool {
	if len(c.instances) == 0 {
		return false
	}
	return c.GetInstance(key) == instanceName
}

// GetInstance returns the instance responsible for key, or "" if none are
// configured.
//
// The hash is read LITTLE-endian. Synapse writes
//
//	int.from_bytes(sha256(key).digest(), byteorder="little")
//
// and big.Int reads big-endian, so the digest is reversed first. This is the
// single most consequential line in the worker: big-endian here would produce a
// uniform, stable, entirely wrong partition, and every downstream comparison
// would disagree with Synapse for a reason that looks like a bug somewhere else.
func (c *Config) GetInstance(key string) string {
	switch len(c.instances) {
	case 0:
		return ""
	case 1:
		// Short-circuited by Synapse before hashing. Kept explicit because it
		// is also the answer the modulo would give, and a reader should not
		// have to check that.
		return c.instances[0]
	}

	sum := sha256.Sum256([]byte(key))
	le := make([]byte, len(sum))
	for i, b := range sum {
		le[len(sum)-1-i] = b
	}

	n := new(big.Int).SetBytes(le)
	idx := new(big.Int).Mod(n, big.NewInt(int64(len(c.instances)))).Int64()
	return c.instances[idx]
}
