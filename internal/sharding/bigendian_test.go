package sharding

import (
	"crypto/sha256"
	"math/big"
)

// bigEndianInstance is the wrong implementation, kept so a test can assert that
// it really is distinguishable from the right one. It is never used outside
// tests.
func bigEndianInstance(instances []string, key string) string {
	sum := sha256.Sum256([]byte(key))
	n := new(big.Int).SetBytes(sum[:])
	idx := new(big.Int).Mod(n, big.NewInt(int64(len(instances)))).Int64()
	return instances[idx]
}
