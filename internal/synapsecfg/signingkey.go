package synapsecfg

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
)

// ParseSigningKeys reads signedjson's signing key file format:
//
//	<algorithm> <version> <unpadded-base64 32-byte seed>
//
// one key per line. Synapse signs with the FIRST key and treats the rest as
// additional keys it publishes and accepts (config/key.py:127); this
// deployment's file has two lines, so taking the last one -- or sorting -- would
// sign every transaction with a key remote servers would reject.
func ParseSigningKeys(body string) ([]SigningKey, error) {
	var keys []SigningKey
	for i, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf(
				"signing key line %d has %d fields, want 3 (algorithm, version, seed)", i+1, len(fields))
		}
		algorithm, version, encoded := fields[0], fields[1], fields[2]
		if algorithm != "ed25519" {
			return nil, fmt.Errorf("signing key line %d: unsupported algorithm %q", i+1, algorithm)
		}
		// The encoding is unpadded standard base64, which is how signedjson
		// writes it. RawStdEncoding rejects the padding rather than silently
		// accepting either form, so a file in the wrong dialect is reported
		// here and not as an invalid signature months later.
		seed, err := base64.RawStdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("signing key line %d: decoding seed: %w", i+1, err)
		}
		if len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf(
				"signing key line %d: seed is %d bytes, want %d", i+1, len(seed), ed25519.SeedSize)
		}
		keys = append(keys, SigningKey{Algorithm: algorithm, Version: version, Seed: seed})
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("signing key file contains no keys")
	}
	return keys, nil
}

// PrivateKey builds the ed25519 private key from the seed.
func (k SigningKey) PrivateKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(k.Seed)
}
