// Package synapsecfg reads the facts we need out of Synapse's own
// homeserver.yaml, at every start.
//
// The alternative is copying them into gofederation-worker.yaml, and for this
// worker that would be worse than merely redundant. `federation_sender_instances`
// is not a value we consume, it is the value that DEFINES which destinations
// belong to us: the shard is `sha256(dest) mod len(instances)`, so a list that
// has drifted by one entry silently reassigns every destination on the server.
// A stale copy would not fail, it would produce a complete, confident, wrong
// answer -- and in shadow mode, an answer nobody is checking against reality.
//
// So Synapse's config is the single source of truth and we re-read it on every
// start. Nothing here is cached, and nothing is ever written back.
//
// The resolution rules are Synapse's, from synapse/config/workers.py,
// synapse/config/key.py and synapse/config/federation.py.
package synapsecfg

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// MainProcessInstanceName is what Synapse calls the main process internally.
// The yaml spells it "main" in instance_map and Synapse rewrites it to
// "master" (config/workers.py:73,338); federation_sender_instances is compared
// against the rewritten form.
const MainProcessInstanceName = "master"

// Synapse's federation retry defaults, from synapse/config/federation.py:83.
// This deployment overrides all three to values far tighter than these.
const (
	DefaultDestinationMinRetryInterval = 10 * time.Minute
	DefaultDestinationRetryMultiplier  = 2.0
	DefaultDestinationMaxRetryInterval = 7 * 24 * time.Hour
)

// SigningKey is one entry from the signing key file.
type SigningKey struct {
	// Algorithm is always "ed25519" in practice.
	Algorithm string
	// Version is the key's short name; the key id is Algorithm + ":" + Version.
	Version string
	// Seed is the 32-byte ed25519 seed, already base64-decoded.
	Seed []byte
}

// ID is the key id as it appears in a signature block, e.g. "ed25519:a_Yofy".
func (k SigningKey) ID() string { return k.Algorithm + ":" + k.Version }

// Instance is where one Synapse instance can be reached for HTTP replication.
type Instance struct {
	Name string
	// Socket is set for a unix-socket instance, URL for a TCP one. Exactly one
	// is ever non-empty.
	Socket string
	URL    string
}

// Redis is how to reach the replication bus.
type Redis struct {
	Enabled bool
	// Socket is redis.path, Addr is host:port. Exactly one is set.
	Socket   string
	Addr     string
	Password string
	DBID     int
}

// Retry is the per-destination backoff tuning from the `federation:` block.
type Retry struct {
	MinInterval time.Duration
	Multiplier  float64
	MaxInterval time.Duration
}

// Synapse's per-request retry defaults for outbound federation, from
// synapse/config/federation.py:67. This deployment tightens all of them.
const (
	DefaultClientTimeout     = 60 * time.Second
	DefaultMaxLongRetries    = 10
	DefaultMaxLongRetryDelay = 60 * time.Second
)

// Config is the subset of homeserver.yaml this worker needs.
type Config struct {
	// Path is the file this was read from, kept for error messages and for
	// resolving relative paths within it.
	Path string

	ServerName string

	// SenderInstances is federation_sender_instances IN FILE ORDER. Order is
	// load-bearing: it is indexed by the shard hash, so sorting it would
	// reshard the homeserver.
	SenderInstances []string

	// SigningKeys are the keys from signing_key_path, in file order. Synapse
	// signs with the first and publishes the rest.
	SigningKeys []SigningKey

	// InstanceMap is every named instance's replication address. We do not
	// make replication calls, but it is read so startup can say something
	// useful when a configured sender has no address at all.
	InstanceMap map[string]Instance

	Redis Redis
	Retry Retry

	// ClientTimeout, MaxLongRetries and MaxLongRetryDelay bound ONE outbound
	// request. Distinct from Retry, which is the per-destination backoff that
	// persists across transactions: these govern a single PUT to /send, which
	// Synapse issues with long_retries=True (transport/client.py:257).
	ClientTimeout     time.Duration
	MaxLongRetries    int
	MaxLongRetryDelay time.Duration

	// DomainWhitelist is federation_domain_whitelist. Nil means no whitelist
	// (send to everyone); an empty non-nil map means send to nobody, which is
	// a configuration Synapse allows and we must not confuse with the former.
	DomainWhitelist map[string]bool
}

// SigningKey returns the key Synapse would sign with: the first in the file.
func (c *Config) SigningKey() (SigningKey, error) {
	if len(c.SigningKeys) == 0 {
		return SigningKey{}, fmt.Errorf("synapsecfg: no signing keys were loaded from %s", c.Path)
	}
	return c.SigningKeys[0], nil
}

// raw mirrors only the keys we read. Everything else in the file is ignored, so
// a Synapse upgrade that adds options cannot stop this parsing.
type raw struct {
	ServerName     string `yaml:"server_name"`
	SigningKeyPath string `yaml:"signing_key_path"`
	SigningKey     string `yaml:"signing_key"`

	// String or list, per _instance_to_list_converter.
	FederationSenderInstances any   `yaml:"federation_sender_instances"`
	SendFederation            *bool `yaml:"send_federation"`

	InstanceMap map[string]struct {
		Path string `yaml:"path"`
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		TLS  bool   `yaml:"tls"`
	} `yaml:"instance_map"`

	Redis struct {
		Enabled  *bool  `yaml:"enabled"`
		Path     string `yaml:"path"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Password string `yaml:"password"`
		DBID     int    `yaml:"dbid"`
	} `yaml:"redis"`

	Federation struct {
		DestinationMinRetryInterval any     `yaml:"destination_min_retry_interval"`
		DestinationRetryMultiplier  float64 `yaml:"destination_retry_multiplier"`
		DestinationMaxRetryInterval any     `yaml:"destination_max_retry_interval"`
		ClientTimeout               any     `yaml:"client_timeout"`
		MaxLongRetries              *int    `yaml:"max_long_retries"`
		MaxLongRetryDelay           any     `yaml:"max_long_retry_delay"`
		MaxShortRetries             *int    `yaml:"max_short_retries"`
		MaxShortRetryDelay          any     `yaml:"max_short_retry_delay"`
	} `yaml:"federation"`

	FederationDomainWhitelist []string `yaml:"federation_domain_whitelist"`
}

// Options adjusts how homeserver.yaml is resolved.
type Options struct {
	// SigningKeyPath overrides signing_key_path.
	//
	// Needed because homeserver.yaml's value is a path inside Synapse's own
	// container (/data/<server_name>.signing.key on this deployment), and the
	// Go workers mount the same file at a path of their own -- media-worker
	// uses /data/signing.key. Without an override, every host-side tool and
	// every container with a different layout would fail at startup on a path
	// that is perfectly correct for Synapse.
	SigningKeyPath string
}

// Load reads and resolves homeserver.yaml with default options.
func Load(path string) (*Config, error) { return LoadWithOptions(path, Options{}) }

// LoadWithOptions reads and resolves homeserver.yaml.
func LoadWithOptions(path string, opts Options) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: %w", err)
	}
	var r raw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("synapsecfg: parsing %s: %w", path, err)
	}
	if r.ServerName == "" {
		return nil, fmt.Errorf("synapsecfg: %s has no server_name", path)
	}

	cfg := &Config{
		Path:       path,
		ServerName: r.ServerName,
		Retry: Retry{
			MinInterval: DefaultDestinationMinRetryInterval,
			Multiplier:  DefaultDestinationRetryMultiplier,
			MaxInterval: DefaultDestinationMaxRetryInterval,
		},
		ClientTimeout:     DefaultClientTimeout,
		MaxLongRetries:    DefaultMaxLongRetries,
		MaxLongRetryDelay: DefaultMaxLongRetryDelay,
	}

	if cfg.SenderInstances, err = senderInstances(r); err != nil {
		return nil, err
	}
	if cfg.InstanceMap, err = instanceMap(r); err != nil {
		return nil, err
	}
	if cfg.SigningKeys, err = loadSigningKeys(r, path, opts.SigningKeyPath); err != nil {
		return nil, err
	}

	cfg.Redis = Redis{
		Enabled:  r.Redis.Enabled != nil && *r.Redis.Enabled,
		Socket:   r.Redis.Path,
		Password: r.Redis.Password,
		DBID:     r.Redis.DBID,
	}
	if cfg.Redis.Socket == "" && r.Redis.Host != "" {
		port := r.Redis.Port
		if port == 0 {
			port = 6379
		}
		cfg.Redis.Addr = fmt.Sprintf("%s:%d", r.Redis.Host, port)
	}

	if d, ok, err := parseDuration(r.Federation.DestinationMinRetryInterval); err != nil {
		return nil, fmt.Errorf("synapsecfg: federation.destination_min_retry_interval: %w", err)
	} else if ok {
		cfg.Retry.MinInterval = d
	}
	if d, ok, err := parseDuration(r.Federation.DestinationMaxRetryInterval); err != nil {
		return nil, fmt.Errorf("synapsecfg: federation.destination_max_retry_interval: %w", err)
	} else if ok {
		cfg.Retry.MaxInterval = d
	}
	if r.Federation.DestinationRetryMultiplier != 0 {
		cfg.Retry.Multiplier = r.Federation.DestinationRetryMultiplier
	}
	if d, ok, err := parseDuration(r.Federation.ClientTimeout); err != nil {
		return nil, fmt.Errorf("synapsecfg: federation.client_timeout: %w", err)
	} else if ok {
		cfg.ClientTimeout = d
	}
	if d, ok, err := parseDuration(r.Federation.MaxLongRetryDelay); err != nil {
		return nil, fmt.Errorf("synapsecfg: federation.max_long_retry_delay: %w", err)
	} else if ok {
		cfg.MaxLongRetryDelay = d
	}
	if r.Federation.MaxLongRetries != nil {
		cfg.MaxLongRetries = *r.Federation.MaxLongRetries
	}

	// Nil and empty mean opposite things here, so the map is only allocated
	// when the key was actually present.
	if r.FederationDomainWhitelist != nil {
		cfg.DomainWhitelist = make(map[string]bool, len(r.FederationDomainWhitelist))
		for _, d := range r.FederationDomainWhitelist {
			cfg.DomainWhitelist[d] = true
		}
	}

	return cfg, nil
}

// senderInstances reproduces _worker_names_performing_this_duty
// (config/workers.py:598) for federation sending.
//
// When federation_sender_instances is absent entirely, Synapse falls back to
// ["master"] if send_federation is truthy (its default) and [] otherwise. The
// empty case is not an error: it means federation sending is switched off, and
// should_handle then answers false for everything.
func senderInstances(r raw) ([]string, error) {
	names, err := instanceList(r.FederationSenderInstances)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: federation_sender_instances: %w", err)
	}
	if r.FederationSenderInstances != nil {
		return names, nil
	}
	if r.SendFederation == nil || *r.SendFederation {
		return []string{MainProcessInstanceName}, nil
	}
	return nil, nil
}

func instanceMap(r raw) (map[string]Instance, error) {
	out := make(map[string]Instance, len(r.InstanceMap))
	for name, loc := range r.InstanceMap {
		// Synapse rewrites the yaml's "main" to the internal "master", and
		// federation_sender_instances is matched against the rewritten name.
		if name == "main" {
			name = MainProcessInstanceName
		}
		inst := Instance{Name: name}
		switch {
		case loc.Path != "":
			inst.Socket = loc.Path
		case loc.Host != "" && loc.Port != 0:
			scheme := "http"
			if loc.TLS {
				scheme = "https"
			}
			inst.URL = fmt.Sprintf("%s://%s:%d", scheme, loc.Host, loc.Port)
		default:
			return nil, fmt.Errorf(
				"synapsecfg: instance_map entry for %q has neither a path nor a host and port", name)
		}
		out[name] = inst
	}
	return out, nil
}

// loadSigningKeys reads signing_key_path, or the inline signing_key.
//
// Format is signedjson's read_signing_keys: one key per line,
// "<algorithm> <version> <unpadded-base64 32-byte seed>". A relative path is
// resolved against homeserver.yaml's directory, which is how Synapse's
// config-directory handling behaves for the deployments this runs in.
func loadSigningKeys(r raw, cfgPath, override string) ([]SigningKey, error) {
	body := r.SigningKey
	// An explicit override wins even over an inline signing_key, so a
	// deployment can always say where the file really is.
	if override != "" {
		body = ""
	}
	if body == "" {
		p := override
		if p == "" {
			p = r.SigningKeyPath
		}
		if p == "" {
			// Synapse's default is "<server_name>.signing.key" in the config
			// directory.
			p = r.ServerName + ".signing.key"
		}
		if !filepath.IsAbs(p) {
			p = filepath.Join(filepath.Dir(cfgPath), p)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("synapsecfg: reading signing key: %w", err)
		}
		body = string(b)
	}
	keys, err := ParseSigningKeys(body)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: %w", err)
	}
	return keys, nil
}

// instanceList accepts Synapse's string-or-list, per _instance_to_list_converter.
func instanceList(v any) ([]string, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{t}, nil
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("expected instance names, got %T", e)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected a string or a list, got %T", v)
	}
}

var durationUnits = map[byte]time.Duration{
	's': time.Second,
	'm': time.Minute,
	'h': time.Hour,
	'd': 24 * time.Hour,
	'w': 7 * 24 * time.Hour,
	'y': 365 * 24 * time.Hour,
}

// parseDuration is Synapse's parse_duration: a bare number is MILLISECONDS, and
// a string may carry an s/m/h/d/w/y suffix.
//
// The millisecond default is the trap. `destination_min_retry_interval: 60`
// means sixty milliseconds to Synapse, not sixty seconds.
func parseDuration(v any) (time.Duration, bool, error) {
	switch t := v.(type) {
	case nil:
		return 0, false, nil
	case int:
		return time.Duration(t) * time.Millisecond, true, nil
	case float64:
		return time.Duration(t) * time.Millisecond, true, nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false, nil
		}
		unit := time.Millisecond
		if u, ok := durationUnits[s[len(s)-1]]; ok {
			unit = u
			s = s[:len(s)-1]
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return 0, false, fmt.Errorf("%q is not a duration", t)
		}
		return time.Duration(n * float64(unit)), true, nil
	default:
		return 0, false, fmt.Errorf("expected a number or a string, got %T", v)
	}
}
