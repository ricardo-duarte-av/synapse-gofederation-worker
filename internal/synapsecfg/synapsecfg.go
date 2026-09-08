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

	// Database is Synapse's own PostgreSQL connection. Read here so the worker
	// does not carry a second, drifting copy of where the homeserver's data
	// actually lives; gofederation-worker.yaml's database.dsn overrides it.
	Database Database

	// ClientTimeout, MaxLongRetries and MaxLongRetryDelay bound ONE outbound
	// request. Distinct from Retry, which is the per-destination backoff that
	// persists across transactions: these govern a single PUT to /send, which
	// Synapse issues with long_retries=True (transport/client.py:257).
	ClientTimeout     time.Duration
	MaxLongRetries    int
	MaxLongRetryDelay time.Duration

	// TrackPresence is Synapse's config.server.track_presence.
	//
	// Read because Synapse's sender refuses to send presence when it is off,
	// regardless of what reaches it (federation/sender/__init__.py:978). In
	// normal operation no presence rows are produced either, so this is a
	// second line rather than the only one -- but the case it covers is real:
	// presence_enabled "untracked" leaves presence working for CLIENTS while
	// forbidding it over federation, and a config change does not empty a queue
	// that already has presence in it.
	TrackPresence bool

	// AllowDeviceNameLookup is allow_device_name_lookup_over_federation. It
	// decides whether a device list update may carry device_display_name, so
	// reading it rather than assuming is the difference between matching
	// Synapse's EDU and leaking a display name it would have withheld.
	AllowDeviceNameLookup bool

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
		Port any    `yaml:"port"`
		TLS  bool   `yaml:"tls"`
	} `yaml:"instance_map"`

	Redis struct {
		Enabled  *bool  `yaml:"enabled"`
		Path     string `yaml:"path"`
		Host     string `yaml:"host"`
		Port     any    `yaml:"port"`
		Password string `yaml:"password"`
		DBID     any    `yaml:"dbid"`
	} `yaml:"redis"`

	Database struct {
		Name string `yaml:"name"`
		Args struct {
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			// psycopg2 accepts either spelling; Synapse's own documented
			// example uses `database`.
			Database string `yaml:"database"`
			DBName   string `yaml:"dbname"`
			Host     string `yaml:"host"`
			Port     any    `yaml:"port"`
			SSLMode  string `yaml:"sslmode"`
		} `yaml:"args"`
	} `yaml:"database"`

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

	// Presence.Enabled is a bool OR the string "untracked", and falls back to
	// the legacy use_presence key (config/server.py:507).
	Presence struct {
		Enabled any `yaml:"enabled"`
	} `yaml:"presence"`
	UsePresence any `yaml:"use_presence"`

	FederationDomainWhitelist []string `yaml:"federation_domain_whitelist"`
	AllowDeviceNameLookup     *bool    `yaml:"allow_device_name_lookup_over_federation"`
}

// Options adjusts how homeserver.yaml is resolved.
type Options struct {
	// SigningKeyPath overrides signing_key_path.
	//
	// Rarely needed now: homeserver.yaml's value is a path inside Synapse's own
	// container (/data/<server_name>.signing.key on this deployment), and a
	// deployment that mounts Synapse's config directory read-only gets the key
	// found beside homeserver.yaml without saying anything -- see
	// readSigningKeyFile. This remains for the case the directory is NOT
	// mounted whole and the key really is somewhere else, which is mostly
	// host-side tools and tests.
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
	if cfg.Database, err = database(r); err != nil {
		return nil, err
	}

	cfg.Redis = Redis{
		Enabled:  r.Redis.Enabled != nil && *r.Redis.Enabled,
		Socket:   r.Redis.Path,
		Password: r.Redis.Password,
	}
	if cfg.Redis.DBID, err = parseInt(r.Redis.DBID, "redis.dbid"); err != nil {
		return nil, err
	}
	if cfg.Redis.Socket == "" && r.Redis.Host != "" {
		port, err := parseInt(r.Redis.Port, "redis.port")
		if err != nil {
			return nil, err
		}
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

	cfg.TrackPresence = trackPresence(r)

	// Defaults to false in Synapse (config/federation.py), so an absent key
	// means device names are NOT sent.
	cfg.AllowDeviceNameLookup = r.AllowDeviceNameLookup != nil && *r.AllowDeviceNameLookup

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
		port, err := parseInt(loc.Port, fmt.Sprintf("instance_map.%s.port", name))
		if err != nil {
			return nil, err
		}
		inst := Instance{Name: name}
		switch {
		case loc.Path != "":
			inst.Socket = loc.Path
		case loc.Host != "" && port != 0:
			scheme := "http"
			if loc.TLS {
				scheme = "https"
			}
			inst.URL = fmt.Sprintf("%s://%s:%d", scheme, loc.Host, port)
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
//
// An ABSOLUTE path from homeserver.yaml gets a second chance beside it. The
// path in that file is the one inside SYNAPSE's container --
// /data/<server_name>.signing.key here -- while this worker mounts Synapse's
// config directory somewhere of its own (/etc/synapse). Since the key lives in
// that same directory, looking for its basename next to the homeserver.yaml we
// just read finds it, and a deployment that mounts the directory read-only says
// where the key is exactly once: in Synapse's own config.
//
// An OVERRIDE gets no such fallback and is read literally. It exists to say
// where the file really is, so guessing at a second location would defeat it --
// and the failure is then a plain "that path is not there", naming the override
// as the thing to remove.
func loadSigningKeys(r raw, cfgPath, override string) ([]SigningKey, error) {
	body := r.SigningKey
	// An explicit override wins even over an inline signing_key, so a
	// deployment can always say where the file really is.
	if override != "" {
		body = ""
	}
	if body == "" {
		if override != "" {
			b, err := os.ReadFile(override)
			if err != nil {
				return nil, fmt.Errorf(
					"synapsecfg: reading signing key: %w; this path is the signing_key_path "+
						"OVERRIDE, not %s's -- remove it to use the key Synapse itself signs with",
					err, cfgPath)
			}
			body = string(b)
		} else {
			p := r.SigningKeyPath
			if p == "" {
				// Synapse's default is "<server_name>.signing.key" in the
				// config directory.
				p = r.ServerName + ".signing.key"
			}
			b, err := readSigningKeyFile(p, cfgPath)
			if err != nil {
				return nil, err
			}
			body = string(b)
		}
	}
	keys, err := ParseSigningKeys(body)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: %w", err)
	}
	return keys, nil
}

// readSigningKeyFile resolves homeserver.yaml's own signing_key_path and reads
// it. Overrides do not come through here; see loadSigningKeys.
//
// The fallback only ever looks in the directory homeserver.yaml itself came
// from, and only for the basename Synapse's own config named. It cannot pick up
// some other key: if that file is not there, the original error is reported,
// naming both paths, because "no such file: /data/x.signing.key" from inside a
// container that has no /data is a confusing thing to be told.
func readSigningKeyFile(p, cfgPath string) ([]byte, error) {
	dir := filepath.Dir(cfgPath)
	resolved := p
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(dir, resolved)
	}
	b, err := os.ReadFile(resolved)
	if err == nil {
		return b, nil
	}
	if !os.IsNotExist(err) || !filepath.IsAbs(p) {
		return nil, fmt.Errorf("synapsecfg: reading signing key: %w", err)
	}
	beside := filepath.Join(dir, filepath.Base(p))
	if beside == resolved {
		return nil, fmt.Errorf("synapsecfg: reading signing key: %w", err)
	}
	b, err2 := os.ReadFile(beside)
	if err2 != nil {
		return nil, fmt.Errorf(
			"synapsecfg: reading signing key: %w (and not at %s, beside %s)",
			err, beside, cfgPath)
	}
	return b, nil
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

// parseInt accepts a number or a string holding one.
//
// Ports and dbids are the fields this exists for. YAML makes `port: 5432` an
// int and `port: "5432"` a string, psycopg2 and redis-py take either, and this
// deployment's own test homeserver quotes it -- so an int-typed field turns a
// working Synapse config into a startup failure with a message about YAML
// types, which says nothing about what to do.
func parseInt(v any, what string) (int, error) {
	switch t := v.(type) {
	case nil:
		return 0, nil
	case int:
		return t, nil
	case float64:
		return int(t), nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("synapsecfg: %s: %q is not a number", what, t)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("synapsecfg: %s: expected a number, got %T", what, v)
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

// Database is Synapse's PostgreSQL connection, from the `database:` block.
//
// Read rather than restated for the same reason as everything else here: it is
// the database this worker must read to produce the same transactions as the
// sender it shadows. A separate copy that drifts -- a renamed database, a moved
// socket, a rotated password -- fails at connect time on a good day and reads a
// stale replica on a bad one.
//
// Only the psycopg2 backend is understood. Synapse's sqlite3 backend is not a
// database this worker can share.
type Database struct {
	// Present is false when homeserver.yaml has no usable `database:` block,
	// which is not by itself an error: gofederation-worker.yaml may carry a
	// full dsn of its own.
	Present bool

	User     string
	Password string
	DBName   string
	// Host is a hostname or, when it starts with a slash, the directory
	// holding the unix socket -- libpq and psycopg2 spell that the same way.
	Host    string
	Port    int
	SSLMode string
}

// DSN renders the connection in libpq keyword form, which is what pgx takes.
//
// Keyword form rather than a URI because a unix-socket directory as the host
// (host=/var/sockets) needs percent-encoding in a URI and does not here, and
// because it is the form the sibling workers already use.
func (d Database) DSN() string {
	if !d.Present {
		return ""
	}
	var b strings.Builder
	add := func(k, v string) {
		if v == "" {
			return
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(quoteDSNValue(v))
	}
	add("host", d.Host)
	if d.Port != 0 {
		add("port", strconv.Itoa(d.Port))
	}
	add("user", d.User)
	add("password", d.Password)
	add("dbname", d.DBName)
	add("sslmode", d.SSLMode)
	return b.String()
}

// Redacted is DSN with the password replaced, for logs and error messages.
func (d Database) Redacted() string {
	r := d
	if r.Password != "" {
		r.Password = "xxxxx"
	}
	return r.DSN()
}

// quoteDSNValue escapes a value for libpq keyword/value form: single quotes
// around anything with whitespace or a quote in it, backslash-escaping the
// quote and the backslash. Passwords are the reason this exists.
func quoteDSNValue(v string) string {
	if !strings.ContainsAny(v, " \t\n\r'\\") {
		return v
	}
	var b strings.Builder
	b.WriteByte('\'')
	for _, c := range v {
		if c == '\'' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(c)
	}
	b.WriteByte('\'')
	return b.String()
}

// database resolves the `database:` block.
//
// Synapse hands `args` straight to psycopg2.connect, so the keys are
// psycopg2's. It accepts both `database` and `dbname` for the database name,
// and so do we. cp_min and cp_max are Twisted pool sizes and are deliberately
// ignored: our pool is sized by database.max_conns.
func database(r raw) (Database, error) {
	name := r.Database.Name
	if name == "" && r.Database.Args.Host == "" && r.Database.Args.Database == "" &&
		r.Database.Args.DBName == "" {
		return Database{}, nil
	}
	if name != "" && name != "psycopg2" {
		// sqlite3 is a file this worker has no business opening alongside a
		// running Synapse, and there is no third backend.
		return Database{}, fmt.Errorf(
			"synapsecfg: database.name is %q; only psycopg2 is supported", name)
	}
	port, err := parseInt(r.Database.Args.Port, "database.args.port")
	if err != nil {
		return Database{}, err
	}
	db := Database{
		Present:  true,
		User:     r.Database.Args.User,
		Password: r.Database.Args.Password,
		DBName:   r.Database.Args.Database,
		Host:     r.Database.Args.Host,
		Port:     port,
		SSLMode:  r.Database.Args.SSLMode,
	}
	if db.DBName == "" {
		db.DBName = r.Database.Args.DBName
	}
	if db.DBName == "" {
		return Database{}, fmt.Errorf(
			"synapsecfg: database.args names no database (neither `database` nor `dbname`)")
	}
	return db, nil
}

// trackPresence reproduces config/server.py:507.
//
//	presence_enabled = presence.enabled, else use_presence, else true
//	presence_enabled_at_all = bool(presence_enabled)
//	track_presence = presence_enabled_at_all and presence_enabled != "untracked"
//
// The string "untracked" is the reason this is not a bool in the yaml: Python's
// bool("untracked") is TRUE, so presence stays on for clients while federation
// of it is off. Reading the key as a plain bool would get that case exactly
// backwards.
func trackPresence(r raw) bool {
	v := r.Presence.Enabled
	if v == nil {
		v = r.UsePresence
	}
	if v == nil {
		// Synapse's default for the legacy key.
		return true
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		if t == "untracked" {
			return false
		}
		// Python truthiness: any non-empty string is true.
		return t != ""
	case int:
		return t != 0
	case float64:
		return t != 0
	default:
		return true
	}
}
