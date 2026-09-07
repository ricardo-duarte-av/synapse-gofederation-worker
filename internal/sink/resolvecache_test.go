package sink

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/federation"
)

func testResolveCache(t *testing.T) (*resolveCache, *time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	c := newResolveCache()
	c.now = func() time.Time { return now }
	c.jitter = func() float64 { return 1 } // deterministic
	return c, &now
}

func resolved(name, host string, expires time.Time) *federation.ResolvedServerName {
	return &federation.ResolvedServerName{
		ServerName: name, HostHeader: host,
		IPPort: []string{host + ":8448"}, Expires: expires,
	}
}

// A server that delegates is telling us where it lives and for how long. That
// answer is kept for its full period.
func TestResolveCacheKeepsDelegations(t *testing.T) {
	c, now := testResolveCache(t)
	c.StoreResolution(resolved("example.com", "matrix.example.com", now.Add(24*time.Hour)))

	got, err := c.LoadResolution("example.com")
	if err != nil || got == nil {
		t.Fatalf("delegation was not cached: %v %v", got, err)
	}
	if d := got.Expires.Sub(*now); d != 24*time.Hour {
		t.Errorf("ttl = %v, want the delegation's own 24h", d)
	}
}

// The case this cache exists for. mautrix caches a resolution reached WITHOUT a
// delegation for a full day, so a delegating server whose .well-known blips
// gets pinned to port 8448 on its bare name until tomorrow. Synapse re-checks
// such a server in two minutes.
func TestResolveCacheShortensAFallbackForADelegatingServer(t *testing.T) {
	c, now := testResolveCache(t)

	// Seen delegating once.
	c.StoreResolution(resolved("example.com", "matrix.example.com", now.Add(24*time.Hour)))
	// Then its .well-known fails and resolution falls back to the bare name,
	// which mautrix hands us with a 24-hour expiry.
	c.StoreResolution(resolved("example.com", "example.com", now.Add(24*time.Hour)))

	got, _ := c.LoadResolution("example.com")
	if got == nil {
		t.Fatal("the fallback was not cached at all")
	}
	if d := got.Expires.Sub(*now); d != resolveDownTTL {
		t.Errorf("ttl = %v, want %v so the delegation is re-checked promptly", d, resolveDownTTL)
	}
}

// A server that simply has no .well-known -- most of them -- is re-checked
// hourly rather than daily, matching WELL_KNOWN_INVALID_CACHE_PERIOD.
func TestResolveCacheShortensNonDelegatingServers(t *testing.T) {
	c, now := testResolveCache(t)
	c.StoreResolution(resolved("plain.example", "plain.example", now.Add(24*time.Hour)))

	got, _ := c.LoadResolution("plain.example")
	if got == nil {
		t.Fatal("not cached")
	}
	if d := got.Expires.Sub(*now); d != resolveNoDelegationTTL {
		t.Errorf("ttl = %v, want %v", d, resolveNoDelegationTTL)
	}
}

// A delegation's own cache headers are honoured, but clamped: a server asking
// to be cached for a year, or for one second, gets neither.
func TestResolveCacheClampsDelegationTTL(t *testing.T) {
	c, now := testResolveCache(t)

	c.StoreResolution(resolved("long.example", "m.long.example", now.Add(365*24*time.Hour)))
	got, _ := c.LoadResolution("long.example")
	if d := got.Expires.Sub(*now); d != resolveMaxTTL {
		t.Errorf("ttl = %v, want the %v cap", d, resolveMaxTTL)
	}

	c.StoreResolution(resolved("short.example", "m.short.example", now.Add(time.Second)))
	got, _ = c.LoadResolution("short.example")
	if d := got.Expires.Sub(*now); d != resolveMinTTL {
		t.Errorf("ttl = %v, want the %v floor", d, resolveMinTTL)
	}
}

// The contract says expired entries must not be returned.
func TestResolveCacheExpires(t *testing.T) {
	c, now := testResolveCache(t)
	c.StoreResolution(resolved("plain.example", "plain.example", now.Add(24*time.Hour)))

	*now = now.Add(resolveNoDelegationTTL + time.Second)
	got, err := c.LoadResolution("plain.example")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("an expired resolution was returned: %+v", got)
	}
}

// The memory of having delegated is what tells a missing .well-known from a
// broken one, and it must not last forever: a server that really has dropped
// its delegation should settle at the hourly period.
func TestResolveCacheForgetsOldDelegations(t *testing.T) {
	c, now := testResolveCache(t)
	c.StoreResolution(resolved("example.com", "matrix.example.com", now.Add(24*time.Hour)))

	*now = now.Add(resolveRememberDelegation + time.Minute)
	c.StoreResolution(resolved("example.com", "example.com", now.Add(24*time.Hour)))

	got, _ := c.LoadResolution("example.com")
	if d := got.Expires.Sub(*now); d != resolveNoDelegationTTL {
		t.Errorf("ttl = %v, want the delegation to have been forgotten (%v)",
			d, resolveNoDelegationTTL)
	}
}

// mautrix's cache never removes an expired entry, it only declines to return
// it, so a sender talking to many destinations grows a map it never frees.
func TestResolveCacheSweepsExpiredEntries(t *testing.T) {
	c, now := testResolveCache(t)
	for _, name := range []string{"a.example", "b.example", "c.example"} {
		c.StoreResolution(resolved(name, name, now.Add(24*time.Hour)))
	}
	if len(c.entries) != 3 {
		t.Fatalf("entries = %d", len(c.entries))
	}

	*now = now.Add(resolveNoDelegationTTL + resolveSweepEvery + time.Minute)
	c.StoreResolution(resolved("fresh.example", "fresh.example", now.Add(24*time.Hour)))

	if len(c.entries) != 1 {
		t.Errorf("entries = %d after a sweep, want only the fresh one", len(c.entries))
	}
}

// Jitter keeps a thousand destinations resolved in one burst from all expiring
// in the same second and re-resolving together.
func TestResolveCacheJitters(t *testing.T) {
	c, now := testResolveCache(t)
	c.jitter = func() float64 { return 1 - resolveJitter }
	c.StoreResolution(resolved("plain.example", "plain.example", now.Add(24*time.Hour)))

	got, _ := c.LoadResolution("plain.example")
	want := time.Duration(float64(resolveNoDelegationTTL) * (1 - resolveJitter))
	if d := got.Expires.Sub(*now); d != want {
		t.Errorf("ttl = %v, want %v", d, want)
	}
}
