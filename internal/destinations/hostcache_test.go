package destinations

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type countingHosts struct {
	mu    sync.Mutex
	calls map[int64]int
	err   error
}

func (c *countingHosts) CurrentJoinedHosts(context.Context, string) ([]string, error) {
	return []string{"current.example"}, nil
}
func (c *countingHosts) GetStateGroupsForEvents(context.Context, []string) (map[string]int64, error) {
	return nil, nil
}
func (c *countingHosts) JoinedHostsAtStateGroup(_ context.Context, g int64) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[int64]int{}
	}
	c.calls[g]++
	if c.err != nil {
		return nil, c.err
	}
	return []string{fmt.Sprintf("host%d.example", g)}, nil
}
func (c *countingHosts) count(g int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[g]
}

// The point of the cache: events between two state changes share a state group,
// 14.5 of them on this deployment, so the expensive recursive query runs once
// per state change rather than once per message.
func TestHostCacheServesRepeats(t *testing.T) {
	inner := &countingHosts{}
	c := NewCachedRoomHosts(inner, 8)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		hosts, err := c.JoinedHostsAtStateGroup(ctx, 42)
		if err != nil {
			t.Fatal(err)
		}
		if len(hosts) != 1 || hosts[0] != "host42.example" {
			t.Fatalf("hosts = %v", hosts)
		}
	}
	if got := inner.count(42); got != 1 {
		t.Errorf("queried the database %d times, want 1", got)
	}
}

// A cache that hands out its own storage is one aliasing bug away from
// rewriting history for every later event on the same state group.
func TestHostCacheDoesNotShareItsStorage(t *testing.T) {
	c := NewCachedRoomHosts(&countingHosts{}, 8)
	ctx := context.Background()

	first, err := c.JoinedHostsAtStateGroup(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	first[0] = "tampered.example"

	second, err := c.JoinedHostsAtStateGroup(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second[0] != "host1.example" {
		t.Errorf("cached entry was mutated through a returned slice: %v", second)
	}
}

func TestHostCacheEvictsOldest(t *testing.T) {
	inner := &countingHosts{}
	c := NewCachedRoomHosts(inner, 2)
	ctx := context.Background()

	for _, g := range []int64{1, 2, 3} {
		if _, err := c.JoinedHostsAtStateGroup(ctx, g); err != nil {
			t.Fatal(err)
		}
	}
	if c.Len() != 2 {
		t.Errorf("Len = %d, want the bound of 2", c.Len())
	}
	// 1 was evicted, so asking again re-queries; 3 is still cached.
	if _, err := c.JoinedHostsAtStateGroup(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if got := inner.count(1); got != 2 {
		t.Errorf("group 1 queried %d times, want it to have been evicted", got)
	}
	if _, err := c.JoinedHostsAtStateGroup(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if got := inner.count(3); got != 1 {
		t.Errorf("group 3 queried %d times, want it still cached", got)
	}
}

// A failed lookup must not be cached as an empty host set, or one database
// blip would silently route nothing for that state group until it is evicted.
func TestHostCacheDoesNotCacheErrors(t *testing.T) {
	inner := &countingHosts{err: errors.New("database is down")}
	c := NewCachedRoomHosts(inner, 8)
	ctx := context.Background()

	if _, err := c.JoinedHostsAtStateGroup(ctx, 7); err == nil {
		t.Fatal("expected the error through")
	}
	inner.err = nil
	hosts, err := c.JoinedHostsAtStateGroup(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Errorf("hosts = %v, want the retry to have reached the database", hosts)
	}
}

// "Who is in the room now" changes with every membership event and has no key
// to hang an invalidation on, so it must go straight through.
func TestHostCachePassesCurrentHostsThrough(t *testing.T) {
	c := NewCachedRoomHosts(&countingHosts{}, 8)
	hosts, err := c.CurrentJoinedHosts(context.Background(), "!r:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0] != "current.example" {
		t.Errorf("hosts = %v", hosts)
	}
}

func TestHostCacheReportsHitsAndMisses(t *testing.T) {
	c := NewCachedRoomHosts(&countingHosts{}, 8)
	var hits, misses int
	c.SetOnLookup(func(hit bool) {
		if hit {
			hits++
		} else {
			misses++
		}
	})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.JoinedHostsAtStateGroup(ctx, 5); err != nil {
			t.Fatal(err)
		}
	}
	if misses != 1 || hits != 2 {
		t.Errorf("hits = %d, misses = %d, want 2 and 1", hits, misses)
	}
}
