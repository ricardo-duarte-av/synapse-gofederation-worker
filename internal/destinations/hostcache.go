package destinations

import (
	"container/list"
	"context"
	"sync"
)

// DefaultHostCacheEntries is how many state groups to remember.
//
// Sized against what the entries cost rather than against the number of rooms:
// a 1,000-server room caches 1,000 hostnames, so a few thousand entries is
// megabytes, not tens. Synapse's equivalent cache holds 10,000
// (roommember.py:1347).
const DefaultHostCacheEntries = 4096

// CachedRoomHosts caches JoinedHostsAtStateGroup and passes everything else
// through.
//
// The cache needs no invalidation, which is the whole reason it is safe. A
// state group is immutable by construction: changing the state creates a NEW
// group rather than editing one, so the set of servers with a joined member at
// a given group is fixed for as long as that group exists. Nothing can make a
// cached answer wrong; the worst that happens is remembering a group nobody
// asks about again.
//
// The hit rate comes from the same fact. Events between two state changes all
// share one state group -- 14.5 of them on this deployment, measured over the
// last 5,000 events -- so the query runs once per state change rather than once
// per message. That query is the most expensive thing in the per-event path: a
// recursive walk of the state group edges, measured at 10.8ms, 108ms and 375ms
// for three random recent groups and 6.3 SECONDS for the deepest, against a
// mean event lag of 43ms.
//
// CurrentJoinedHosts is deliberately NOT cached. It answers "who is in the room
// now", which changes with every membership event and has no key to hang an
// invalidation on.
type CachedRoomHosts struct {
	RoomHosts

	mu      sync.Mutex
	entries map[int64]*list.Element
	lru     *list.List
	max     int

	onLookup func(hit bool)
}

type hostCacheEntry struct {
	group int64
	hosts []string
}

// NewCachedRoomHosts wraps inner with a bounded cache of state group lookups.
func NewCachedRoomHosts(inner RoomHosts, max int) *CachedRoomHosts {
	if max <= 0 {
		max = DefaultHostCacheEntries
	}
	return &CachedRoomHosts{
		RoomHosts: inner,
		entries:   make(map[int64]*list.Element, max),
		lru:       list.New(),
		max:       max,
	}
}

// SetOnLookup registers a callback for each lookup, so the hit rate is
// observable. A cache nobody can see the hit rate of is a cache nobody can tell
// is broken.
func (c *CachedRoomHosts) SetOnLookup(f func(hit bool)) { c.onLookup = f }

// JoinedHostsAtStateGroup returns the cached host set for a state group,
// reading through on a miss.
func (c *CachedRoomHosts) JoinedHostsAtStateGroup(ctx context.Context, group int64) ([]string, error) {
	if hosts, ok := c.get(group); ok {
		if c.onLookup != nil {
			c.onLookup(true)
		}
		return hosts, nil
	}
	if c.onLookup != nil {
		c.onLookup(false)
	}

	// Deliberately not single-flighted. Two events arriving together for one
	// state group do the query twice, which costs one extra read; holding a
	// lock across the database call instead would stall every other room's
	// resolution behind the slowest query in the process.
	hosts, err := c.RoomHosts.JoinedHostsAtStateGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	c.put(group, hosts)
	return append([]string(nil), hosts...), nil
}

func (c *CachedRoomHosts) get(group int64) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[group]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(el)
	// Copied out. The caller builds a set from this and has no reason to write
	// to it, but a cache that hands out its own storage is one aliasing bug
	// away from rewriting history for every later event on the same group.
	return append([]string(nil), el.Value.(*hostCacheEntry).hosts...), true
}

func (c *CachedRoomHosts) put(group int64, hosts []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[group]; ok {
		el.Value.(*hostCacheEntry).hosts = hosts
		c.lru.MoveToFront(el)
		return
	}
	c.entries[group] = c.lru.PushFront(&hostCacheEntry{group: group, hosts: hosts})
	for c.lru.Len() > c.max {
		oldest := c.lru.Back()
		c.lru.Remove(oldest)
		delete(c.entries, oldest.Value.(*hostCacheEntry).group)
	}
}

// Len is how many state groups are cached, for metrics and tests.
func (c *CachedRoomHosts) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}
