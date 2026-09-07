package sender

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sink"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

type fixedHosts struct {
	hosts []string
	err   error
	calls int
}

func (f *fixedHosts) CurrentJoinedHosts(context.Context, string) ([]string, error) {
	f.calls++
	return f.hosts, f.err
}

// typingSink records the EDUs of every transaction, keyed by destination.
type typingSink struct {
	mu   sync.Mutex
	edus map[string][]gjson.Result
	// block, when set, makes every send hang. Nothing is ever dequeued, so the
	// queue contents stay still long enough to be asserted on -- Wake starts a
	// transmission loop whether or not the manager has been started, so an
	// "idle" manager is not a thing that exists.
	block chan struct{}
}

func (s *typingSink) Mode() string { return "typing-test" }

func (s *typingSink) Send(ctx context.Context, req *txn.Request) (sink.Result, error) {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return sink.Result{}, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range gjson.GetBytes(req.Body, "edus").Array() {
		if e.Get("edu_type").String() == txn.EDUTypeTyping {
			s.edus[req.Destination] = append(s.edus[req.Destination], e)
		}
	}
	return sink.Result{Delivered: true}, nil
}

// take returns and clears what was sent to a destination, waiting briefly for
// the transmission loop to get to it.
func (s *typingSink) take(t *testing.T, destination string, want int) []gjson.Result {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		got := s.edus[destination]
		if len(got) >= want || time.Now().After(deadline) {
			delete(s.edus, destination)
			s.mu.Unlock()
			return got
		}
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// newQueuedTyping builds a Typing whose sends all hang, so nothing is ever
// dequeued and the queue can be asserted on. Tests about what ends up QUEUED
// use this; tests about what is SENT use newTyping.
func newQueuedTyping(t *testing.T, hosts *fixedHosts) (*Typing, *queue.Manager) {
	t.Helper()
	ty, s, m := newTypingWith(t, hosts, make(chan struct{}))
	_ = s
	return ty, m
}

func newTypingWith(t *testing.T, hosts *fixedHosts, block chan struct{}) (*Typing, *typingSink, *queue.Manager) {
	t.Helper()
	signer, err := txn.NewSigner("example.com",
		"ed25519 a_Yofy AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
	if err != nil {
		t.Fatal(err)
	}
	s := &typingSink{edus: map[string][]gjson.Result{}, block: block}
	m := queue.NewManager(queue.ManagerConfig{
		Limits:        queue.Limits{MaxPDUs: 50, MaxEDUs: 100},
		Signer:        signer,
		IDs:           txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink:          s,
		Log:           zerolog.Nop(),
		MaxConcurrent: 10,
	})
	return NewTyping(TypingConfig{
		Hosts:        hosts,
		Queues:       m,
		Log:          zerolog.Nop(),
		ServerName:   "example.com",
		ShouldHandle: func(string) bool { return true },
	}), s, m
}

func newTyping(t *testing.T, hosts *fixedHosts) (*Typing, *typingSink) {
	t.Helper()
	ty, s, m := newTypingWith(t, hosts, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m.Start(ctx)
	return ty, s
}

// pendingEDUs is how many EDUs are waiting for a destination.
func pendingEDUs(m *queue.Manager, destination string) int {
	_, edus := m.Get(destination).Pending()
	return edus
}

// A row is the whole typing set for a room, so the start/stop distinction only
// exists in the diff. This is the single most important behaviour here: reading
// a row as "these users started" would never produce a stop, and every remote
// indicator would hang until the far side timed it out.
func TestTypingDiffProducesStartsAndStops(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"example.com", "remote.example"}}
	ty, s := newTyping(t, hosts)
	ctx := context.Background()

	ty.HandleRows(ctx, 1, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	edus := s.take(t, "remote.example", 1)
	if len(edus) != 1 {
		t.Fatalf("got %d EDUs, want one start", len(edus))
	}
	if !edus[0].Get("content.typing").Bool() {
		t.Errorf("first EDU is not typing:true: %s", edus[0].Raw)
	}

	// Alice absent from the new set means she stopped.
	ty.HandleRows(ctx, 2, []TypingRow{{RoomID: "!r", UserIDs: []string{}}})
	edus = s.take(t, "remote.example", 1)
	if len(edus) != 1 {
		t.Fatalf("got %d EDUs, want one stop", len(edus))
	}
	if edus[0].Get("content.typing").Bool() {
		t.Errorf("second EDU is not typing:false: %s", edus[0].Raw)
	}
}

// Only our own users are ours to announce. A remote user typing reaches us
// through the same rows and announcing them would tell their own server what it
// already knows, in our name.
func TestTypingIgnoresRemoteUsers(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, s := newTyping(t, hosts)

	ty.HandleRows(context.Background(), 1,
		[]TypingRow{{RoomID: "!r", UserIDs: []string{"@bob:remote.example"}}})
	if edus := s.take(t, "remote.example", 0); len(edus) != 0 {
		t.Errorf("announced a remote user's typing: %v", edus)
	}
	if hosts.calls != 0 {
		t.Error("resolved room hosts for a remote user's typing")
	}
}

// Our own server is in the room's host list and must never be a destination.
func TestTypingNeverSendsToOurselves(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"example.com"}}
	ty, s := newTyping(t, hosts)

	ty.HandleRows(context.Background(), 1,
		[]TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	if edus := s.take(t, "example.com", 0); len(edus) != 0 {
		t.Errorf("queued typing for our own server: %v", edus)
	}
}

// The EDU replaces a pending one for the same (room, user) rather than queueing
// behind it: delivering both to a destination that was briefly unreachable
// would switch the indicator on after the user had finished.
func TestTypingClobbersPerRoomAndUser(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, m := newQueuedTyping(t, hosts)
	ctx := context.Background()

	ty.HandleRows(ctx, 1, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	ty.HandleRows(ctx, 2, []TypingRow{{RoomID: "!r", UserIDs: []string{}}})

	// One pending EDU, not two: the stop replaced the start rather than
	// queueing behind it. WHICH one survives is the queue's business and is
	// covered by TestKeyedEDUReplacesRatherThanAccumulates; what is being
	// tested here is the key this package chooses.
	if n := pendingEDUs(m, "remote.example"); n != 1 {
		t.Errorf("%d EDUs queued, want the stop to have replaced the start", n)
	}
}

// Two users typing in one room are two separate facts and must not clobber
// each other, which is why the key is (room, user) and not the room alone.
func TestTypingDoesNotClobberAcrossUsers(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, m := newQueuedTyping(t, hosts)

	ty.HandleRows(context.Background(), 1, []TypingRow{{RoomID: "!r",
		UserIDs: []string{"@alice:example.com", "@bob:example.com"}}})

	if n := pendingEDUs(m, "remote.example"); n != 2 {
		t.Errorf("%d EDUs queued, want one per user", n)
	}
}

// A writer that restarts sends a lower token. Synapse forgets everything here,
// because a remembered "alice is typing" whose matching stop was missed would
// leave an indicator on forever.
func TestTypingResetsWhenTheStreamGoesBackwards(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, s := newTyping(t, hosts)
	ctx := context.Background()

	ty.HandleRows(ctx, 10, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	s.take(t, "remote.example", 1)

	// The same set again after a rewind is a START, not a no-op: the previous
	// state was discarded, so nothing says the far side still knows.
	ty.HandleRows(ctx, 3, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	if edus := s.take(t, "remote.example", 1); len(edus) != 1 {
		t.Errorf("got %d EDUs after a rewind, want the typing re-announced", len(edus))
	}
}

// Without the keep-alive a remote server expires the indicator after a minute
// and a user composing a long message appears to have stopped.
func TestTypingKeepAliveReannounces(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, s := newTyping(t, hosts)
	ctx := context.Background()

	ty.HandleRows(ctx, 1, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	s.take(t, "remote.example", 1)

	// Not yet due: a keep-alive on every tick would be a transaction per tick
	// per typing user.
	ty.KeepAlive(ctx)
	if edus := s.take(t, "remote.example", 0); len(edus) != 0 {
		t.Errorf("re-announced before the ping interval: %v", edus)
	}

	ty.mu.Lock()
	ty.lastPoke[member{"!r", "@alice:example.com"}] = time.Now().Add(-FederationPingInterval - time.Second)
	ty.mu.Unlock()

	ty.KeepAlive(ctx)
	edus := s.take(t, "remote.example", 1)
	if len(edus) != 1 {
		t.Fatalf("got %d EDUs, want the typing re-announced", len(edus))
	}
	if !edus[0].Get("content.typing").Bool() {
		t.Error("the keep-alive announced a stop")
	}
}

// A user who has stopped is no longer tracked, so the keep-alive must not
// resurrect them.
func TestTypingKeepAliveForgetsStoppedUsers(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, s := newTyping(t, hosts)
	ctx := context.Background()

	ty.HandleRows(ctx, 1, []TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})
	ty.HandleRows(ctx, 2, []TypingRow{{RoomID: "!r", UserIDs: []string{}}})
	s.take(t, "remote.example", 1)

	ty.KeepAlive(ctx)
	if edus := s.take(t, "remote.example", 0); len(edus) != 0 {
		t.Errorf("kept a stopped user alive: %v", edus)
	}
}

// The content is what a remote server reads; a missing field is an EDU that
// parses and means nothing.
func TestTypingEDUContent(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example"}}
	ty, s := newTyping(t, hosts)

	ty.HandleRows(context.Background(), 1,
		[]TypingRow{{RoomID: "!room:example.com", UserIDs: []string{"@alice:example.com"}}})

	edus := s.take(t, "remote.example", 1)
	if len(edus) != 1 {
		t.Fatal("no EDU")
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(edus[0].Get("content").Raw), &body); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"room_id", "user_id", "typing"} {
		if _, ok := body[k]; !ok {
			t.Errorf("content is missing %q: %s", k, edus[0].Raw)
		}
	}
}

// A destination that is backing off is dropped before the EDU is built, as
// Synapse does (typing.py:180): a typing notification is worthless by the time
// a dead server returns.
func TestTypingSkipsBackingOffDestinations(t *testing.T) {
	hosts := &fixedHosts{hosts: []string{"remote.example", "up.example"}}
	ty, s := newTyping(t, hosts)
	ty.due = func(destination string) bool { return destination == "up.example" }

	ty.HandleRows(context.Background(), 1,
		[]TypingRow{{RoomID: "!r", UserIDs: []string{"@alice:example.com"}}})

	if edus := s.take(t, "up.example", 1); len(edus) != 1 {
		t.Errorf("got %d EDUs for the reachable destination, want 1", len(edus))
	}
	if edus := s.take(t, "remote.example", 0); len(edus) != 0 {
		t.Errorf("queued typing for a backing-off destination: %v", edus)
	}
}
