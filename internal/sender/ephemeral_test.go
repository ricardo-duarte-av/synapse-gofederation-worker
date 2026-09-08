package sender

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// ephemeralQueues builds a manager whose sends hang, so what was queued stays
// queued and can be asserted on. Wake starts a transmission loop whether or not
// the manager has been started, so there is no such thing as an idle one.
func ephemeralQueues(t *testing.T) *queue.Manager {
	t.Helper()
	signer, err := txn.NewSigner("example.com",
		"ed25519 a_Yofy AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
	if err != nil {
		t.Fatal(err)
	}
	return queue.NewManager(queue.ManagerConfig{
		Signer: signer,
		IDs:    txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink:   &typingSink{edus: map[string][]gjson.Result{}, block: make(chan struct{})},
		Log:    zerolog.Nop(), MaxConcurrent: 4,
	})
}

// fakeEphemeralStore records what the ephemeral paths ask the database for, so
// a test can assert that work was skipped rather than merely discarded.
type fakeEphemeralStore struct {
	hosts         []string
	presence      map[string]store.PresenceState
	presenceReads int
}

func (f *fakeEphemeralStore) CurrentJoinedHosts(context.Context, string) ([]string, error) {
	return f.hosts, nil
}

func (f *fakeEphemeralStore) GetPresenceStates(
	_ context.Context, userIDs []string,
) (map[string]store.PresenceState, error) {
	f.presenceReads++
	out := map[string]store.PresenceState{}
	for _, u := range userIDs {
		if s, ok := f.presence[u]; ok {
			out[u] = s
		}
	}
	return out, nil
}

// A destination that is down must be filtered BEFORE the presence state is read
// and the EDU built, not after it is queued.
//
// Measured on the main deployment before this: 8.98 presence EDUs per second
// dropped against 8.67 sent -- about half of all presence was read from the
// database, encoded, queued and woken, purely to be thrown away by the
// long-outage rule. Synapse filters at the same point and for the same reason
// (federation/sender/__init__.py:986).
func TestPresenceSkipsBackingOffDestinationsBeforeAnyWork(t *testing.T) {
	st := &fakeEphemeralStore{
		hosts: []string{"up.example", "down.example"},
		presence: map[string]store.PresenceState{
			"@alice:example.com": {UserID: "@alice:example.com", State: "online"},
		},
	}
	m := ephemeralQueues(t)
	e := NewEphemeral(EphemeralConfig{
		Store: st, Queues: m, Log: zerolog.Nop(), ServerName: "example.com",
		ShouldHandle: func(string) bool { return true },
		DueWithin:    func(d string) bool { return d == "up.example" },
	})

	ctx := context.Background()
	if err := e.HandlePresence(ctx, "down.example", []string{"@alice:example.com"}); err != nil {
		t.Fatal(err)
	}
	if st.presenceReads != 0 {
		t.Errorf("read presence from the database %d times for a destination that is down",
			st.presenceReads)
	}
	if _, edus := m.Get("down.example").Pending(); edus != 0 {
		t.Errorf("queued %d EDUs for a destination that is down", edus)
	}

	// The reachable one is unaffected.
	if err := e.HandlePresence(ctx, "up.example", []string{"@alice:example.com"}); err != nil {
		t.Fatal(err)
	}
	if _, edus := m.Get("up.example").Pending(); edus != 1 {
		t.Errorf("queued %d EDUs for a reachable destination, want 1", edus)
	}
}

// Same rule for receipts (federation/sender/__init__.py:915): a read receipt
// for a server that has been down for a week is worth nothing when it returns.
func TestReceiptSkipsBackingOffDestinations(t *testing.T) {
	st := &fakeEphemeralStore{hosts: []string{"up.example", "down.example", "example.com"}}
	m := ephemeralQueues(t)
	e := NewEphemeral(EphemeralConfig{
		Store: st, Queues: m, Log: zerolog.Nop(), ServerName: "example.com",
		ShouldHandle: func(string) bool { return true },
		DueWithin:    func(d string) bool { return d == "up.example" },
	})

	err := e.HandleReceipt(context.Background(), ReceiptUpdate{
		RoomID: "!r:example.com", ReceiptType: "m.read", UserID: "@alice:example.com",
		EventID: "$e", Data: []byte(`{"ts":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, edus := m.Get("down.example").Pending(); edus != 0 {
		t.Errorf("queued %d receipt EDUs for a destination that is down", edus)
	}
	if _, edus := m.Get("up.example").Pending(); edus != 1 {
		t.Errorf("queued %d receipt EDUs for a reachable destination, want 1", edus)
	}
}

// With no backoff configured every destination is due, which is what a shadow
// and every test wants: a nil hook must not silently filter everything out.
func TestEphemeralWithNoBackoffSendsToEveryone(t *testing.T) {
	st := &fakeEphemeralStore{
		hosts: []string{"a.example"},
		presence: map[string]store.PresenceState{
			"@alice:example.com": {UserID: "@alice:example.com", State: "online"},
		},
	}
	m := ephemeralQueues(t)
	e := NewEphemeral(EphemeralConfig{
		Store: st, Queues: m, Log: zerolog.Nop(), ServerName: "example.com",
		ShouldHandle: func(string) bool { return true },
	})
	if err := e.HandlePresence(context.Background(), "a.example",
		[]string{"@alice:example.com"}); err != nil {
		t.Fatal(err)
	}
	if _, edus := m.Get("a.example").Pending(); edus != 1 {
		t.Errorf("queued %d EDUs with no backoff hook, want 1", edus)
	}
}
