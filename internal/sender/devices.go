package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/queue"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// DeviceStore is the database access the device EDU path needs.
type DeviceStore interface {
	GetNewDeviceMsgsForRemote(ctx context.Context, destination string, last, current int64, limit int) ([]store.ToDeviceMessage, int64, error)
	GetDeviceUpdatesByRemote(ctx context.Context, destination string, from, now int64, limit int) ([]store.DevicePoke, error)
	GetDestinationsForDevice(ctx context.Context, streamID int64) ([]string, error)
	MaxDeviceOutboxStreamID(ctx context.Context) (int64, error)
	MaxDeviceListOutboundStreamID(ctx context.Context) (int64, error)
	GetDestinationRetryTimings(ctx context.Context, destinations []string) (map[string]store.RetryTimings, error)
}

// Devices moves to-device messages and device list updates into destination
// queues.
//
// This path is where being read-only bites hardest. A real sender DELETES from
// device_federation_outbox and device_lists_outbound_pokes once a transaction
// succeeds -- that deletion IS its cursor. We must not delete (it would stop
// real device updates reaching real servers and break E2EE for real users, with
// nothing in any log to say so), so each destination gets a high-water mark in
// our own table instead. See docs/shadow-safety.md.
//
// The consequence: our marks and Synapse's deletions drift apart the moment
// either sender does anything the other does not, which is expected and is
// itself worth watching.
type Devices struct {
	store   DeviceStore
	cursors Cursors
	queues  *queue.Manager

	shouldHandle func(destination string) bool
	// edusPerTransaction is the per-destination read limit. Synapse budgets
	// MAX_EDUS_PER_TRANSACTION minus ten reserved for to-device messages
	// (per_destination_queue.py:794); the same budget is applied here so a
	// destination is not handed more than one transaction's worth at a time.
	edusPerRead int
}

// DevicesConfig builds a Devices.
type DevicesConfig struct {
	Store        DeviceStore
	Cursors      Cursors
	Queues       *queue.Manager
	ShouldHandle func(destination string) bool
	EDUsPerRead  int
}

// NewDevices builds the device EDU path.
func NewDevices(cfg DevicesConfig) *Devices {
	n := cfg.EDUsPerRead
	if n <= 0 {
		// MAX_EDUS_PER_TRANSACTION minus NUMBER_OF_RESERVED_EDUS_PER_TRANSACTION
		// (api/constants.py:56,59).
		n = 90
	}
	return &Devices{
		store: cfg.Store, cursors: cfg.Cursors, queues: cfg.Queues,
		shouldHandle: cfg.ShouldHandle, edusPerRead: n,
	}
}

// HandleToDevice queues the to-device messages owed to the given destinations.
//
// The destinations come from the to_device replication stream, whose rows carry
// an entity that is either a user id or a remote server name; only the latter
// concern a sender (replication/tcp/client.py:472).
func (d *Devices) HandleToDevice(ctx context.Context, servers []string) error {
	servers = d.ourShare(servers)
	if len(servers) == 0 {
		return nil
	}
	current, err := d.store.MaxDeviceOutboxStreamID(ctx)
	if err != nil {
		return err
	}

	for _, server := range servers {
		if err := d.toDeviceFor(ctx, server, current); err != nil {
			return err
		}
	}
	return nil
}

func (d *Devices) toDeviceFor(ctx context.Context, server string, current int64) error {
	name := state.ToDeviceCursor(server)
	last, _, err := d.cursors.Get(ctx, name)
	if err != nil {
		return err
	}

	msgs, next, err := d.store.GetNewDeviceMsgsForRemote(ctx, server, last, current, d.edusPerRead)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		// Still advance: the gap is other destinations' rows, and leaving the
		// cursor behind them would re-scan the same range forever.
		return d.cursors.Set(ctx, name, next)
	}

	q := d.queues.Get(server)
	for _, m := range msgs {
		// Synapse emits one EDU per row rather than merging them
		// (per_destination_queue.py:700).
		q.EnqueueEDU(txn.EDU{
			Type:    txn.EDUTypeDirectToDevice,
			Content: json.RawMessage(m.MessagesJSON),
		})
	}
	d.queues.Wake(ctx, q)

	// The cursor advances on the READ, not on delivery. That is a real
	// divergence from Synapse, which deletes the rows only after a successful
	// transaction -- but the alternative is re-reading these rows forever,
	// since we can never delete them. In dry-run nothing is lost by it; before
	// this worker sends for real, the cursor must move to delivery instead.
	return d.cursors.Set(ctx, name, next)
}

// HandleDeviceLists queues device list updates for a stream id.
//
// The replication row carries only the user id and a hosts_calculated flag; the
// destinations live in device_lists_outbound_pokes for that stream id
// (devices.py:1933), which is the only place they exist.
func (d *Devices) HandleDeviceLists(ctx context.Context, streamID int64) error {
	servers, err := d.store.GetDestinationsForDevice(ctx, streamID)
	if err != nil {
		return err
	}
	servers = d.ourShare(servers)
	if len(servers) == 0 {
		return nil
	}
	current, err := d.store.MaxDeviceListOutboundStreamID(ctx)
	if err != nil {
		return err
	}

	for _, server := range servers {
		if err := d.deviceListsFor(ctx, server, current); err != nil {
			return err
		}
	}
	return nil
}

func (d *Devices) deviceListsFor(ctx context.Context, server string, current int64) error {
	name := state.DeviceListCursor(server)
	last, _, err := d.cursors.Get(ctx, name)
	if err != nil {
		return err
	}

	pokes, err := d.store.GetDeviceUpdatesByRemote(ctx, server, last, current, d.edusPerRead)
	if err != nil {
		return err
	}
	if len(pokes) == 0 {
		return d.cursors.Set(ctx, name, current)
	}

	q := d.queues.Get(server)
	highest := last
	for _, p := range pokes {
		content, err := json.Marshal(map[string]any{
			"user_id":   p.UserID,
			"device_id": p.DeviceID,
			"stream_id": p.StreamID,
		})
		if err != nil {
			return fmt.Errorf("sender: encoding device list update: %w", err)
		}
		// A faithful m.device_list_update also carries prev_id, deleted, keys
		// and device_display_name, assembled from the device tables and
		// device_lists_outbound_last_success. Phase 1 queues the shape without
		// them: the shadow's question is WHICH destinations get an update and
		// WHEN, and that is answered by this. Filling in the body is what turns
		// this from a shadow into a sender, and is deliberately not done while
		// the answer is never put on a wire.
		q.EnqueueEDU(txn.EDU{Type: txn.EDUTypeDeviceListUpdate, Content: content})
		if p.StreamID > highest {
			highest = p.StreamID
		}
	}
	d.queues.Wake(ctx, q)

	return d.cursors.Set(ctx, name, highest)
}

// ourShare keeps the destinations belonging to this worker's shard, dropping
// any that are backing off.
func (d *Devices) ourShare(servers []string) []string {
	out := make([]string, 0, len(servers))
	for _, s := range servers {
		if d.shouldHandle == nil || d.shouldHandle(s) {
			out = append(out, s)
		}
	}
	return out
}

// FilterDue drops destinations in a long backoff, matching send_device_messages
// (federation/sender/__init__.py:1061).
func (d *Devices) FilterDue(ctx context.Context, servers []string) ([]string, error) {
	if len(servers) == 0 {
		return nil, nil
	}
	timings, err := d.store.GetDestinationRetryTimings(ctx, servers)
	if err != nil {
		return nil, err
	}
	return store.FilterDestinationsByRetryLimiter(servers, timings, time.Now(), catchupRetryInterval), nil
}
