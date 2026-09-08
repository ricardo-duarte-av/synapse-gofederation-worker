package sender

import (
	"context"
	"encoding/json"

	"github.com/rs/zerolog"

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
	GetDeviceDetails(ctx context.Context, userIDs, deviceIDs []string) (map[string]store.DeviceDetail, error)
	GetLastDeviceUpdateForRemoteUser(ctx context.Context, destination, userID string, fromStreamID int64) (int64, error)
	GetCrossSigningKeys(ctx context.Context, userIDs []string) ([]store.CrossSigningKey, error)
	MaxDeviceOutboxStreamID(ctx context.Context) (int64, error)
	MaxDeviceListOutboundStreamID(ctx context.Context) (int64, error)
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
	log     zerolog.Logger

	shouldHandle func(destination string) bool
	// dueWithin reports whether a destination is due now or within the hour.
	// Synapse applies it in the same expression as the shard filter
	// (federation/sender/__init__.py:1063), so ourShare applies both.
	dueWithin func(destination string) bool
	// allowDeviceNameLookup mirrors Synapse's
	// allow_device_name_lookup_over_federation.
	allowDeviceNameLookup bool
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
	Log          zerolog.Logger
	ShouldHandle func(destination string) bool
	// DueWithin reports whether a destination is due now or within the hour.
	// Nil treats every destination as due.
	DueWithin   func(destination string) bool
	EDUsPerRead int
	// AllowDeviceNameLookup comes from Synapse's config, not ours.
	AllowDeviceNameLookup bool
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
		store: cfg.Store, cursors: cfg.Cursors, queues: cfg.Queues, log: cfg.Log,
		shouldHandle: cfg.ShouldHandle,
		dueWithin:    cfg.DueWithin, edusPerRead: n,
		allowDeviceNameLookup: cfg.AllowDeviceNameLookup,
	}
}

// HandleToDevice queues the to-device messages owed to the given destinations.
//
// The destinations come from the to_device replication stream, whose rows carry
// an entity that is either a user id or a remote server name; only the latter
// concern a sender (replication/tcp/client.py:472).
func (d *Devices) HandleToDevice(ctx context.Context, servers []string) error {
	// Logged before the shard filter as well as after. This path is a race
	// against Synapse's own sender, which DELETES the rows it sends, so "we
	// saw nothing" and "we were too late" are different facts and only a log
	// taken at both points can tell them apart.
	all := servers
	servers = d.ourShare(servers)
	d.log.Debug().Strs("servers", all).Strs("ours", servers).
		Msg("to-device poke")
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
	d.log.Debug().
		Str("destination", server).Int64("from", last).Int64("current", current).
		Int("messages", len(msgs)).
		Msg("read to-device outbox")
	if len(msgs) == 0 {
		// Still advance: the gap is other destinations' rows, and leaving the
		// cursor behind them would re-scan the same range forever.
		return d.cursors.Set(ctx, name, next)
	}

	q := d.queues.Get(server)
	for i, m := range msgs {
		// Synapse emits one EDU per row rather than merging them
		// (per_destination_queue.py:700).
		e := queue.EDU{Unit: txn.EDU{
			Type:    txn.EDUTypeDirectToDevice,
			Content: json.RawMessage(m.MessagesJSON),
		}}
		// Only the LAST unit carries the mark. The deletion is
		// "everything up to this stream id", so marking each one would
		// delete rows whose EDU had not yet been delivered if the
		// transaction took only a prefix of the batch.
		if i == len(msgs)-1 {
			e.ToDeviceUpTo = next
		}
		q.EnqueueMarkedEDU(e)
	}
	d.queues.Wake(q)

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
	all := servers
	servers = d.ourShare(servers)
	d.log.Debug().Int64("stream_id", streamID).
		Strs("servers", all).Strs("ours", servers).
		Msg("device list poke")
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

	edus, highest, err := d.buildDeviceListEDUs(ctx, server, last, pokes)
	if err != nil {
		return err
	}

	q := d.queues.Get(server)
	for i, e := range edus {
		m := queue.EDU{Unit: e}
		if i == len(edus)-1 {
			m.DeviceListUpTo = highest
		}
		q.EnqueueMarkedEDU(m)
	}
	d.queues.Wake(q)

	return d.cursors.Set(ctx, name, highest)
}

// ourShare keeps the destinations belonging to this worker's shard, dropping
// any that are backing off.
// ourShare keeps the destinations this worker should act on: in our shard, and
// worth acting on at all.
//
// Both filters together, because Synapse applies them together in
// send_device_messages (federation/sender/__init__.py:1063): the shard check
// inline, the retry check around it with an hour of slack. Doing the retry half
// HERE rather than after the database read is the point -- a device poke for a
// server that has been down for a week costs a query over
// device_federation_outbox and an EDU built to be dropped.
func (d *Devices) ourShare(servers []string) []string {
	out := make([]string, 0, len(servers))
	for _, s := range servers {
		if d.shouldHandle != nil && !d.shouldHandle(s) {
			continue
		}
		if d.dueWithin != nil && !d.dueWithin(s) {
			continue
		}
		out = append(out, s)
	}
	return out
}

// The retry filter used to live here as a FilterDue method that nothing ever
// called. It read Synapse's `destinations` table, which this worker stopped
// writing when the backoff moved to its own schema (see
// internal/state.SetRetryTimings), so by the end it would also have been
// reading the wrong source. The filter is in ourShare now, where both call
// sites already go.
