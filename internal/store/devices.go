package store

import (
	"context"
	"fmt"
)

// ToDeviceMessage is one row of device_federation_outbox.
type ToDeviceMessage struct {
	StreamID int64
	// MessagesJSON becomes the content of one m.direct_to_device EDU. Synapse
	// emits one EDU per row rather than merging them (per_destination_queue.py:700).
	MessagesJSON []byte
}

// GetNewDeviceMsgsForRemote is Synapse's get_new_device_msgs_for_remote
// (storage/databases/main/deviceinbox.py:627).
//
// It returns the messages and the position to resume from. When fewer than
// `limit` rows came back, Synapse fast-forwards the cursor to currentStreamID
// rather than to the last row -- the gap is rows for other destinations, and
// leaving the cursor behind them would re-scan the same range forever.
//
// The real sender DELETES these rows once the transaction succeeds. We never
// do; our cursor lives in internal/state instead. See docs/shadow-safety.md:
// consuming them would silently stop to-device messages reaching real servers.
func (s *Store) GetNewDeviceMsgsForRemote(
	ctx context.Context, destination string, lastStreamID, currentStreamID int64, limit int,
) ([]ToDeviceMessage, int64, error) {
	if lastStreamID >= currentStreamID {
		return nil, currentStreamID, nil
	}
	const q = `
		SELECT stream_id, messages_json FROM device_federation_outbox
		WHERE destination = $1 AND $2 < stream_id AND stream_id <= $3
		ORDER BY stream_id ASC
		LIMIT $4`

	rows, err := s.pool.Query(ctx, q, destination, lastStreamID, currentStreamID, limit)
	if err != nil {
		return nil, lastStreamID, fmt.Errorf("store: to-device outbox: %w", err)
	}
	defer rows.Close()

	var out []ToDeviceMessage
	for rows.Next() {
		var m ToDeviceMessage
		if err := rows.Scan(&m.StreamID, &m.MessagesJSON); err != nil {
			return nil, lastStreamID, fmt.Errorf("store: to-device outbox: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, lastStreamID, fmt.Errorf("store: to-device outbox: %w", err)
	}

	if len(out) < limit {
		return out, currentStreamID, nil
	}
	return out, out[len(out)-1].StreamID, nil
}

// DevicePoke is one row of device_lists_outbound_pokes.
type DevicePoke struct {
	UserID             string
	DeviceID           string
	StreamID           int64
	OpentracingContext string
}

// GetDeviceUpdatesByRemote is the query behind Synapse's
// get_device_updates_by_remote (storage/databases/main/devices.py:797).
//
// Synapse asks for limit+1 rows so it can tell a full batch from a partial one;
// callers here do the same. As with to-device messages, the real sender deletes
// these rows on success and we never do.
func (s *Store) GetDeviceUpdatesByRemote(
	ctx context.Context, destination string, fromStreamID, nowStreamID int64, limit int,
) ([]DevicePoke, error) {
	const q = `
		SELECT user_id, device_id, stream_id, COALESCE(opentracing_context, '')
		FROM device_lists_outbound_pokes
		WHERE destination = $1 AND $2 < stream_id AND stream_id <= $3
		ORDER BY stream_id
		LIMIT $4`

	rows, err := s.pool.Query(ctx, q, destination, fromStreamID, nowStreamID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: device pokes: %w", err)
	}
	defer rows.Close()

	var out []DevicePoke
	for rows.Next() {
		var p DevicePoke
		if err := rows.Scan(&p.UserID, &p.DeviceID, &p.StreamID, &p.OpentracingContext); err != nil {
			return nil, fmt.Errorf("store: device pokes: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetDestinationsForDevice is Synapse's get_destinations_for_device
// (devices.py:1933): which servers a device_lists replication row concerns.
//
// The row itself carries only the user id and a "hosts were calculated" flag,
// so this is the only way to learn who the poke is actually for.
func (s *Store) GetDestinationsForDevice(ctx context.Context, streamID int64) ([]string, error) {
	const q = `SELECT destination FROM device_lists_outbound_pokes WHERE stream_id = $1`
	rows, err := s.pool.Query(ctx, q, streamID)
	if err != nil {
		return nil, fmt.Errorf("store: destinations for device: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("store: destinations for device: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MaxDeviceOutboxStreamID and MaxDeviceListOutboundStreamID bound the device
// queries above.
//
// A real sender learns these from the replication stream's POSITION. We do not
// publish REPLICATE, so we read them (docs/shadow-safety.md).
func (s *Store) MaxDeviceOutboxStreamID(ctx context.Context) (int64, error) {
	return s.maxStreamID(ctx, `SELECT COALESCE(MAX(stream_id), 0) FROM device_federation_outbox`)
}

// MaxDeviceListOutboundStreamID is the newest outbound device list poke.
func (s *Store) MaxDeviceListOutboundStreamID(ctx context.Context) (int64, error) {
	return s.maxStreamID(ctx, `SELECT COALESCE(MAX(stream_id), 0) FROM device_lists_outbound_pokes`)
}

func (s *Store) maxStreamID(ctx context.Context, q string) (int64, error) {
	var v int64
	if err := s.pool.QueryRow(ctx, q).Scan(&v); err != nil {
		return 0, fmt.Errorf("store: max stream id: %w", err)
	}
	return v, nil
}
