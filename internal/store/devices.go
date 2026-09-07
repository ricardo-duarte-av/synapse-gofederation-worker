package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
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

// DeviceDetail is what a device list update needs about one device.
type DeviceDetail struct {
	UserID   string
	DeviceID string
	// Exists is false when the device row is gone, which Synapse reports as
	// "deleted": true rather than by omitting the update
	// (devices.py:877). A receiver that never learns a device was deleted
	// keeps encrypting to it.
	Exists bool
	// DisplayName is only sent when allow_device_name_lookup_over_federation
	// is on; the caller applies that, since it is a config question.
	DisplayName string
	// KeysJSON is the device's e2e keys, verbatim from e2e_device_keys_json.
	// Nil when the device has never uploaded any.
	KeysJSON []byte
}

// GetDeviceDetails loads the device rows and e2e keys behind a set of pokes.
//
// Deleted devices are included with Exists false, which is why this is a LEFT
// JOIN from the requested pairs rather than a lookup in devices: the whole
// point of a device list update is often that the device is gone.
func (s *Store) GetDeviceDetails(ctx context.Context, userIDs, deviceIDs []string) (map[string]DeviceDetail, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT p.user_id, p.device_id,
		       d.user_id IS NOT NULL AS exists_now,
		       COALESCE(d.display_name, ''),
		       k.key_json
		FROM unnest($1::text[], $2::text[]) AS p(user_id, device_id)
		LEFT JOIN devices d USING (user_id, device_id)
		LEFT JOIN e2e_device_keys_json k USING (user_id, device_id)`

	rows, err := s.pool.Query(ctx, q, userIDs, deviceIDs)
	if err != nil {
		return nil, fmt.Errorf("store: device details: %w", err)
	}
	defer rows.Close()

	out := make(map[string]DeviceDetail, len(userIDs))
	for rows.Next() {
		var d DeviceDetail
		if err := rows.Scan(&d.UserID, &d.DeviceID, &d.Exists, &d.DisplayName, &d.KeysJSON); err != nil {
			return nil, fmt.Errorf("store: device details: %w", err)
		}
		out[d.UserID+"\x00"+d.DeviceID] = d
	}
	return out, rows.Err()
}

// DeviceDetailKey is how GetDeviceDetails keys its result.
func DeviceDetailKey(userID, deviceID string) string { return userID + "\x00" + deviceID }

// GetLastDeviceUpdateForRemoteUser is Synapse's
// _get_last_device_update_for_remote_user (devices.py:892): the stream id of the
// newest update already delivered to this destination for this user, at or
// below fromStreamID.
//
// It is the `prev_id` of the first update in a batch, and it is what lets the
// receiving server notice a gap and resync rather than silently missing a
// device. Zero means there is no predecessor, which Synapse sends as an empty
// prev_id list.
func (s *Store) GetLastDeviceUpdateForRemoteUser(ctx context.Context, destination, userID string, fromStreamID int64) (int64, error) {
	const q = `
		SELECT COALESCE(MAX(stream_id), 0) FROM device_lists_outbound_last_success
		WHERE destination = $1 AND user_id = $2 AND stream_id <= $3`
	var id int64
	if err := s.pool.QueryRow(ctx, q, destination, userID, fromStreamID).Scan(&id); err != nil {
		return 0, fmt.Errorf("store: last device update: %w", err)
	}
	return id, nil
}

// CrossSigningKey is one of a user's cross-signing keys.
type CrossSigningKey struct {
	UserID string
	// KeyType is "master" or "self_signing".
	KeyType string
	// KeyData is the key object as stored, sent verbatim as the EDU's
	// master_key or self_signing_key.
	KeyData []byte
	// PseudoDeviceID is the version part of the key's id -- everything after
	// "ed25519:".
	//
	// This is what makes cross-signing updates findable at all. Synapse
	// records a key change as a row in device_lists_outbound_pokes whose
	// device_id is this value rather than a real device
	// (devices.py:636). A sender that does not recognise it sends an
	// m.device_list_update for a device that does not exist, and the real
	// key change is never announced -- so the far side keeps trusting a
	// signature that has been rotated away.
	PseudoDeviceID string
}

// GetCrossSigningKeys loads the master and self-signing keys for a set of users.
func (s *Store) GetCrossSigningKeys(ctx context.Context, userIDs []string) ([]CrossSigningKey, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	// DISTINCT ON keeps the newest row per (user, keytype): a rotated key
	// leaves the old one behind, and announcing that would tell the far side
	// to trust a key the user has replaced.
	const q = `
		SELECT DISTINCT ON (user_id, keytype) user_id, keytype, keydata
		FROM e2e_cross_signing_keys
		WHERE user_id = ANY($1) AND keytype IN ('master', 'self_signing')
		ORDER BY user_id, keytype, stream_id DESC`

	rows, err := s.pool.Query(ctx, q, userIDs)
	if err != nil {
		return nil, fmt.Errorf("store: cross-signing keys: %w", err)
	}
	defer rows.Close()

	var out []CrossSigningKey
	for rows.Next() {
		var k CrossSigningKey
		if err := rows.Scan(&k.UserID, &k.KeyType, &k.KeyData); err != nil {
			return nil, fmt.Errorf("store: cross-signing keys: %w", err)
		}
		k.PseudoDeviceID = crossSigningPseudoDeviceID(k.KeyData)
		if k.PseudoDeviceID == "" {
			// A key with no usable id cannot be matched against a poke, so it
			// would silently never be announced. Skipping it is the same
			// outcome, but at least it does not masquerade as a match.
			continue
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// crossSigningPseudoDeviceID extracts the version from a cross-signing key.
//
// The key object carries exactly one entry in `keys`, of the form
// "ed25519:<version>": "<public key>". signedjson calls that version the key's
// device id, and Synapse compares device pokes against it directly.
func crossSigningPseudoDeviceID(keyData []byte) string {
	keys := gjson.GetBytes(keyData, "keys")
	if !keys.IsObject() {
		return ""
	}
	var version string
	keys.ForEach(func(key, _ gjson.Result) bool {
		if _, v, ok := strings.Cut(key.String(), ":"); ok {
			version = v
		}
		return false // exactly one key; take the first and stop
	})
	return version
}

// PresenceState is a user's current presence, for an m.presence EDU.
type PresenceState struct {
	UserID          string
	State           string
	LastActiveTS    int64
	StatusMsg       string
	CurrentlyActive bool
}

// GetPresenceStates reads the current presence for a set of users.
//
// Read at send time rather than taken from the replication row, because the row
// carries only who the update is about and where it goes. Presence changes far
// faster than it can be delivered, so a state captured when the row was written
// would often be stale by the time the transaction goes out -- and stale
// presence is worse than none: it says someone is online who left.
func (s *Store) GetPresenceStates(ctx context.Context, userIDs []string) (map[string]PresenceState, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT DISTINCT ON (user_id) user_id, state,
		       COALESCE(last_active_ts, 0), COALESCE(status_msg, ''),
		       COALESCE(currently_active, false)
		FROM presence_stream
		WHERE user_id = ANY($1)
		ORDER BY user_id, stream_id DESC`

	rows, err := s.pool.Query(ctx, q, userIDs)
	if err != nil {
		return nil, fmt.Errorf("store: presence states: %w", err)
	}
	defer rows.Close()

	out := make(map[string]PresenceState, len(userIDs))
	for rows.Next() {
		var p PresenceState
		if err := rows.Scan(&p.UserID, &p.State, &p.LastActiveTS, &p.StatusMsg,
			&p.CurrentlyActive); err != nil {
			return nil, fmt.Errorf("store: presence states: %w", err)
		}
		out[p.UserID] = p
	}
	return out, rows.Err()
}
