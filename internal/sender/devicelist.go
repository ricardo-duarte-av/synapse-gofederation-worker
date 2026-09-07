package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// buildDeviceListEDUs turns device pokes into m.device_list_update EDUs, with
// the body Synapse builds (devices.py:848-889).
//
// The fields that were missing before are not decoration:
//
//   - prev_id chains the updates. A receiver compares it against the last
//     stream id it saw for that user and, on a gap, throws away what it knows
//     and resyncs. Omitting it entirely -- which an earlier version did -- means
//     the receiver can never detect a gap, so a missed update is never
//     repaired and it keeps encrypting to a device that may be gone.
//   - deleted says the device no longer exists. Without it a removed device
//     looks like one that simply has no keys.
//   - keys carry the device's identity. A device list update without them tells
//     the receiver a device exists but not how to encrypt to it.
//
// Confirmed against a real update: Synapse sent device_display_name and
// prev_id where we sent neither.
func (d *Devices) buildDeviceListEDUs(
	ctx context.Context, destination string, fromStreamID int64, pokes []store.DevicePoke,
) ([]txn.EDU, int64, error) {
	if len(pokes) == 0 {
		return nil, fromStreamID, nil
	}

	userIDs := make([]string, 0, len(pokes))
	deviceIDs := make([]string, 0, len(pokes))
	for _, p := range pokes {
		userIDs = append(userIDs, p.UserID)
		deviceIDs = append(deviceIDs, p.DeviceID)
	}

	// A poke whose device_id is a user's cross-signing key version is a KEY
	// change, not a device change (devices.py:692). Treating it as a device
	// sends an m.device_list_update for a device that does not exist and never
	// announces the rotation -- so the far side goes on trusting a signing key
	// the user has replaced, which is an E2EE trust failure rather than a
	// missing feature.
	crossSigning, err := d.store.GetCrossSigningKeys(ctx, userIDs)
	if err != nil {
		return nil, fromStreamID, err
	}
	keyByPseudoDevice := map[string]store.CrossSigningKey{}
	for _, k := range crossSigning {
		keyByPseudoDevice[store.DeviceDetailKey(k.UserID, k.PseudoDeviceID)] = k
	}

	details, err := d.store.GetDeviceDetails(ctx, userIDs, deviceIDs)
	if err != nil {
		return nil, fromStreamID, err
	}

	// Grouped by user, because prev_id chains per user rather than globally.
	byUser := map[string][]store.DevicePoke{}
	order := make([]string, 0, len(pokes))
	for _, p := range pokes {
		if _, seen := byUser[p.UserID]; !seen {
			order = append(order, p.UserID)
		}
		byUser[p.UserID] = append(byUser[p.UserID], p)
	}

	var edus []txn.EDU
	highest := fromStreamID
	// Accumulated per user, because one user's master and self-signing keys
	// arrive as two separate pokes and Synapse sends them as ONE EDU carrying
	// both (devices.py:717).
	signing := map[string]map[string]json.RawMessage{}

	for _, userID := range order {
		// The first update's prev_id is the last one already delivered to this
		// destination for this user.
		prevID, err := d.store.GetLastDeviceUpdateForRemoteUser(ctx, destination, userID, fromStreamID)
		if err != nil {
			return nil, fromStreamID, err
		}

		// Stream order within the user, so the chain is built in the order the
		// receiver will read it.
		ups := byUser[userID]
		sort.Slice(ups, func(i, j int) bool { return ups[i].StreamID < ups[j].StreamID })

		for _, p := range ups {
			// Cross-signing key changes short-circuit the device path
			// entirely: they carry no prev_id chain and no device fields.
			if k, ok := keyByPseudoDevice[store.DeviceDetailKey(p.UserID, p.DeviceID)]; ok {
				signing[p.UserID] = mergeSigningKey(signing[p.UserID], k)
				if p.StreamID > highest {
					highest = p.StreamID
				}
				continue
			}

			body := map[string]any{
				"user_id":   p.UserID,
				"device_id": p.DeviceID,
				"stream_id": p.StreamID,
				// An empty list rather than a missing key when there is no
				// predecessor: Synapse sends [] and the receiver distinguishes
				// "no previous update" from "the sender forgot".
				"prev_id": prevIDs(prevID),
			}

			det, ok := details[store.DeviceDetailKey(p.UserID, p.DeviceID)]
			switch {
			case !ok || !det.Exists:
				body["deleted"] = true
			default:
				if len(det.KeysJSON) > 0 {
					var keys json.RawMessage = det.KeysJSON
					body["keys"] = keys
				}
				// Only when the homeserver allows it; the config is Synapse's
				// and is read from homeserver.yaml rather than assumed.
				if d.allowDeviceNameLookup && det.DisplayName != "" {
					body["device_display_name"] = det.DisplayName
				}
			}

			content, err := json.Marshal(body)
			if err != nil {
				return nil, fromStreamID, fmt.Errorf("sender: encoding device list update: %w", err)
			}
			edus = append(edus, txn.EDU{Type: txn.EDUTypeDeviceListUpdate, Content: content})

			// The next update in this batch chains from this one.
			prevID = p.StreamID
			if p.StreamID > highest {
				highest = p.StreamID
			}
		}
	}

	// Emitted last, and twice each. Synapse sends both m.signing_key_update
	// and the legacy org.matrix.signing_key_update with identical content
	// (devices.py:760), because servers that predate the stable name only
	// understand the unstable one. Sending just the stable name would leave
	// those servers trusting a rotated key.
	for _, userID := range sortedKeys(signing) {
		body := map[string]any{"user_id": userID}
		for field, key := range signing[userID] {
			body[field] = key
		}
		content, err := json.Marshal(body)
		if err != nil {
			return nil, fromStreamID, fmt.Errorf("sender: encoding signing key update: %w", err)
		}
		edus = append(edus,
			txn.EDU{Type: txn.EDUTypeSigningKeyUpdate, Content: content},
			txn.EDU{Type: txn.EDUTypeUnstableSigningKeyUpdate, Content: content})
	}

	return edus, highest, nil
}

// mergeSigningKey folds one cross-signing key into a user's pending update.
func mergeSigningKey(into map[string]json.RawMessage, k store.CrossSigningKey) map[string]json.RawMessage {
	if into == nil {
		into = map[string]json.RawMessage{}
	}
	switch k.KeyType {
	case "master":
		into["master_key"] = k.KeyData
	case "self_signing":
		into["self_signing_key"] = k.KeyData
	}
	return into
}

// sortedKeys gives a stable emission order, so two runs over the same pokes
// produce the same transaction and a capture comparison stays readable.
func sortedKeys(m map[string]map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// prevIDs is Synapse's `[prev_id] if prev_id else []`.
func prevIDs(prev int64) []int64 {
	if prev == 0 {
		return []int64{}
	}
	return []int64{prev}
}
