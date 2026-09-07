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
	return edus, highest, nil
}

// prevIDs is Synapse's `[prev_id] if prev_id else []`.
func prevIDs(prev int64) []int64 {
	if prev == 0 {
		return []int64{}
	}
	return []int64{prev}
}
