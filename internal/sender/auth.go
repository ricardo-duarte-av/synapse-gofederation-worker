package sender

import (
	"context"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/store"
)

// loadAuthEvents fetches an event's auth events.
//
// Only the rescinded-invite rule needs these, and only for a membership leave
// whose sender is not its subject, so this is called through a closure the
// resolver invokes at most once per event and usually not at all.
func (s *Sender) loadAuthEvents(ctx context.Context, e store.Event) ([][]byte, error) {
	refs := gjson.ParseBytes(e.JSON).Get("auth_events").Array()
	if len(refs) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(refs))
	for _, r := range refs {
		// Room version 1 and 2 write auth_events as [event_id, hashes] pairs;
		// every later version writes bare event ids. Both still exist in a
		// database this old, so both are read.
		if r.IsArray() {
			if a := r.Array(); len(a) > 0 {
				ids = append(ids, a[0].String())
			}
			continue
		}
		ids = append(ids, r.String())
	}
	if len(ids) == 0 {
		return nil, nil
	}

	events, err := s.cfg.Store.GetEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.JSON)
	}
	return out, nil
}
