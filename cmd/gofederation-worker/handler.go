package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/replication"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/sender"
)

// handler turns replication rows into work.
//
// Every stream is handled the way a real federation sender handles it
// (replication/tcp/client.py:472), and the streams a sender ignores are ignored
// here too rather than being treated as unexpected.
type handler struct {
	worker *worker
	log    zerolog.Logger
}

func (h *handler) OnRows(stream string, position int64, rows []replication.Row) {
	metrics.ReplicationRows.WithLabelValues(stream).Add(float64(len(rows)))

	switch stream {
	case replication.StreamEvents:
		// A poke: the row says an event exists and the content comes from the
		// database. Passing the position is all the sender needs.
		h.worker.sender.NotifyNewEvents(position)

	case replication.StreamToDevice:
		h.handleToDevice(rows)

	case replication.StreamDeviceLists:
		h.handleDeviceLists(position, rows)

	case replication.StreamReceipts:
		h.handleReceipts(rows)

	case replication.StreamPresenceFederation:
		h.handlePresenceFederation(rows)

	case replication.StreamTyping:
		// Typing is built HERE, from the room's typing set, rather than
		// arriving ready-made on the federation stream. Being the federation
		// sender means doing that work: on every other instance Synapse's
		// typing handler has no federation sender to hand an EDU to and does
		// nothing at all (typing.py:89 and replication/tcp/client.py:148).
		h.handleTyping(position, rows)

	case replication.StreamFederation:
		// Arbitrary EDUs relayed by FederationRemoteSendQueue. In practice
		// nothing arrives: its only producer is build_and_send_edu, which
		// typing alone calls, and only on an instance that is already a
		// federation sender. Handled regardless, since a queue that is empty
		// by accident of who calls it is not a guarantee.
		h.handleFederation(rows)
	}
}

func (h *handler) handleToDevice(rows []replication.Row) {
	// The entity is a user id for a local delivery or a server name for a
	// federated one; only the latter concerns a sender.
	seen := map[string]bool{}
	var servers []string
	for _, row := range rows {
		entity, remote, ok := replication.ParseToDeviceEntity(row.JSON)
		if !ok || !remote || seen[entity] {
			continue
		}
		seen[entity] = true
		servers = append(servers, entity)
	}
	// Logged even when nothing survives the filter: a to_device row naming a
	// local user looks identical, in a metrics counter, to one naming a remote
	// server we then failed to act on.
	h.log.Debug().Int("rows", len(rows)).Strs("remote_servers", servers).
		Msg("to_device replication rows")
	if len(servers) == 0 {
		return
	}

	// Run off the replication goroutine: a database read here would stall the
	// subscription, and a stalled subscription is how a worker silently falls
	// behind.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
		defer cancel()
		if err := h.worker.devices.HandleToDevice(ctx, servers); err != nil {
			h.log.Error().Err(err).Int("servers", len(servers)).
				Msg("failed to queue to-device messages")
		}
	}()
}

func (h *handler) handleDeviceLists(position int64, rows []replication.Row) {
	// The row carries a user id and a hosts_calculated flag. When hosts have
	// not been calculated there are no outbound pokes to read yet, so there is
	// nothing for a sender to do (replication/tcp/client.py:472).
	any := false
	for _, row := range rows {
		if r, ok := replication.ParseDeviceListRow(row.JSON); ok && r.HostsCalculated {
			any = true
			break
		}
	}
	if !any {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
		defer cancel()
		if err := h.worker.devices.HandleDeviceLists(ctx, position); err != nil {
			h.log.Error().Err(err).Int64("stream_id", position).
				Msg("failed to queue device list updates")
		}
	}()
}

// handleReceipts routes read receipts to the servers in each room.
func (h *handler) handleReceipts(rows []replication.Row) {
	var updates []sender.ReceiptUpdate
	for _, row := range rows {
		r, ok := replication.ParseReceiptRow(row.JSON)
		if !ok {
			continue
		}
		updates = append(updates, sender.ReceiptUpdate{
			RoomID: r.RoomID, ReceiptType: r.ReceiptType, UserID: r.UserID,
			EventID: r.EventID, ThreadID: r.ThreadID, Data: r.Data,
		})
	}
	if len(updates) == 0 {
		return
	}
	// Off the replication goroutine: routing a receipt reads the room's host
	// list, and a database query in the subscriber's path would stall the
	// stream for every other worker's traffic too.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
		defer cancel()
		for _, u := range updates {
			if err := h.worker.ephemeral.HandleReceipt(ctx, u); err != nil {
				h.log.Error().Err(err).Str("room", u.RoomID).Msg("failed to route a receipt")
			}
		}
	}()
}

// handlePresenceFederation routes presence to the destinations named in the
// rows.
func (h *handler) handlePresenceFederation(rows []replication.Row) {
	// Grouped by destination, because one transaction carries one m.presence
	// EDU describing many users -- sending one EDU per user would spend the
	// whole per-transaction EDU budget on presence.
	byDestination := map[string][]string{}
	for _, row := range rows {
		r, ok := replication.ParsePresenceFederationRow(row.JSON)
		if !ok {
			continue
		}
		byDestination[r.Destination] = append(byDestination[r.Destination], r.UserID)
	}
	if len(byDestination) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
		defer cancel()
		for destination, users := range byDestination {
			if err := h.worker.ephemeral.HandlePresence(ctx, destination, users); err != nil {
				h.log.Error().Err(err).Str("destination", destination).
					Msg("failed to route presence")
			}
		}
	}()
}

// handleTyping turns the room typing sets into federation EDUs.
func (h *handler) handleTyping(position int64, rows []replication.Row) {
	parsed := make([]sender.TypingRow, 0, len(rows))
	for _, row := range rows {
		r, ok := replication.ParseTypingRow(row.JSON)
		if !ok {
			continue
		}
		parsed = append(parsed, sender.TypingRow{RoomID: r.RoomID, UserIDs: r.UserIDs})
	}
	if len(parsed) == 0 {
		return
	}
	// Off the replication goroutine: this resolves room membership from the
	// database, and blocking the subscriber would stall every other stream.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
		defer cancel()
		h.worker.typing.HandleRows(ctx, position, parsed)
	}()
}

// handleFederation routes EDUs another worker has handed us.
func (h *handler) handleFederation(rows []replication.Row) {
	for _, row := range rows {
		r, ok := replication.ParseFederationRow(row.JSON)
		if !ok {
			continue
		}
		switch r.Kind {
		case "k":
			if r.Destination == "" {
				continue
			}
			// Keyed by type AND key, so typing for one room does not clobber
			// typing for another.
			h.worker.ephemeral.HandleTyping(r.Destination, r.EDUType+"|"+r.Key, r.Content)
		case "e":
			if r.Destination == "" {
				continue
			}
			h.worker.ephemeral.HandleEDU(r.Destination, r.EDUType, r.Content)
		case "pd":
			// Presence carried inline rather than by reference. Handled by
			// re-reading the state for consistency with the
			// presence_federation path, which is the one this deployment uses.
			userID := gjson.GetBytes(r.PresenceState, "user_id").String()
			if userID == "" || len(r.PresenceDestinations) == 0 {
				continue
			}
			dests := r.PresenceDestinations
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), replicationWorkTimeout)
				defer cancel()
				for _, d := range dests {
					if err := h.worker.ephemeral.HandlePresence(ctx, d, []string{userID}); err != nil {
						h.log.Error().Err(err).Str("destination", d).
							Msg("failed to route presence")
					}
				}
			}()
		}
	}
}

func (h *handler) OnPosition(stream, _ string, position int64) {
	if stream == replication.StreamEvents {
		// A POSITION jumps the stream forward over rows nobody sent us, which
		// still means there are events to pick up.
		h.worker.sender.NotifyNewEvents(position)
	}
}

func (h *handler) OnRemoteServerUp(server string) {
	// A destination somebody else has seen working. Our backoff for it is now
	// stale: that is not proof WE can reach it, but retrying once and failing
	// is cheap next to leaving a working server unreachable for hours.
	if !h.worker.cfg.ShouldHandle(server) {
		return
	}
	h.worker.limiter.Recovered(server)
	q := h.worker.queues.Get(server)
	if pdus, edus := q.Pending(); pdus == 0 && edus == 0 {
		return
	}
	h.log.Debug().Str("destination", server).Msg("remote server is up; waking its queue")
	h.worker.queues.Wake(q)
}
