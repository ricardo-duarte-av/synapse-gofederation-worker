package main

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/replication"
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

func (h *handler) OnPosition(stream, _ string, position int64) {
	if stream == replication.StreamEvents {
		// A POSITION jumps the stream forward over rows nobody sent us, which
		// still means there are events to pick up.
		h.worker.sender.NotifyNewEvents(position)
	}
}

func (h *handler) OnRemoteServerUp(server string) {
	// A destination somebody else has seen working. A real sender wakes it
	// immediately; we do the same, which costs nothing if it has no queue.
	if !h.worker.cfg.ShouldHandle(server) {
		return
	}
	q := h.worker.queues.Get(server)
	if pdus, edus := q.Pending(); pdus == 0 && edus == 0 {
		return
	}
	h.log.Debug().Str("destination", server).Msg("remote server is up; waking its queue")
	h.worker.queues.Wake(context.Background(), q)
}
