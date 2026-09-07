package main

import (
	"context"
	"time"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/config"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/retry"
	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/state"
)

// bookkeepingTimeout bounds one write. Generous, because losing a deletion is
// worse than a slow one: the row comes back on the next pass and is sent again.
const bookkeepingTimeout = 30 * time.Second

// onEDUsDelivered performs the deletions a delivered transaction makes due.
//
// This is the point of primary mode. A real federation sender's cursor for
// to-device messages and device pokes IS the deletion of those rows -- Synapse
// does it in __aexit__, only when the transaction succeeded, and a sender that
// skips it re-reads the same rows forever and grows the outbox without bound.
//
// A failure here is logged and not retried. The rows survive, so the next pass
// sends them again and the receiving server deduplicates on message_id; the
// alternative, retrying inline, would hold a destination's transmission loop on
// a database that is already unhappy.
func (w *worker) onEDUsDelivered(destination string, toDeviceUpTo, deviceListUpTo int64) {
	if w.writer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()

	if toDeviceUpTo > 0 {
		if err := w.writer.DeleteDeviceMsgsForRemote(ctx, destination, toDeviceUpTo); err != nil {
			metrics.WriteOps.WithLabelValues("delete_to_device", "error").Inc()
			w.log.Error().Err(err).Str("destination", destination).Int64("up_to", toDeviceUpTo).
				Msg("failed to delete delivered to-device messages; they will be sent again")
		} else {
			metrics.WriteOps.WithLabelValues("delete_to_device", "ok").Inc()
		}
	}
	if deviceListUpTo > 0 {
		// Deletes the pokes AND records the highest stream id per user, in one
		// transaction. The second half is where the next batch's prev_id comes
		// from, so losing it breaks the chain a receiver uses to notice a gap.
		if err := w.writer.MarkAsSentDevicesByRemote(ctx, destination, deviceListUpTo); err != nil {
			metrics.WriteOps.WithLabelValues("mark_devices_sent", "error").Inc()
			w.log.Error().Err(err).Str("destination", destination).Int64("up_to", deviceListUpTo).
				Msg("failed to mark device list updates sent; prev_id chaining will lag")
		} else {
			metrics.WriteOps.WithLabelValues("mark_devices_sent", "ok").Inc()
		}
	}
}

// onDelivered advances a destination's catch-up cursor after a delivered
// transaction (transactions.py:359).
//
// In shadow mode this goes to our own table and Synapse's is untouched; in
// primary mode it is Synapse's, because there is no other sender to keep it.
func (w *worker) onDelivered(destination string, streamOrdering int64) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()

	if w.writer == nil {
		return
	}
	if err := w.writer.SetDestinationLastSuccessfulStreamOrdering(ctx, destination, streamOrdering); err != nil {
		metrics.WriteOps.WithLabelValues("last_successful_ordering", "error").Inc()
		w.log.Error().Err(err).Str("destination", destination).
			Msg("failed to advance the catch-up cursor")
		return
	}
	metrics.WriteOps.WithLabelValues("last_successful_ordering", "ok").Inc()
}

// recordRoutes writes the record of which destinations are owed which room.
//
// Synapse writes destination_rooms BEFORE the retry filter, so an event is
// recorded as owed to a server that is currently down and catch-up can find it
// later (federation/sender/__init__.py:828). A primary sender must do the same
// or nothing survives its own downtime.
func (w *worker) recordRoutes(ctx context.Context, destinations []string, roomID string, streamOrdering int64) error {
	if w.writer == nil {
		return nil
	}
	if err := w.writer.StoreDestinationRoomsEntries(ctx, destinations, roomID, streamOrdering); err != nil {
		metrics.WriteOps.WithLabelValues("destination_rooms", "error").Inc()
		return err
	}
	metrics.WriteOps.WithLabelValues("destination_rooms", "ok").Inc()
	return nil
}

// recordPosition writes our events position where Synapse keeps it.
//
// Not optional for a primary. Synapse rewrites federation_stream_position at
// startup from MIN(stream_id) across the configured senders
// (stream.py:2162), so a row we never advance would rewind the homeserver's
// entire federation position the next time it restarts -- resending everything
// since the worker was deployed.
func (w *worker) recordPosition(ctx context.Context, streamID int64) error {
	if w.writer == nil {
		return nil
	}
	if err := w.writer.UpdateFederationOutPos(ctx, "events", w.cfg.WorkerName, streamID); err != nil {
		metrics.WriteOps.WithLabelValues("federation_stream_position", "error").Inc()
		return err
	}
	metrics.WriteOps.WithLabelValues("federation_stream_position", "ok").Inc()
	return nil
}

// onSendOutcome updates the destination's backoff after an attempt.
//
// The growth and the jitter live in internal/retry rather than here, so the
// same arithmetic governs both the gate in front of the transmission loop and
// the state written to the database. Two implementations of a backoff is one
// too many: they would disagree under exactly the conditions that make a
// backoff matter.
func (w *worker) onSendOutcome(destination string, delivered bool) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()

	if delivered {
		w.limiter.Success(ctx, destination)
		return
	}
	t := w.limiter.Failure(ctx, destination)
	w.log.Debug().
		Str("destination", destination).
		Dur("retry_in", time.Duration(t.RetryInterval)*time.Millisecond).
		Msg("destination failed; backing off")
}

// persistTimings writes a destination's backoff to OUR table, in primary mode.
//
// Not Synapse's `destinations`, even though that is where a Python sender keeps
// it and we are otherwise doing a sender's bookkeeping. Synapse caches those
// timings and only its own writes invalidate the cache, so a backoff we cleared
// from outside the process would go on suppressing Synapse's own outbound
// federation for as long as the cached interval said. The full reasoning, and
// why publishing the invalidation would be worse, is on state.SetRetryTimings.
//
// A shadow persists nothing at all: it keeps the backoff in memory for its own
// lifetime, because writing anywhere is what a shadow must never do.
func (w *worker) persistTimings(ctx context.Context, destination string, t retry.Timings) error {
	if w.writer == nil || w.cursors == nil {
		return nil
	}
	err := w.cursors.SetRetryTimings(ctx, destination, state.RetryTimings{
		FailureTS: t.FailureTS, RetryLastTS: t.RetryLastTS, RetryInterval: t.RetryInterval,
	})
	if err != nil {
		metrics.WriteOps.WithLabelValues("retry_timings", "error").Inc()
		w.log.Error().Err(err).Str("destination", destination).Msg("failed to persist a backoff")
		return err
	}
	metrics.WriteOps.WithLabelValues("retry_timings", "ok").Inc()
	return nil
}

// loadTimings seeds the backoff for destinations this process has not seen yet.
//
// Ours first, Synapse's as a fallback, and the fallback is not a leftover: a
// primary taking over from a Python sender starts with an empty table of its
// own, and a quarter of a real destination list has been unreachable for years.
// Ignoring that would mean waking every dead server once on every deploy.
//
// A shadow reads only Synapse's, which is the point of a shadow: it must make
// the same decisions from the same state as the sender it is compared against.
func (w *worker) loadTimings(ctx context.Context, dests []string) (map[string]retry.Timings, error) {
	out := make(map[string]retry.Timings, len(dests))

	synapse, err := w.db.GetDestinationRetryTimings(ctx, dests)
	if err != nil {
		return nil, err
	}
	for d, t := range synapse {
		out[d] = retry.Timings{
			FailureTS: t.FailureTS, RetryLastTS: t.RetryLastTS, RetryInterval: t.RetryInterval,
		}
	}

	// Ours wins where it exists, because it is the only record of what THIS
	// sender has seen. Synapse's row for the same destination is its client's
	// experience, which is a different question and may be much older.
	if w.cfg.Mode == config.ModePrimary && w.cursors != nil {
		mine, err := w.cursors.GetRetryTimings(ctx, dests)
		if err != nil {
			return nil, err
		}
		for d, t := range mine {
			out[d] = retry.Timings{
				FailureTS: t.FailureTS, RetryLastTS: t.RetryLastTS, RetryInterval: t.RetryInterval,
			}
		}
	}
	return out, nil
}
