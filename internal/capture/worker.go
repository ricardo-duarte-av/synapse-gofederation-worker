package capture

import (
	"encoding/json"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/txn"
)

// WorkerCapture adapts a Writer to the sink's Capturer interface.
//
// It records the transaction the worker WOULD have sent, in the same format the
// recording proxy writes for what Synapse actually sent, so the two files can
// be handed to Compare without either being reshaped first.
type WorkerCapture struct {
	w      *Writer
	origin string
}

// NewWorkerCapture wraps a Writer. origin is our own server_name.
func NewWorkerCapture(w *Writer, origin string) *WorkerCapture {
	return &WorkerCapture{w: w, origin: origin}
}

// Captures reports whether a destination is being recorded.
func (c *WorkerCapture) Captures(destination string) bool { return c.w.Captures(destination) }

// Capture records one built transaction.
func (c *WorkerCapture) Capture(req *txn.Request) error {
	// req.Body is the canonical JSON that was signed. Stored verbatim: the
	// whole comparison is about bytes, and re-encoding here would compare our
	// encoder against itself.
	return c.w.Write(Record{
		Time:        time.Now().UTC(),
		Source:      SourceWorker,
		Destination: req.Destination,
		Origin:      c.origin,
		TxnID:       req.TransactionID,
		Path:        req.Path,
		Auth:        req.AuthHeader,
		Body:        json.RawMessage(req.Body),
	})
}

// Close flushes the underlying writer.
func (c *WorkerCapture) Close() error { return c.w.Close() }

// EventIDs lists the event ids in a record, for logging.
func EventIDs(r Record) []string {
	var out []string
	for _, p := range gjson.GetBytes(r.Body, "pdus").Array() {
		if id := p.Get("event_id").String(); id != "" {
			out = append(out, id)
		}
	}
	return out
}
