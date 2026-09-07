// Package capture records federation transactions, from either side, in one
// shared format.
//
// Two producers write it. cmd/fedrecorder sits in front of a test homeserver
// and records what Synapse ACTUALLY sent; the worker records what it WOULD
// have sent to the same destination. cmd/fedcompare diffs the two.
//
// One format for both sides is the point: a comparison between two files with
// different shapes is a comparison of two parsers as much as of two senders.
package capture

import (
	"encoding/json"
	"time"
)

// Source says which side produced a record.
type Source string

const (
	// SourceSynapse is a transaction observed on the wire by the recording
	// proxy -- ground truth.
	SourceSynapse Source = "synapse"
	// SourceWorker is a transaction this worker assembled and would have sent.
	SourceWorker Source = "gofed"
)

// Record is one federation transaction.
type Record struct {
	Time        time.Time `json:"time"`
	Source      Source    `json:"source"`
	Destination string    `json:"destination"`
	// Origin is the sending server, taken from the body rather than the
	// header: the body is what was signed.
	Origin string `json:"origin,omitempty"`
	TxnID  string `json:"txn_id"`
	// Path is the request path, which is also what the signature covers.
	Path string `json:"path"`
	// Auth is the full Authorization header. Recorded because a malformed one
	// is a failure mode that leaves the body looking perfectly correct.
	Auth string `json:"auth,omitempty"`
	// Body is the transaction as it appeared on the wire, verbatim. Never
	// re-encoded: re-marshalling would change the bytes and destroy the only
	// thing worth comparing.
	Body json.RawMessage `json:"body"`
	// Status is the response code, on the proxy side only.
	Status int `json:"status,omitempty"`
	// Error is set when the proxy could not reach the upstream. A failed
	// delivery is still evidence of what was sent.
	Error string `json:"error,omitempty"`
}

// Transaction is the decoded body.
type Transaction struct {
	Origin         string            `json:"origin"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PDUs           []json.RawMessage `json:"pdus"`
	EDUs           []EDU             `json:"edus"`
}

// EDU is one ephemeral unit.
type EDU struct {
	Type    string          `json:"edu_type"`
	Content json.RawMessage `json:"content"`
}

// Decode parses a record's body.
func (r Record) Decode() (Transaction, error) {
	var t Transaction
	if err := json.Unmarshal(r.Body, &t); err != nil {
		return Transaction{}, err
	}
	return t, nil
}
