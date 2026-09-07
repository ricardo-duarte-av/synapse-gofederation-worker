package main

import (
	"encoding/base64"
	"time"
)

// base64RawStd is the encoding signedjson uses for signing key seeds: standard
// alphabet, no padding.
var base64RawStd = base64.RawStdEncoding

// replicationWorkTimeout bounds the database work a replication row triggers.
//
// Generous, because the work is a handful of indexed reads and the cost of
// timing out too early is a device update that is never queued. It exists to
// stop a wedged query leaking goroutines, not to enforce latency.
const replicationWorkTimeout = 2 * time.Minute
