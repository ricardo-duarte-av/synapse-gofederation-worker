package queue

import "errors"

// errNotDelivered is returned when a sink reports a transaction was not
// accepted without giving an error of its own.
var errNotDelivered = errors.New("queue: transaction was not delivered")
