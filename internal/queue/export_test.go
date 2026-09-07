package queue

import "time"

// isRunning exposes the transmission loop's state to tests, which need to know
// when a loop has finished rather than guessing with a sleep.
func (d *Destination) isRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running
}

// timeoutAfterSeconds is a readable deadline for the tests that assert a call
// returns rather than blocking.
func timeoutAfterSeconds(n int) <-chan time.Time {
	return time.After(time.Duration(n) * time.Second)
}
