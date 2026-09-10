package logfmt

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// zerolog writes from every goroutine that logs, and this worker logs from one
// per destination. A dropped or interleaved line here would look exactly like
// "the send happened but nothing was logged".
func TestNoLinesLostUnderConcurrency(t *testing.T) {
	var out bytes.Buffer
	log := zerolog.New(plain(&out))

	const writers, each = 32, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			l := log.With().Str("destination", "d.example").Logger()
			for i := 0; i < each; i++ {
				l.Info().Int("w", w).Int("i", i).Int("bytes", 224).
					Msg("sent transaction")
			}
		}(w)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != writers*each {
		t.Fatalf("got %d lines, want %d", len(lines), writers*each)
	}
	for _, l := range lines {
		if !strings.HasPrefix(l, "INF sent transaction destination=d.example w=") {
			t.Fatalf("garbled line: %q", l)
		}
	}
}

// A long value must not truncate the line or the fields after it.
func TestLongValuesSurvive(t *testing.T) {
	var out bytes.Buffer
	log := zerolog.New(plain(&out))
	long := strings.Repeat("x", 64*1024)
	log.Warn().Str("destination", "d.example").Err(errAs(long)).Msg("transaction failed")

	got := out.String()
	if !strings.Contains(got, "destination=d.example") || !strings.Contains(got, long) {
		t.Errorf("long line lost content (%d bytes out)", len(got))
	}
}
