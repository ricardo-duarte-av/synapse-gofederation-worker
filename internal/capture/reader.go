package capture

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// maxLineBytes bounds one record. A federation transaction is at most 50 PDUs,
// each capped at 64 KiB by the spec, so a few megabytes is generous; the bound
// is here so a truncated or corrupt file cannot exhaust memory.
const maxLineBytes = 16 << 20

// ReadFile loads every record from a capture file.
//
// Malformed lines are skipped and counted rather than failing the read. A
// capture is evidence collected from a live system: a truncated last line
// because the process was killed mid-write is normal, and refusing to read the
// other 40,000 records because of it would be perverse.
func ReadFile(path string) (records []Record, skipped int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("capture: %w", err)
	}
	defer f.Close()
	return Read(f)
}

// Read loads records from a stream.
func Read(r io.Reader) (records []Record, skipped int, err error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			skipped++
			continue
		}
		records = append(records, rec)
	}
	if err := sc.Err(); err != nil {
		// A line longer than the bound ends the read, but what was already
		// parsed is still returned: partial evidence beats none.
		return records, skipped, fmt.Errorf("capture: %w", err)
	}
	return records, skipped, nil
}
