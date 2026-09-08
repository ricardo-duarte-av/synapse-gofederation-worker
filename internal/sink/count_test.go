package sink

import "testing"

// Synapse counts EDUs by type and we could not, which made the first real
// comparison against it unanswerable: our EDU rate was 4.4x lower than the
// Python senders', and with one unlabelled number there was no telling whether
// that meant less presence (a tuning difference) or no typing (a broken
// feature).
func TestCountEDUTypes(t *testing.T) {
	body := []byte(`{"pdus":[{}],"edus":[
		{"edu_type":"m.presence","content":{}},
		{"edu_type":"m.presence","content":{}},
		{"edu_type":"m.typing","content":{}},
		{"content":{}}
	]}`)

	got := countEDUTypes(body)
	want := map[string]int{"m.presence": 2, "m.typing": 1, "unknown": 1}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d", k, got[k], v)
		}
	}

	// A transaction with no EDUs must produce no series at all, rather than a
	// zero for every type this worker has ever sent.
	if n := len(countEDUTypes([]byte(`{"pdus":[{}]}`))); n != 0 {
		t.Errorf("an EDU-less transaction produced %d entries", n)
	}
}
