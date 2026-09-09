package logfmt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The point of this package: the unbounded field goes last, so every fixed
// field before it keeps its column. zerolog's ConsoleWriter puts it first.
func TestErrorTextIsLast(t *testing.T) {
	var out bytes.Buffer
	log := zerolog.New(New(&out)).With().Str("destination", "pleco.space").Logger()

	log.Warn().
		Str("event_id", "$3Kpx").
		Str("room_id", "!XA1h").
		Err(errAs("Server is banned from room")).
		Msg("remote rejected a PDU")

	got := strings.TrimRight(out.String(), "\n")
	want := `WRN remote rejected a PDU destination=pleco.space event_id=$3Kpx room_id=!XA1h error="Server is banned from room"`
	if !strings.HasSuffix(got, want) {
		t.Errorf("line was\n  %s\nwant it to end with\n  %s", got, want)
	}
}

// Fields keep the order they were logged in. Alphabetical sorting would put
// bytes before destination and separate the counts from each other.
func TestFieldsKeepTheOrderTheyWereLogged(t *testing.T) {
	var out bytes.Buffer
	log := zerolog.New(New(&out))
	log.Info().Str("destination", "a.example").Int("pdus", 1).Int("edus", 0).
		Int("bytes", 224).Msg("sent transaction")

	if got := out.String(); !strings.Contains(got, "destination=a.example pdus=1 edus=0 bytes=224") {
		t.Errorf("fields were reordered: %s", got)
	}
}

// A value with a space has to survive, or a multi-word error silently becomes
// several bogus fields.
func TestValuesAreQuotedWhenTheyNeedIt(t *testing.T) {
	var out bytes.Buffer
	log := zerolog.New(New(&out))
	log.Info().Str("plain", "no-spaces").Str("spaced", "two words").Msg("m")

	got := out.String()
	if !strings.Contains(got, `plain=no-spaces`) {
		t.Errorf("an unremarkable value was quoted: %s", got)
	}
	if !strings.Contains(got, `spaced="two words"`) {
		t.Errorf("a value with a space was not quoted: %s", got)
	}
}

// Anything not from us must not be swallowed.
func TestNonJSONPassesThrough(t *testing.T) {
	var out bytes.Buffer
	c := New(&out)
	if _, err := c.Write([]byte("a bare line\n")); err != nil {
		t.Fatal(err)
	}
	if out.String() != "a bare line\n" {
		t.Errorf("passthrough mangled the line: %q", out.String())
	}
}

type errString string

func (e errString) Error() string { return string(e) }
func errAs(s string) error        { return errString(s) }
