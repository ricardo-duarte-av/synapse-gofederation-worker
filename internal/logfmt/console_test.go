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
	log := zerolog.New(plain(&out)).With().Str("destination", "pleco.space").Logger()

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
	log := zerolog.New(plain(&out))
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
	log := zerolog.New(plain(&out))
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
	c := plain(&out)
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

// plain is a Console with colour off, so assertions read the text and not the
// escape codes.
func plain(out *bytes.Buffer) *Console {
	c := New(out)
	c.NoColor = true
	return c
}

// Colour is zerolog's, and must survive the fields being reordered: the error
// value is the one that is bold red, wherever in the line it ends up.
func TestColour(t *testing.T) {
	var out bytes.Buffer
	c := New(&out)
	c.NoColor = false
	log := zerolog.New(c)
	log.Warn().Str("destination", "a.example").
		Err(errAs("nope")).Msg("remote rejected a PDU")

	got := out.String()
	for _, want := range []string{
		"\x1b[33mWRN\x1b[0m",                  // warn is yellow
		"\x1b[1mremote rejected a PDU\x1b[0m", // message is bold
		"\x1b[36mdestination=\x1b[0m",         // field names are cyan
		"\x1b[31m\x1b[1mnope\x1b[0m\x1b[0m",   // error value is bold red
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

// NO_COLOR is honoured, which is how a log shipper gets clean text.
func TestNoColorEnv(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	log := zerolog.New(New(&out))
	log.Info().Str("k", "v").Msg("m")
	if strings.Contains(out.String(), "\x1b[") {
		t.Errorf("NO_COLOR was set but the line has escapes: %q", out.String())
	}
}
