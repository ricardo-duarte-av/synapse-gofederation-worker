// Package logfmt renders zerolog events for a human reading `docker logs`.
//
// It exists because zerolog's own ConsoleWriter moves the error field to the
// FRONT of the line (console.go:218, "Move the 'error' field to the front"),
// unconditionally and regardless of FieldsOrder. Error text is the one field
// with no bounded length -- a remote's rejection reason, a DNS failure, a
// pasted 429 body -- so putting it first pushes every short, fixed-width field
// (destination, event id, counts) to a different column on every line, which
// is exactly what makes a log unscannable.
//
// Here the long text goes last and the fixed fields keep their columns. The
// colours are zerolog's own, so the output still reads the way its does:
// dark-grey timestamp, coloured level, bold message, cyan field names, and a
// bold red error value.
package logfmt

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
)

// ANSI codes, matching zerolog's (console.go:20). Colour is on unless NO_COLOR
// is set, which is also zerolog's rule -- it does not test for a terminal, and
// these logs are read through `docker logs` rather than from a tty.
const (
	colorRed      = 31
	colorGreen    = 32
	colorYellow   = 33
	colorBlue     = 34
	colorCyan     = 36
	colorBold     = 1
	colorDarkGray = 90
)

// trailing are rendered after everything else, in this order. They are the
// fields whose length is unbounded.
var trailing = []string{"error"}

// Console writes one line per event: time, level, message, then fields in the
// order they were logged, with the unbounded ones last.
type Console struct {
	Out io.Writer
	// NoColor strips the ANSI codes. Set from NO_COLOR by New.
	NoColor bool

	mu  sync.Mutex
	buf bytes.Buffer
}

// New builds a Console writing to out.
func New(out io.Writer) *Console {
	return &Console{Out: out, NoColor: os.Getenv("NO_COLOR") != ""}
}

// colorize wraps s in an ANSI code, or returns it unchanged.
func (c *Console) colorize(s string, code int) string {
	if c.NoColor || code == 0 {
		return s
	}
	return "\x1b[" + strconv.Itoa(code) + "m" + s + "\x1b[0m"
}

func (c *Console) Write(p []byte) (int, error) {
	if !gjson.ValidBytes(p) {
		// Not something we produced. Pass it through rather than losing it.
		return c.Out.Write(p)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Reset()

	lvl := gjson.GetBytes(p, "level").String()
	c.buf.WriteString(c.colorize(gjson.GetBytes(p, "time").String(), colorDarkGray))
	c.buf.WriteByte(' ')
	c.buf.WriteString(c.colorize(level(lvl), levelColor(lvl)))
	if msg := gjson.GetBytes(p, "message").String(); msg != "" {
		c.buf.WriteByte(' ')
		// Bold only from info up, as zerolog does: a debug line should not
		// shout louder than the warning above it.
		if levelColor(lvl) != 0 && lvl != "debug" {
			msg = c.colorize(msg, colorBold)
		}
		c.buf.WriteString(msg)
	}

	var deferred []string
	gjson.ParseBytes(p).ForEach(func(k, v gjson.Result) bool {
		key := k.String()
		switch key {
		case "time", "level", "message":
			return true
		}
		if isTrailing(key) {
			deferred = append(deferred, key)
			return true
		}
		c.field(key, v)
		return true
	})
	// In the order trailing declares, not the order they were logged, so two
	// lines carrying the same fields end the same way.
	for _, key := range trailing {
		for _, got := range deferred {
			if got == key {
				c.field(key, gjson.GetBytes(p, escapePath(key)))
			}
		}
	}

	c.buf.WriteByte('\n')
	if _, err := c.Out.Write(c.buf.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Console) field(key string, v gjson.Result) {
	c.buf.WriteByte(' ')
	c.buf.WriteString(c.colorize(key+"=", colorCyan))
	value := quote(v.String())
	if key == "error" {
		value = c.colorize(c.colorize(value, colorBold), colorRed)
	}
	c.buf.WriteString(value)
}

func isTrailing(key string) bool {
	for _, t := range trailing {
		if t == key {
			return true
		}
	}
	return false
}

// escapePath protects a field name from gjson's path syntax, so a key
// containing a dot or a wildcard is looked up literally.
func escapePath(key string) string {
	return strings.NewReplacer(".", `\.`, "*", `\*`, "?", `\?`).Replace(key)
}

// level abbreviates to the fixed width zerolog uses, so the column holds.
func level(l string) string {
	switch l {
	case "trace":
		return "TRC"
	case "debug":
		return "DBG"
	case "info":
		return "INF"
	case "warn":
		return "WRN"
	case "error":
		return "ERR"
	case "fatal":
		return "FTL"
	case "panic":
		return "PNC"
	case "":
		return "???"
	}
	return strings.ToUpper(l)
}

// levelColor is zerolog's LevelColors (globals.go:147). Debug is deliberately
// uncoloured there, and 0 means "leave it alone".
func levelColor(l string) int {
	switch l {
	case "trace":
		return colorBlue
	case "info":
		return colorGreen
	case "warn":
		return colorYellow
	case "error", "fatal", "panic":
		return colorRed
	}
	return 0
}

func quote(s string) string {
	if s == "" {
		return `""`
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e || s[i] == ' ' || s[i] == '"' || s[i] == '\\' {
			return strconv.Quote(s)
		}
	}
	return s
}
