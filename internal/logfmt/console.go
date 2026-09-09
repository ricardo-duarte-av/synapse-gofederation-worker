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
// Here the long text goes last and the fixed fields keep their columns.
package logfmt

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
)

// trailing are rendered after everything else, in this order. They are the
// fields whose length is unbounded.
var trailing = []string{"error"}

// Console writes one line per event: time, level, message, then fields in the
// order they were logged, with the unbounded ones last.
type Console struct {
	Out io.Writer

	mu  sync.Mutex
	buf bytes.Buffer
}

// New builds a Console writing to out.
func New(out io.Writer) *Console { return &Console{Out: out} }

func (c *Console) Write(p []byte) (int, error) {
	if !gjson.ValidBytes(p) {
		// Not something we produced. Pass it through rather than losing it.
		return c.Out.Write(p)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Reset()

	c.buf.WriteString(gjson.GetBytes(p, "time").String())
	c.buf.WriteByte(' ')
	c.buf.WriteString(level(gjson.GetBytes(p, "level").String()))
	if msg := gjson.GetBytes(p, "message").String(); msg != "" {
		c.buf.WriteByte(' ')
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
	c.buf.WriteString(key)
	c.buf.WriteByte('=')
	c.buf.WriteString(quote(v.String()))
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
