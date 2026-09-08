// Package dbtrace measures every database call a pool makes.
//
// It exists because the alternative is instrumenting call sites, and call sites
// are exactly what gets missed: the query nobody thought to time is the one
// that turns out to be slow. pgx calls a QueryTracer for every Query, QueryRow
// and Exec, so hooking the pool measures everything by construction, including
// queries added later by somebody who never reads this file.
//
// Naming is opt-in and coverage is not. A query with no name recorded against
// it is reported as "other" rather than dropped, so an unlabelled query shows up
// as a rising "other" line rather than as nothing at all.
package dbtrace

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Observer is told about each completed query. Kept as a function rather than
// this package importing the metrics package, matching internal/queue and
// internal/sink: the packages that do the work do not decide how it is counted.
type Observer func(query string, took time.Duration, err error)

type nameKey struct{}
type startKey struct{}

// WithQueryName labels the queries made under ctx.
//
// Applied at the call site rather than derived from the SQL, because the SQL
// text is both unstable and unbounded as a metric label -- a typo in whitespace
// would become a new time series.
func WithQueryName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, nameKey{}, name)
}

// QueryName returns the label in effect, or "other".
func QueryName(ctx context.Context) string {
	if s, ok := ctx.Value(nameKey{}).(string); ok && s != "" {
		return s
	}
	return "other"
}

// Tracer implements pgx.QueryTracer.
type Tracer struct{ observe Observer }

// New builds a Tracer reporting to observe. A nil observer disables tracing.
func New(observe Observer) *Tracer {
	if observe == nil {
		return nil
	}
	return &Tracer{observe: observe}
}

var _ pgx.QueryTracer = (*Tracer)(nil)

// TraceQueryStart stamps the start time into the context pgx threads through.
func (t *Tracer) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData,
) context.Context {
	return context.WithValue(ctx, startKey{}, time.Now())
}

// TraceQueryEnd records the duration and the outcome.
//
// For Query (as opposed to Exec and QueryRow) pgx calls this when the rows are
// CLOSED, not when the statement returns, so the duration includes reading the
// result. That is the right measurement for this worker -- a query returning a
// thousand hosts costs what it costs to read them -- but it is worth knowing
// before comparing these numbers against a database's own statement timings.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(startKey{}).(time.Time)
	if !ok {
		return
	}
	t.observe(QueryName(ctx), time.Since(start), data.Err)
}
