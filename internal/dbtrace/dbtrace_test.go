package dbtrace

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// An unnamed query must be counted as "other", not dropped. Coverage is the
// point of tracing at the pool: a query nobody labelled should appear as a
// rising "other" line rather than as nothing at all.
func TestUnnamedQueriesAreCountedAsOther(t *testing.T) {
	var got string
	tr := New(func(query string, _ time.Duration, _ error) { got = query })

	ctx := tr.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{})
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	if got != "other" {
		t.Errorf("query = %q, want %q", got, "other")
	}
}

func TestNamedQueriesKeepTheirName(t *testing.T) {
	var got string
	var took time.Duration
	tr := New(func(query string, d time.Duration, _ error) { got, took = query, d })

	ctx := WithQueryName(context.Background(), "joined_hosts_at_state_group")
	ctx = tr.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{})
	time.Sleep(2 * time.Millisecond)
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	if got != "joined_hosts_at_state_group" {
		t.Errorf("query = %q", got)
	}
	if took <= 0 {
		t.Errorf("took = %v, want a measured duration", took)
	}
}

// A fast error is the fastest query there is, so the duration alone would read
// as excellent performance. The error has to reach the observer.
func TestErrorsReachTheObserver(t *testing.T) {
	var gotErr error
	tr := New(func(_ string, _ time.Duration, err error) { gotErr = err })

	ctx := tr.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{})
	want := errors.New("relation does not exist")
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: want})

	if !errors.Is(gotErr, want) {
		t.Errorf("err = %v, want %v", gotErr, want)
	}
}

// A nil observer means tracing is off, and must not leave pgx holding a tracer
// that panics on the first query.
func TestNilObserverDisablesTracing(t *testing.T) {
	if tr := New(nil); tr != nil {
		t.Errorf("New(nil) = %v, want nil so pgx is given no tracer", tr)
	}
}

// TraceQueryEnd without a matching start must not record a duration measured
// from the zero time, which would be fifty years.
func TestEndWithoutStartRecordsNothing(t *testing.T) {
	called := false
	tr := New(func(string, time.Duration, error) { called = true })

	tr.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})

	if called {
		t.Error("recorded a query that never started")
	}
}
