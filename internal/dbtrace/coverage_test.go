package dbtrace_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every function that runs a query must name it.
//
// This is a source check rather than a runtime one because the failure it
// prevents is silent by nature. Coverage is structural -- the tracer hooks the
// pool, so an unnamed query is still counted -- but it is counted as "other",
// and "other" is where a hot query hides while every panel looks healthy. That
// is not hypothetical: this check was written after "other" turned out to be
// the single busiest line on the dashboard, at 1.026 queries a second, because
// fourteen call sites had been missed by hand.
//
// It follows the precedent of TestHTTPSinkHasNoAllowlistOfItsOwn: an invariant
// that is easy to state, easy to break by accident, and cheap to assert against
// the AST.
func TestEveryQuerySiteIsNamed(t *testing.T) {
	// Packages that own a connection pool.
	for _, pkg := range []string{"../store", "../state"} {
		files, err := filepath.Glob(filepath.Join(pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			checkFile(t, file)
		}
	}
}

func checkFile(t *testing.T, file string) {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		var queries, named bool
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Query", "QueryRow", "Exec", "SendBatch":
				// Only calls on a pool or transaction, not on arbitrary values.
				queries = true
			case "WithQueryName":
				named = true
			}
			return true
		})
		if queries && !named {
			t.Errorf("%s: %s runs a query without dbtrace.WithQueryName, so it "+
				"is counted as \"other\" and can hide there",
				fset.Position(fn.Pos()), fn.Name.Name)
		}
	}
}
