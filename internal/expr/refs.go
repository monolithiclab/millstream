package expr

import (
	"slices"

	"cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
)

// Refs is what the ${expr} segments of one config string read, as far as it can be known statically.
type Refs struct {
	// HasExpr reports whether the string holds any ${expr}; a plain string reads nothing.
	HasExpr bool
	// Vars and Secrets are the names read as vars.name, vars["name"], secrets.name or secrets["name"], sorted and
	// without duplicates.
	Vars    []string
	Secrets []string
	// AllVars and AllSecrets report a use of the whole map (vars, or vars[k] with a computed k), which may read any
	// name.
	AllVars    bool
	AllSecrets bool
}

// TemplateRefs compiles every ${expr} in s, as Interpolate would, and reports the variables and secrets they read.
// It evaluates nothing, so `check` can type-check interpolation and see which config values depend on a secret or
// a required variable without knowing their values. A compile error is returned as Interpolate would return it.
func TemplateRefs(s string) (Refs, error) {
	var r Refs
	segs, err := splitTemplate(s)
	if err != nil {
		return r, err
	}
	for _, seg := range segs {
		if !seg.expr {
			continue
		}
		r.HasExpr = true
		a, _, err := compile(interpEnv, seg.text)
		if err != nil {
			return r, err
		}
		collectRefs(a.NativeRep(), &r)
	}
	slices.Sort(r.Vars)
	slices.Sort(r.Secrets)
	r.Vars, r.Secrets = slices.Compact(r.Vars), slices.Compact(r.Secrets)
	return r, nil
}

// collectRefs adds the names one expression reads. Every identifier vars or secrets is either the operand of a
// field selection or constant index, which names what it reads, or a use of the whole map.
func collectRefs(a *ast.AST, r *Refs) {
	root := ast.NavigateAST(a)
	named := map[string]int{}
	add := func(operand ast.Expr, name string) {
		if operand.Kind() != ast.IdentKind {
			return
		}
		switch operand.AsIdent() {
		case varVars:
			r.Vars = append(r.Vars, name)
		case varSecrets:
			r.Secrets = append(r.Secrets, name)
		default:
			return
		}
		named[operand.AsIdent()]++
	}
	for _, e := range ast.MatchDescendants(root, ast.KindMatcher(ast.SelectKind)) {
		add(e.AsSelect().Operand(), e.AsSelect().FieldName())
	}
	for _, e := range ast.MatchDescendants(root, ast.FunctionMatcher(operators.Index)) {
		args := e.AsCall().Args()
		if len(args) != 2 || args[1].Kind() != ast.LiteralKind {
			continue
		}
		if key, ok := args[1].AsLiteral().(types.String); ok {
			add(args[0], string(key))
		}
	}
	idents := map[string]int{}
	for _, e := range ast.MatchDescendants(root, ast.KindMatcher(ast.IdentKind)) {
		idents[e.AsIdent()]++
	}
	r.AllVars = r.AllVars || idents[varVars] > named[varVars]
	r.AllSecrets = r.AllSecrets || idents[varSecrets] > named[varSecrets]
}
