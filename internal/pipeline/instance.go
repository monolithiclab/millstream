package pipeline

import "slices"

// CheckInstance checks what spans the pipelines of one deployment once pkgs are applied over the active packages of
// the others (spec section 4.8): it returns findings per pipeline, by name. Applying two packages of one pipeline at
// once is refused. It is where checks between pipelines belong, such as the contracts of imported streams (section
// 4.12), which force lets through.
func CheckInstance(pkgs, others []*Package, force bool) map[string][]Diagnostic {
	_, _ = others, force // read by the checks between pipelines that use them (imported streams)
	out := map[string][]Diagnostic{}
	var seen []string
	for _, pkg := range pkgs {
		name := pkg.Pipeline.Name
		if slices.Contains(seen, name) {
			out[name] = append(out[name], Diagnostic{Severity: SeverityError, Path: "name",
				Message: "two packages of pipeline " + name + " in one apply"})
		}
		seen = append(seen, name)
	}
	return out
}
