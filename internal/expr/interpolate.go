package expr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// InterpCtx is what config interpolation sees: the run's resolved vars and secrets, and the run itself.
type InterpCtx struct {
	Vars    map[string]any
	Run     Run
	Secrets map[string]string
}

// Interpolate returns a copy of config with every ${expr} in its strings evaluated over vars, run and secrets,
// through nested maps and lists. A string that is exactly one ${expr} takes the expression's type
// ("${vars.maxPrice}" is an integer); otherwise each result is spliced into the string. Errors name the config key
// path and the expression.
func Interpolate(config map[string]any, ctx InterpCtx) (map[string]any, error) {
	act := map[string]any{
		varVars:    orEmpty(ctx.Vars),
		varRun:     ctx.Run.value(),
		varSecrets: orEmpty(ctx.Secrets),
	}
	out, err := interpolateValue(config, "", act)
	if err != nil {
		return nil, err
	}
	m, _ := out.(map[string]any)
	return m, nil
}

func interpolateValue(v any, path string, act map[string]any) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		if t == nil {
			return nil, nil
		}
		out := make(map[string]any, len(t))
		for k, child := range t {
			r, err := interpolateValue(child, joinKey(path, k), act)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, child := range t {
			r, err := interpolateValue(child, fmt.Sprintf("%s[%d]", path, i), act)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	case string:
		r, err := interpolateString(t, act)
		if err != nil {
			return nil, fmt.Errorf("config %s: %w", path, err)
		}
		return r, nil
	default:
		return v, nil
	}
}

func joinKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func interpolateString(s string, act map[string]any) (any, error) {
	segs, err := splitTemplate(s)
	if err != nil {
		return nil, err
	}
	if len(segs) == 1 && segs[0].expr {
		return evalInterp(segs[0].text, act)
	}
	var b strings.Builder
	for _, seg := range segs {
		if !seg.expr {
			b.WriteString(seg.text)
			continue
		}
		v, err := evalInterp(seg.text, act)
		if err != nil {
			return nil, err
		}
		text, err := splice(v)
		if err != nil {
			return nil, fmt.Errorf("splice %q: %w", seg.text, err)
		}
		b.WriteString(text)
	}
	return b.String(), nil
}

func evalInterp(src string, act map[string]any) (any, error) {
	_, prg, err := compile(interpEnv, src)
	if err != nil {
		return nil, err
	}
	out, _, err := prg.Eval(act)
	if err != nil {
		return nil, evalErr(src, err)
	}
	v, err := native(out)
	if err != nil {
		return nil, fmt.Errorf("evaluate %q: %w", src, err)
	}
	return v, nil
}

// segment is a literal run of text or the source of one ${expr}.
type segment struct {
	text string
	expr bool
}

// splitTemplate splits s into literal text and ${expr} segments. The closing brace is found by counting braces
// outside CEL string literals, so map literals and strings containing braces work inside an expression.
func splitTemplate(s string) ([]segment, error) {
	var segs []segment
	for {
		start := strings.Index(s, "${")
		if start < 0 {
			if s != "" {
				segs = append(segs, segment{text: s})
			}
			return segs, nil
		}
		if start > 0 {
			segs = append(segs, segment{text: s[:start]})
		}
		end, err := closingBrace(s, start+2)
		if err != nil {
			return nil, err
		}
		src := strings.TrimSpace(s[start+2 : end])
		if src == "" {
			return nil, fmt.Errorf("empty expression at offset %d", start)
		}
		segs = append(segs, segment{text: src, expr: true})
		s = s[end+1:]
	}
}

// closingBrace returns the index of the brace closing the expression that starts at from.
func closingBrace(s string, from int) (int, error) {
	depth := 0
	var quote byte
	for i := from; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '{':
			depth++
		case c == '}':
			if depth == 0 {
				return i, nil
			}
			depth--
		}
	}
	return 0, fmt.Errorf("unterminated ${ at offset %d", from-2)
}

// splice formats a value for insertion into a string: scalars as text, lists and maps as JSON.
func splice(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "null", nil
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case uint64:
		return strconv.FormatUint(t, 10), nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano), nil
	case time.Duration:
		return t.String(), nil
	default:
		b, err := json.Marshal(t)
		return string(b), err
	}
}
