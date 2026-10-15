package expr

import (
	"fmt"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
)

// native converts a CEL value to plain Go: nil, bool, int64, uint64, float64, string, []byte, time.Time,
// time.Duration, []any or map[string]any.
func native(v ref.Val) (any, error) {
	switch t := v.(type) {
	case types.Null:
		return nil, nil
	case types.Bool:
		return bool(t), nil
	case types.Int:
		return int64(t), nil
	case types.Uint:
		return uint64(t), nil
	case types.Double:
		return float64(t), nil
	case types.String:
		return string(t), nil
	case types.Bytes:
		return []byte(t), nil
	case types.Timestamp:
		return t.Time, nil
	case types.Duration:
		return t.Duration, nil
	case traits.Mapper:
		return nativeMap(t)
	case traits.Lister:
		return nativeList(t)
	default:
		return nil, fmt.Errorf("unsupported result type %s", v.Type())
	}
}

func nativeList(l traits.Lister) ([]any, error) {
	out := []any{}
	for it := l.Iterator(); it.HasNext() == types.True; {
		e, err := native(it.Next())
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func nativeMap(m traits.Mapper) (map[string]any, error) {
	out := map[string]any{}
	for it := m.Iterator(); it.HasNext() == types.True; {
		k := it.Next()
		key, ok := k.(types.String)
		if !ok {
			return nil, fmt.Errorf("map key of type %s: only string keys are supported", k.Type())
		}
		e, err := native(m.Get(k))
		if err != nil {
			return nil, err
		}
		out[string(key)] = e
	}
	return out, nil
}
