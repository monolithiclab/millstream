//go:build e2e

package protocol

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// A message of exactly MaxLine bytes is read in full, and the last line needs no newline.
func TestReadLimitsFullLine(t *testing.T) {
	t.Parallel()
	payload := strings.Repeat("x", MaxLine-len(`{"type":"emit","payload":""}`))
	line := `{"type":"emit","payload":"` + payload + `"}`
	m, err := NewReader(strings.NewReader(line)).Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if e, ok := m.(Emit); !ok || len(e.Payload) != len(payload)+2 {
		t.Fatalf("got %T", m)
	}
}

// An oversized payload is refused by the writer as well as an oversized key.
func TestWriteOversizedPayload(t *testing.T) {
	t.Parallel()
	err := NewWriter(io.Discard).Write(Emit{Payload: json.RawMessage(`"` + strings.Repeat("x", MaxLine) + `"`)})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want exceeds", err)
	}
}

// A line one byte over MaxLine is refused, with or without its newline.
func TestReadOversizedFullLine(t *testing.T) {
	t.Parallel()
	for name, input := range map[string]string{
		"with newline": `{"type":"emit","payload":"` + strings.Repeat("x", MaxLine) + `"}` + "\n",
		"last line":    strings.Repeat(" ", MaxLine+1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewReader(strings.NewReader(input)).Read(); err == nil ||
				!strings.Contains(err.Error(), "line longer than") {
				t.Fatalf("err = %v, want line longer than", err)
			}
		})
	}
}
