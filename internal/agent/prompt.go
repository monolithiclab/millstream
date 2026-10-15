package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"text/template"
)

// PromptData is what a prompt template sees: the item's payload, the step's config and the run's vars, as decoded
// JSON values.
type PromptData struct {
	Payload any
	Config  map[string]any
	Vars    map[string]any
}

// ParsePrompt parses a prompt template (text/template).
func ParsePrompt(name, text string) (*template.Template, error) {
	t, err := template.New(name).Parse(text)
	if err != nil {
		return nil, fmt.Errorf("prompt: %w", err)
	}
	return t, nil
}

// Render renders the prompt for one item.
func Render(t *template.Template, data PromptData) (string, error) {
	var b strings.Builder
	if err := t.Execute(&b, data); err != nil {
		return "", fmt.Errorf("render prompt: %w", err)
	}
	return b.String(), nil
}

// fence matches a fenced code block tagged json; its group is the block's content.
var fence = regexp.MustCompile("(?s)```json[ \t]*\r?\n(.*?)\r?\n?```")

// Extract returns the structured output of a response: the harness's native structured output when it has one,
// else the last fenced ```json block of its text, else the whole text when it is JSON itself.
func Extract(resp Response) (json.RawMessage, error) {
	if len(resp.Structured) > 0 {
		if !json.Valid(resp.Structured) {
			return nil, retryable("the harness's structured output is not JSON")
		}
		return resp.Structured, nil
	}
	if blocks := fence.FindAllStringSubmatch(resp.Text, -1); len(blocks) > 0 {
		body := []byte(strings.TrimSpace(blocks[len(blocks)-1][1]))
		if !json.Valid(body) {
			return nil, retryable("the last fenced json block of the answer is not JSON")
		}
		return body, nil
	}
	if body := bytes.TrimSpace([]byte(resp.Text)); len(body) > 0 && json.Valid(body) {
		return body, nil
	}
	return nil, retryable("the answer holds no structured output: no fenced json block")
}
