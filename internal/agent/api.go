package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// API defaults.
const (
	DefaultAnthropicURL = "https://api.anthropic.com"
	AnthropicVersion    = "2023-06-01"
	DefaultMaxTokens    = 4096
)

// API is the api harness: one direct model call, to Anthropic's Messages API or to an OpenAI-compatible chat
// completions endpoint (Mistral, a local model). It has no tool loop in M2: an action that declares tools is refused
// before it runs ("api harness tools arrive later"). Neither provider's native structured output is used, since
// OpenAI-compatible endpoints differ in it: the prompt asks for a final fenced JSON block matching the output schema.
// A repair turn sends the conversation so far, each rejected answer followed by the feedback on it.
type API struct {
	// Provider is pipeline.ProviderAnthropic or pipeline.ProviderOpenAI.
	Provider string
	// BaseURL is the endpoint: Anthropic's by default; an OpenAI-compatible one's base, to which
	// /chat/completions is appended (https://api.mistral.ai/v1).
	BaseURL string
	APIKey  string
	// Client sends the requests; nil means http.DefaultClient.
	Client *http.Client
}

// schemaInstruction asks for the answer as a final fenced JSON block, since this harness has no native structured
// output.
const schemaInstruction = "\n\nEnd your answer with a fenced ```json block holding only your result, a JSON value " +
	"valid against this JSON Schema:\n\n```\n%s\n```\n"

// Run implements Harness.
func (a API) Run(ctx context.Context, req Request) (Response, error) {
	if len(req.Tools) > 0 {
		return Response{}, permanent("api harness tools arrive later: declare no tools, or use the claude-code harness")
	}
	if a.APIKey == "" {
		return Response{}, permanent("api harness: no API key: name the pipeline secret holding it in " +
			"runtime.agent.apiKeySecret")
	}
	prompt := req.Prompt
	if len(req.Schema) > 0 {
		prompt += fmt.Sprintf(schemaInstruction, bytes.TrimSpace(req.Schema))
	}
	messages := []map[string]string{{"role": "user", "content": prompt}}
	for _, r := range req.Repairs {
		messages = append(messages, map[string]string{"role": "assistant", "content": r.Answer},
			map[string]string{"role": "user", "content": r.Feedback})
	}
	body, err := json.Marshal(map[string]any{"model": req.Model, "max_tokens": DefaultMaxTokens, "messages": messages})
	if err != nil {
		return Response{}, permanent("api harness: encode request: %v", err)
	}
	url, parse := "", anthropicResponse
	header := http.Header{"Content-Type": {"application/json"}}
	if a.Provider == pipeline.ProviderOpenAI {
		url, parse = strings.TrimSuffix(a.BaseURL, "/")+"/chat/completions", openAIResponse
		header.Set("Authorization", "Bearer "+a.APIKey)
	} else {
		base := a.BaseURL
		if base == "" {
			base = DefaultAnthropicURL
		}
		url = strings.TrimSuffix(base, "/") + "/v1/messages"
		header.Set("X-Api-Key", a.APIKey)
		header.Set("Anthropic-Version", AnthropicVersion)
	}
	resp := Response{Turns: 1, Transcript: []Turn{{Role: "user", Text: messages[len(messages)-1]["content"]}}}
	raw, err := a.post(ctx, url, header, body)
	if err != nil {
		return resp, err
	}
	err = parse(raw, &resp)
	resp.Transcript = append(resp.Transcript, Turn{Role: "assistant", Text: resp.Text, Data: raw})
	return resp, err
}

// post sends the request and returns the response body. A transport failure, a rate limit (429) and a server error
// (5xx, 529 overloaded) are retryable; any other refusal (a bad request, a bad key) is not.
func (a API) post(ctx context.Context, url string, header http.Header, body []byte) (json.RawMessage, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, permanent("api harness: %v", err)
	}
	hreq.Header = header
	client := a.Client
	if client == nil {
		client = http.DefaultClient
	}
	hresp, err := client.Do(hreq) // #nosec G704 -- the endpoint is the manifest's baseUrl, or the provider's own
	if err != nil {
		return nil, retryable("api harness: %v", err)
	}
	defer func() { _ = hresp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(hresp.Body, maxHarnessOutput))
	if err != nil {
		return nil, retryable("api harness: read response: %v", err)
	}
	if hresp.StatusCode/100 != 2 {
		msg := fmt.Sprintf("api harness: %s answered %s: %s", url, hresp.Status, bytes.TrimSpace(raw))
		if hresp.StatusCode == http.StatusTooManyRequests || hresp.StatusCode >= 500 {
			return nil, retryable("%s", msg)
		}
		return nil, permanent("%s", msg)
	}
	if !json.Valid(raw) {
		return nil, retryable("api harness: %s answered something other than JSON", url)
	}
	return raw, nil
}

// anthropicResponse reads a Messages API response: its text blocks and its usage.
func anthropicResponse(raw []byte, resp *Response) error {
	var m struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return retryable("api harness: decode response: %v", err)
	}
	var text strings.Builder
	for _, c := range m.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}
	resp.Text, resp.TokensIn, resp.TokensOut = text.String(), m.Usage.InputTokens, m.Usage.OutputTokens
	return nil
}

// openAIResponse reads a chat completions response: the first choice's message and the usage.
func openAIResponse(raw []byte, resp *Response) error {
	var m struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return retryable("api harness: decode response: %v", err)
	}
	if len(m.Choices) == 0 {
		return retryable("api harness: the response has no choices")
	}
	resp.Text = m.Choices[0].Message.Content
	resp.TokensIn, resp.TokensOut = m.Usage.PromptTokens, m.Usage.CompletionTokens
	return nil
}
