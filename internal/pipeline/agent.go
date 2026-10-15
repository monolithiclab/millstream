package pipeline

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"path"
	"slices"
	"strings"
)

// Agent harnesses (spec section 7.1).
const (
	HarnessClaudeCode = "claude-code"
	HarnessAPI        = "api"
)

// Model providers of the api harness: Anthropic's Messages API, or any OpenAI-compatible chat completions endpoint.
const (
	ProviderAnthropic = "anthropic"
	ProviderOpenAI    = "openai"
)

// Agent defaults (spec section 6.1).
const (
	DefaultPromptFile = "prompt.md"
	DefaultMaxTurns   = 10
)

// AgentRuntime is an agent action's runtime.agent (spec section 6.1).
type AgentRuntime struct {
	Harness string // claude-code or api
	Model   string
	// Prompt is the prompt template, slash-separated and relative to the package: runtime.agent.prompt resolved
	// against the action directory (prompt.md by default).
	Prompt string
	// Tools, MCP and Network are the agent's allowlists: the harness tools it may use, its MCP servers, and the
	// network destinations its sandbox may reach.
	Tools, MCP, Network []string
	// MaxTurns bounds the harness's turns (DefaultMaxTurns when the manifest says nothing); MaxCostUSD bounds the
	// cost of one delivery, zero meaning no bound of the action's own.
	MaxTurns   int
	MaxCostUSD float64
	// Provider, BaseURL and APIKeySecret configure the api harness: the provider (anthropic by default), its
	// endpoint (required for openai), and the name of the pipeline secret holding the API key. APIKeySecret also
	// gives the claude-code harness its ANTHROPIC_API_KEY.
	Provider     string
	BaseURL      string
	APIKeySecret string
}

// agentKeys are the keys of runtime.agent.
var agentKeys = []string{"harness", "model", "prompt", "tools", "mcp", "network", "maxTurns", "maxCostUsd",
	"provider", "baseUrl", "apiKeySecret"}

// agentRuntime reads runtime.agent; the prompt is kept as written, for finish to resolve against the package.
func agentRuntime(rt map[string]any) (*AgentRuntime, error) {
	raw, ok := rt[KindAgent].(map[string]any)
	if !ok {
		return nil, errors.New("runtime.agent: an agent action needs runtime.agent with its harness and model")
	}
	for _, k := range slices.Sorted(maps.Keys(raw)) {
		if !slices.Contains(agentKeys, k) {
			return nil, fmt.Errorf("runtime.agent.%s: unknown key", k)
		}
	}
	a := &AgentRuntime{Prompt: DefaultPromptFile, MaxTurns: DefaultMaxTurns}
	var err error
	strs := []struct {
		key      string
		dst      *string
		required bool
		allowed  []string
	}{
		{"harness", &a.Harness, true, []string{HarnessClaudeCode, HarnessAPI}},
		{"model", &a.Model, true, nil},
		{"prompt", &a.Prompt, false, nil},
		{"provider", &a.Provider, false, []string{ProviderAnthropic, ProviderOpenAI}},
		{"baseUrl", &a.BaseURL, false, nil},
		{"apiKeySecret", &a.APIKeySecret, false, nil},
	}
	for _, s := range strs {
		v, set := raw[s.key]
		if !set {
			if s.required {
				return nil, fmt.Errorf("runtime.agent.%s: is required", s.key)
			}
			continue
		}
		str, ok := v.(string)
		if !ok || str == "" {
			return nil, fmt.Errorf("runtime.agent.%s: must be a non-empty string", s.key)
		}
		if s.allowed != nil && !slices.Contains(s.allowed, str) {
			return nil, fmt.Errorf("runtime.agent.%s: must be one of %s", s.key, strings.Join(s.allowed, ", "))
		}
		*s.dst = str
	}
	for key, dst := range map[string]*[]string{"tools": &a.Tools, "mcp": &a.MCP, "network": &a.Network} {
		if *dst, err = stringList("runtime.agent."+key, raw[key]); err != nil {
			return nil, err
		}
	}
	if v, set := raw["maxTurns"]; set {
		n, ok := v.(int64)
		if !ok || n < 1 || n > math.MaxInt32 {
			return nil, errors.New("runtime.agent.maxTurns: must be a positive integer")
		}
		a.MaxTurns = int(n)
	}
	if v, set := raw["maxCostUsd"]; set {
		switch n := v.(type) {
		case int64:
			a.MaxCostUSD = float64(n)
		case float64:
			a.MaxCostUSD = n
		default:
			a.MaxCostUSD = math.NaN()
		}
		if !(a.MaxCostUSD > 0) || math.IsInf(a.MaxCostUSD, 0) { // NaN fails the comparison
			return nil, errors.New("runtime.agent.maxCostUsd: must be a positive number")
		}
	}
	switch {
	case a.Harness == HarnessAPI && a.Provider == "":
		a.Provider = ProviderAnthropic
	case a.Harness != HarnessAPI && (a.Provider != "" || a.BaseURL != ""):
		return nil, errors.New("runtime.agent: provider and baseUrl configure the api harness only")
	}
	if a.Provider == ProviderOpenAI && a.BaseURL == "" {
		return nil, errors.New("runtime.agent.baseUrl: the openai provider needs the endpoint's base URL")
	}
	if a.BaseURL != "" && !strings.HasPrefix(a.BaseURL, "https://") && !strings.HasPrefix(a.BaseURL, "http://") {
		return nil, errors.New("runtime.agent.baseUrl: must be an http or https URL")
	}
	return a, nil
}

// finishAgent resolves an agent action's prompt against the package and checks what the adapter needs: the prompt
// file and the output schema it validates every answer against. What a later milestone brings (MCP servers, tools
// for the api harness) is reported as unavailable.
func finishAgent(pkg *Package, m *Action) error {
	a := m.Agent
	p := path.Clean(path.Join(m.Dir, a.Prompt))
	if path.IsAbs(a.Prompt) || p == ".." || strings.HasPrefix(p, "../") {
		return fmt.Errorf("runtime.agent.prompt: %s leaves the package", a.Prompt)
	}
	if _, ok := pkg.Files[p]; !ok {
		return fmt.Errorf("runtime.agent.prompt: %s is not in the package", p)
	}
	a.Prompt = p
	if m.Output == nil {
		return errors.New("output: an agent action declares its output schema: every answer is validated against it")
	}
	switch {
	case len(a.MCP) > 0:
		return fmt.Errorf("runtime.agent.mcp: MCP servers arrive in a later milestone: %w", ErrUnavailable)
	case a.Harness == HarnessAPI && len(a.Tools) > 0:
		return fmt.Errorf("runtime.agent.tools: api harness tools arrive later: %w", ErrUnavailable)
	}
	return nil
}
