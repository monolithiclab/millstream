// Package improve is the improve pipeline (spec section 11.3): an ordinary package, embedded in the binary, that
// drafts proposals from the insights of the other pipelines and has the engine validate them. It runs only in
// development deployments, with improve.enabled and improve.harness set (Check).
//
// Its steps are exec actions served by the binary itself (millstream improve-action <step>, Serve), calling the
// public HTTP API with the proposals-only token the engine mints at start: gather-evidence reads the insight, its
// evidence and the files a proposal may change; draft is an agent action; validate turns the draft into a patch
// and creates the proposal, which the engine validates before it answers; propose records it; publish hands
// accepted proposals to the engine's publish action (POST /proposals/{id}/publish), which improve.publish names.
package improve

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// Name is the improve pipeline's name.
const Name = "improve"

// The secrets the engine gives the package: the API's URL and the proposals-only token. APIKeySecret holds the api
// harness's key.
const (
	SecretAPI    = "improveApi"
	SecretToken  = "improveToken" // #nosec G101 -- a secret's name, not a credential
	APIKeySecret = "anthropicKey" // #nosec G101 -- a secret's name, not a credential
)

// APIModel is the draft agent's model with the api harness; the claude-code harness uses the CLI's sonnet.
const APIModel = "claude-sonnet-5-5"

//go:embed all:package
var embedded embed.FS

// Errors of Check.
var (
	ErrDisabled  = errors.New("the improve pipeline is off: set improve.enabled: true in the environment file")
	ErrNoHarness = errors.New("the improve pipeline needs a harness for its draft agent: set improve.harness " +
		"(claude-code or api) in the environment file")
)

// Check refuses to run the improve pipeline where it may not: in production (pipeline.ErrProduction), without
// improve.enabled (ErrDisabled) or without improve.harness (ErrNoHarness). def is the command's default mode.
func Check(env *pipeline.Environment, def string) error {
	if err := env.RequireDevelopment(def, "the improve pipeline"); err != nil {
		return err
	}
	switch {
	case !env.Improve.Enabled:
		return ErrDisabled
	case env.Improve.Harness == "":
		return ErrNoHarness
	}
	return nil
}

// Source returns the package's files as committed, for millstream init pipeline --from improve.
func Source() (map[string][]byte, error) {
	sub, err := fs.Sub(embedded, "package")
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{}
	err = fs.WalkDir(sub, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(sub, p)
		files[p] = data
		return err
	})
	return files, err
}

// Files returns the package as the deployment runs it: its source with the environment file's settings filled in
// (the draft agent's harness, the budget, the publisher, lessons.maxTokens and improve.filter.maxFalseDrop).
func Files(env *pipeline.Environment) (map[string][]byte, error) {
	files, err := Source()
	if err != nil {
		return nil, err
	}
	im := env.Improve
	p := string(files[pipeline.FileName])
	vars := map[string]string{"publisher": strconv.Quote(im.Publish)}
	if env.Lessons.MaxTokens > 0 {
		vars["lessonsMaxTokens"] = strconv.Itoa(env.Lessons.MaxTokens)
	}
	if im.Filter.MaxFalseDrop > 0 {
		vars["maxFalseDrop"] = strconv.FormatFloat(im.Filter.MaxFalseDrop, 'f', -1, 64)
	}
	if im.MinSeverity != "" {
		vars["minSeverity"] = im.MinSeverity
	}
	for name, value := range vars {
		if p, err = setVar(p, name, value); err != nil {
			return nil, err
		}
	}
	if im.Budget.DayUSD > 0 {
		p += "\nbudget: { dayUsd: " + strconv.FormatFloat(im.Budget.DayUSD, 'f', -1, 64) + " }\n"
	}
	if im.Harness == pipeline.HarnessAPI {
		const draft = "actions/draft/action.yaml"
		m, err := replaceOnce(string(files[draft]), draft, "    harness: claude-code\n    model: sonnet\n",
			"    harness: api\n    model: "+APIModel+"\n    apiKeySecret: "+APIKeySecret+"\n")
		if err != nil {
			return nil, err
		}
		files[draft] = []byte(m)
		secrets := "secrets: [" + SecretAPI + ", " + SecretToken
		if p, err = replaceOnce(p, pipeline.FileName, secrets+"]", secrets+", "+APIKeySecret+"]"); err != nil {
			return nil, err
		}
	}
	files[pipeline.FileName] = []byte(p)
	return files, nil
}

// replaceOnce replaces from with to in src, the text of file, failing when src lacks it.
func replaceOnce(src, file, from, to string) (string, error) {
	if !strings.Contains(src, from) {
		return "", fmt.Errorf("improve package: %s lacks %q", file, strings.TrimSpace(from))
	}
	return strings.Replace(src, from, to, 1), nil
}

// setVar replaces the default of a variable declared on its own line, "  name: value # comment", keeping the
// comment.
func setVar(src, name, value string) (string, error) {
	prefix := "\n  " + name + ": "
	i := strings.Index(src, prefix)
	if i < 0 {
		return "", fmt.Errorf("improve package: variable %s is not declared on its own line", name)
	}
	start := i + len(prefix)
	end := start + strings.IndexByte(src[start:], '\n')
	if c := strings.Index(src[start:end], " #"); c >= 0 {
		end = start + c
	}
	return src[:start] + value + src[end:], nil
}

// Fork returns the package's source renamed name, for millstream init pipeline --from improve: its pipeline key
// and the steps' pipeline config, naming the pipeline itself.
func Fork(name string) (map[string][]byte, error) {
	files, err := Source()
	if err != nil {
		return nil, err
	}
	p, err := replaceOnce(string(files[pipeline.FileName]), pipeline.FileName, "\npipeline: improve\n",
		"\npipeline: "+name+"\n")
	if err != nil {
		return nil, err
	}
	files[pipeline.FileName] = []byte(strings.ReplaceAll(p, "      pipeline: improve #", "      pipeline: "+name+" #"))
	return files, nil
}
