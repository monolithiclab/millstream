package improve

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// volatile are the evidence keys a brief leaves out: times that change with every window, and ids the agent has no
// use for. A brief is the draft agent's prompt, so it holds what a run reproduces: recordings key on it.
var volatile = map[string]bool{"samples": true, "timeToDecisionP50Seconds": true, "timeToDecisionP95Seconds": true,
	"windowStart": true, "windowEnd": true}

// brief is what the draft agent reads (spec section 11.6): the insight and the pattern, its evidence without
// reviewer identities (which the API never holds in evidence), the edits, the rejections of the same idea with
// their reasons, and the files the proposal may change, as the base revision has them.
func brief(id idea, files api.RevisionFiles, examples []example, rejected []api.Proposal) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Pipeline %s\n\n", id.pipeline())
	writeInsight(&b, id.insight)
	if id.hint != nil {
		writeJSON(&b, "The pattern, and the proposal it calls for", id.hint)
	}
	if len(examples) > 0 {
		writeJSON(&b, "Edits reviewers made (the item as the review received it, the value before and after)",
			examples)
	}
	b.WriteString(rejections(rejected))
	b.WriteString("## Files\n")
	for _, p := range relevant(id, files) {
		content, ok := files.Files[p]
		if !ok {
			fmt.Fprintf(&b, "\n### %s\n\nThe file does not exist yet.\n", p)
			continue
		}
		fmt.Fprintf(&b, "\n### %s\n\n````\n%s````\n", p, ensureNewline(content))
	}
	return b.String()
}

// rejections is the section listing the proposals of the same idea people rejected, with their reasons.
func rejections(rejected []api.Proposal) string {
	if len(rejected) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("## Proposals of this idea people rejected\n\n")
	for _, p := range rejected {
		fmt.Fprintf(&b, "- %s, rejected because: %s\n  Its rationale was: %s\n", p.Kind, p.Reason, p.Rationale)
	}
	b.WriteString("\n")
	return b.String()
}

// relevant are the files a brief shows for an idea: the pipeline file, and for a lessons or prompt proposal the
// step's lessons file and its agent's prompt.
func relevant(id idea, files api.RevisionFiles) []string {
	out := []string{pipeline.FileName}
	if id.kind != string(store.ProposalLessons) && id.kind != string(store.ProposalPrompt) || id.step == "" {
		return out
	}
	lessons := "lessons/" + id.step + ".yaml"
	if p, err := pipeline.Parse([]byte(files.Files[pipeline.FileName])); err == nil {
		if st, ok := p.Steps[id.step]; ok {
			if st.Lessons != "" {
				lessons = st.Lessons
			}
			if dir, ok := strings.CutPrefix(st.Action, "./"); ok {
				if prompt := dir + "/" + pipeline.DefaultPromptFile; files.Files[prompt] != "" {
					out = append(out, prompt)
				}
			}
		}
	}
	return append(out, lessons)
}

// writeJSON writes a section holding v as indented JSON, its keys sorted.
func writeJSON(b *strings.Builder, title string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	fmt.Fprintf(b, "## %s\n\n```json\n%s\n```\n\n", title, data)
}

func ensureNewline(s string) string {
	if strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}
