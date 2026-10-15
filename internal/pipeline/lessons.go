package pipeline

import (
	"fmt"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
)

// DefaultLessonsMaxTokens is lessons.maxTokens when the environment file does not set it (spec section 11.4).
const DefaultLessonsMaxTokens = 2000

// Lessons is a step's lessons file (spec section 11.4): rules and examples the agent adapter adds to the prompt.
type Lessons struct {
	Rules    []LessonRule    `json:"rules,omitempty"`
	Examples []LessonExample `json:"examples,omitempty"`
}

// LessonRule is one rule, with the proposal, review or insight it came from and when it was added.
type LessonRule struct {
	Text   string `json:"text"`
	Source string `json:"source,omitempty"`
	Added  string `json:"added,omitempty"`
}

// LessonExample is one worked example: an input excerpt, the output wanted for it, and why.
type LessonExample struct {
	Input  any    `json:"input"`
	Output any    `json:"output"`
	Why    string `json:"why,omitempty"`
	Source string `json:"source,omitempty"`
}

// Empty reports whether the lessons hold nothing.
func (l Lessons) Empty() bool { return len(l.Rules) == 0 && len(l.Examples) == 0 }

// LessonsTokens approximates the tokens of a lessons file: its characters divided by four, rounded up.
func LessonsTokens(data []byte) int { return (utf8.RuneCount(data) + 3) / 4 }

// StepLessons reads the lessons file of a step, nil when it has none. The file is the step's lessons key (Load sets
// lessons/<step>.yaml when the package has it): a path inside the package, which must exist.
func StepLessons(pkg *Package, s Step) (*Lessons, []byte, error) {
	if s.Lessons == "" {
		return nil, nil, nil
	}
	p, inside := packagePath("", s.Lessons)
	if !inside {
		return nil, nil, fmt.Errorf("lessons file %s leaves the package", s.Lessons)
	}
	data, ok := pkg.Files[p]
	if !ok {
		return nil, nil, fmt.Errorf("lessons file %s is not in the package", p)
	}
	l, err := ParseLessons(data)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", p, err)
	}
	return l, data, nil
}

// ParseLessons parses a lessons file strictly: rules[{text, source?, added?}] and examples[{input, output, why?,
// source?}], an unknown key being an error located by its key path.
func ParseLessons(data []byte) (*Lessons, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	l := &Lessons{}
	if len(doc.Content) == 0 {
		return l, nil
	}
	f, err := mapping(doc.Content[0], "")
	if err != nil {
		return nil, err
	}
	if err := f.allow("rules", "examples"); err != nil {
		return nil, err
	}
	if err := eachMapping(f, "rules", func(rf *fields) error {
		var r LessonRule
		if err := rf.allow("text", "source", "added"); err != nil {
			return err
		}
		for _, err := range []error{
			required(rf, "text", &r.Text, nonEmptyStr),
			optional(rf, "source", &r.Source, nonEmptyStr),
			optional(rf, "added", &r.Added, nonEmptyStr),
		} {
			if err != nil {
				return err
			}
		}
		l.Rules = append(l.Rules, r)
		return nil
	}); err != nil {
		return nil, err
	}
	err = eachMapping(f, "examples", func(ef *fields) error {
		var e LessonExample
		if err := ef.allow("input", "output", "why", "source"); err != nil {
			return err
		}
		for _, err := range []error{
			required(ef, "input", &e.Input, value),
			required(ef, "output", &e.Output, value),
			optional(ef, "why", &e.Why, nonEmptyStr),
			optional(ef, "source", &e.Source, nonEmptyStr),
		} {
			if err != nil {
				return err
			}
		}
		l.Examples = append(l.Examples, e)
		return nil
	})
	return l, err
}
