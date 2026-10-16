package pipeline

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

// The packages of the CheckInstance tests: a producer whose crawl writes listings (url required, price optional) and
// whose keep filter passes them on as kept, and consumers built on the Check tests' actions.
const (
	producerYAML = `pipeline: src
triggers:
  go: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.go, out: listings, config: { city: Lyon } }
  keep: { action: filter, in: listings, out: kept, config: { expr: "true" } }
`
	// noteYAML reads the producer's stream with an action without an input schema: nothing to check.
	noteYAML = `pipeline: note
streams:
  raw: { pipeline: src, stream: kept }
steps:
  send: { action: ./actions/notify, in: raw }
`
	// scoreYAML's score action requires a price, which the producer's crawl does not.
	scoreYAML = `pipeline: rank
streams:
  raw: { pipeline: src, stream: listings }
steps:
  keep: { action: filter, in: raw, out: picked, config: { expr: "true" } }
  score: { action: ./actions/score, in: picked }
`
)

func instanceLines(got map[string][]Diagnostic) []string {
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(got)) {
		for _, d := range got[name] {
			lines = append(lines, name+": "+d.String())
		}
	}
	return lines
}

func TestCheckInstance(t *testing.T) {
	t.Parallel()
	price := "output of step crawl of pipeline src on stream picked against the input schema of ./actions/score at " +
		"/properties/price: the consumer requires property \"price\", which the producer declares but does not require"
	tests := []struct {
		name string
		yaml []string
		opts InstanceOptions
		want []string
	}{
		{name: "valid", yaml: []string{producerYAML, noteYAML}},
		{name: "producer outside the packages", yaml: []string{noteYAML}, want: []string{
			"note: warning streams.raw: pipeline src is not among the packages checked: the import of its stream kept " +
				"is not checked",
		}},
		{name: "producer missing from the instance", yaml: []string{noteYAML}, opts: InstanceOptions{Complete: true},
			want: []string{"note: error streams.raw: pipeline src does not exist"}},
		{name: "contract through a builtin", yaml: []string{producerYAML, scoreYAML},
			want: []string{"rank: error steps.score.in[0]: " + price}},
		{name: "producer revision breaks the contract", yaml: []string{producerYAML, scoreYAML},
			opts: InstanceOptions{Changed: []string{"src"}},
			want: []string{"src: error pipeline: breaks the contract of pipeline rank, step score: " + price}},
		{name: "forced", yaml: []string{producerYAML, scoreYAML},
			opts: InstanceOptions{Changed: []string{"src"}, Force: true},
			want: []string{"src: warning pipeline: breaks the contract of pipeline rank, step score: " + price}},
		{name: "consumer revision breaks the contract", yaml: []string{producerYAML, scoreYAML},
			opts: InstanceOptions{Changed: []string{"rank"}, Force: true},
			want: []string{"rank: error steps.score.in[0]: " + price}},
		{name: "no such stream", yaml: []string{producerYAML, strings.Replace(noteYAML, "kept", "gone", 1)},
			want: []string{"note: error streams.raw: pipeline src has no stream gone"}},
		{name: "producer revision removes the stream",
			yaml: []string{producerYAML, strings.Replace(noteYAML, "kept", "gone", 1)},
			opts: InstanceOptions{Changed: []string{"src"}},
			want: []string{"src: error pipeline: pipeline note imports stream gone as raw, and this revision no " +
				"longer writes it"}},
		{name: "removal forced", yaml: []string{producerYAML, strings.Replace(noteYAML, "kept", "gone", 1)},
			opts: InstanceOptions{Changed: []string{"src"}, Force: true},
			want: []string{"src: warning pipeline: pipeline note imports stream gone as raw, and this revision no " +
				"longer writes it"}},
		{name: "trigger, dead-letter and imported streams exist", yaml: []string{producerYAML, `pipeline: note
streams:
  a: { pipeline: src, stream: trigger.go }
  b: { pipeline: src, stream: crawl.dlq }
steps:
  send: { action: ./actions/notify, in: [a, b] }
`, `pipeline: relay
streams:
  c: { pipeline: note, stream: a }
steps:
  send: { action: ./actions/notify, in: c }
`}},
		{name: "chained imports carry the contract", yaml: []string{producerYAML, `pipeline: relay
streams:
  mid: { pipeline: src, stream: listings }
steps:
  pass: { action: filter, in: mid, out: out, config: { expr: "true" } }
`, `pipeline: rank
streams:
  picked: { pipeline: relay, stream: out }
steps:
  score: { action: ./actions/score, in: picked }
`}, want: []string{"rank: error steps.score.in[0]: " + price}},
		{name: "cycle", yaml: []string{
			"pipeline: a\nstreams:\n  in: { pipeline: b, stream: out }\nsteps:\n  pass: { action: filter, in: in, " +
				"out: out, config: { expr: \"true\" } }\n",
			"pipeline: b\nstreams:\n  in: { pipeline: a, stream: out }\nsteps:\n  pass: { action: filter, in: in, " +
				"out: out, config: { expr: \"true\" } }\n",
		}, opts: InstanceOptions{Force: true}, want: []string{
			"a: error streams.in: cycle between pipelines through imported streams: a -> b -> a",
		}},
		{name: "self import", yaml: []string{
			"pipeline: a\ntriggers: { go: { manual: {} } }\nstreams:\n  back: { pipeline: a, stream: out }\nsteps:\n" +
				"  pass: { action: filter, in: [trigger.go, back], out: out, config: { expr: \"true\" } }\n",
		}, want: []string{"a: error streams.back: cycle between pipelines through imported streams: a -> a"}},
		{name: "twice", yaml: []string{producerYAML, producerYAML},
			want: []string{"src: error pipeline: pipeline src is given twice"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkgs := make([]*Package, len(tt.yaml))
			for i, y := range tt.yaml {
				pkgs[i] = checkPackage(t, y)
			}
			got := instanceLines(CheckInstance(pkgs, Resolver{}, tt.opts))
			if !slices.Equal(got, tt.want) {
				t.Errorf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

func TestTriggersOptionalWithImports(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(noteYAML))
	if err != nil {
		t.Fatal(err)
	}
	if p.Triggers == nil || len(p.Triggers) != 0 || !slices.Equal(p.Imports(), []string{"raw"}) {
		t.Fatalf("triggers %v, imports %v", p.Triggers, p.Imports())
	}
	_, err = Parse([]byte("pipeline: p\nsteps: { s: { action: x, in: a } }\n"))
	if err == nil || !strings.Contains(err.Error(), "triggers: is required unless the pipeline imports a stream") {
		t.Fatalf("err = %v", err)
	}
}
