package pipeline

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Graph formats.
const (
	FormatMermaid = "mermaid"
	FormatDOT     = "dot"
)

// Node kinds of the step graph.
const (
	nodeStep    = "step"
	nodeTrigger = "trigger" // a trigger stream, written by its trigger
	nodeImport  = "import"  // a stream imported from another pipeline
	nodeStream  = "stream"  // an output stream no step reads
)

type graphNode struct {
	kind, name string
}

func (n graphNode) id() string { return n.kind + ":" + n.name }

type graphEdge struct {
	from, to graphNode
	stream   string
}

// Graph renders p's step graph as Mermaid (flowchart LR) or DOT. Steps are nodes and streams are labelled edges
// from each producer to each consumer. Streams with no producing step (trigger and imported streams) and output
// streams nobody reads appear as nodes of their own, so no stream is lost. A dead-letter stream is drawn only when
// a step reads it. The output is deterministic: nodes and edges are sorted.
func Graph(p *Pipeline, format string) (string, error) {
	nodes, edges := buildGraph(p)
	switch format {
	case FormatMermaid:
		return mermaid(nodes, edges), nil
	case FormatDOT:
		return dot(p.Name, nodes, edges), nil
	default:
		return "", fmt.Errorf("unknown graph format %q: want %s or %s", format, FormatMermaid, FormatDOT)
	}
}

// streamIndex maps each stream to the steps writing it (outputs and routes) and the steps reading it. A step's
// dead-letter stream counts as written by it only when some step reads that stream. Step names are sorted.
func (p *Pipeline) streamIndex() (producers, consumers map[string][]string) {
	producers, consumers = map[string][]string{}, map[string][]string{}
	for _, name := range slices.Sorted(maps.Keys(p.Steps)) {
		s := p.Steps[name]
		for _, in := range s.In {
			consumers[in] = append(consumers[in], name)
		}
		for _, out := range s.Out.targets() {
			producers[out] = append(producers[out], name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(p.Steps)) {
		dlq := p.Steps[name].DLQ
		if _, read := consumers[dlq]; read && !slices.Contains(producers[dlq], name) {
			producers[dlq] = append(producers[dlq], name)
		}
	}
	return producers, consumers
}

// buildGraph returns the sorted nodes and edges of p.
func buildGraph(p *Pipeline) ([]graphNode, []graphEdge) {
	producers, consumers := p.streamIndex()

	nodeSet := map[graphNode]bool{}
	for name := range p.Steps {
		nodeSet[graphNode{nodeStep, name}] = true
	}
	edgeSet := map[graphEdge]bool{}
	for stream, readers := range consumers {
		writers := producers[stream]
		if len(writers) == 0 {
			src := graphNode{nodeStream, stream}
			switch {
			case strings.HasPrefix(stream, TriggerStreamPrefix):
				src.kind = nodeTrigger
			case p.Streams[stream].Import != nil:
				src.kind = nodeImport
			}
			nodeSet[src] = true
			for _, r := range readers {
				edgeSet[graphEdge{src, graphNode{nodeStep, r}, stream}] = true
			}
			continue
		}
		for _, w := range writers {
			for _, r := range readers {
				edgeSet[graphEdge{graphNode{nodeStep, w}, graphNode{nodeStep, r}, stream}] = true
			}
		}
	}
	for stream, writers := range producers {
		if _, read := consumers[stream]; read {
			continue
		}
		sink := graphNode{nodeStream, stream}
		nodeSet[sink] = true
		for _, w := range writers {
			edgeSet[graphEdge{graphNode{nodeStep, w}, sink, stream}] = true
		}
	}

	nodes := slices.SortedFunc(maps.Keys(nodeSet), func(a, b graphNode) int {
		return cmp.Compare(a.id(), b.id())
	})
	edges := slices.SortedFunc(maps.Keys(edgeSet), func(a, b graphEdge) int {
		return cmp.Or(cmp.Compare(a.from.id(), b.from.id()), cmp.Compare(a.to.id(), b.to.id()),
			cmp.Compare(a.stream, b.stream))
	})
	return nodes, edges
}

// targets returns every stream out may write to, routes included, without duplicates, in declaration order.
func (o Out) targets() []string {
	out := slices.Clone(o.Streams)
	for _, r := range o.Routes {
		if !slices.Contains(out, r.Out) {
			out = append(out, r.Out)
		}
	}
	return out
}

func mermaid(nodes []graphNode, edges []graphEdge) string {
	nodeIDs := make(map[graphNode]string, len(nodes))
	var b strings.Builder
	b.WriteString("flowchart LR\n")
	for i, n := range nodes {
		id := fmt.Sprintf("n%d", i)
		nodeIDs[n] = id
		label := mermaidText(n.name)
		switch n.kind {
		case nodeStep:
			fmt.Fprintf(&b, "  %s[\"%s\"]\n", id, label)
		case nodeTrigger, nodeImport:
			fmt.Fprintf(&b, "  %s([\"%s\"])\n", id, label)
		default:
			fmt.Fprintf(&b, "  %s[(\"%s\")]\n", id, label)
		}
	}
	for _, e := range edges {
		fmt.Fprintf(&b, "  %s -->|\"%s\"| %s\n", nodeIDs[e.from], mermaidText(e.stream), nodeIDs[e.to])
	}
	return b.String()
}

// mermaidText escapes a name for a quoted Mermaid label.
func mermaidText(s string) string { return mermaidEscaper.Replace(s) }

var (
	mermaidEscaper = strings.NewReplacer(`"`, "#quot;", "<", "#lt;", ">", "#gt;")
	dotEscaper     = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
)

func dot(name string, nodes []graphNode, edges []graphEdge) string {
	var b strings.Builder
	fmt.Fprintf(&b, "digraph %s {\n  rankdir=LR;\n", dotQuote(name))
	for _, n := range nodes {
		shape := "box"
		switch n.kind {
		case nodeTrigger, nodeImport:
			shape = "oval"
		case nodeStream:
			shape = "cylinder"
		}
		fmt.Fprintf(&b, "  %s [label=%s, shape=%s];\n", dotQuote(n.id()), dotQuote(n.name), shape)
	}
	for _, e := range edges {
		fmt.Fprintf(&b, "  %s -> %s [label=%s];\n", dotQuote(e.from.id()), dotQuote(e.to.id()), dotQuote(e.stream))
	}
	b.WriteString("}\n")
	return b.String()
}

// dotQuote returns s as a quoted DOT identifier.
func dotQuote(s string) string {
	return `"` + dotEscaper.Replace(s) + `"`
}
