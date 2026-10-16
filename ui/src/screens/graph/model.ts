import dagre from '@dagrejs/dagre';
import type {
  DeadLetterGroup,
  Import,
  Insight,
  Pipeline,
  PipelineSnapshot,
  Step,
  Stream,
  Trigger,
} from '../../api/schema';

export type StepKind = 'action' | 'filter' | 'review' | 'dedupe';
export type StepState = 'active' | 'paused' | 'failing' | 'slow' | 'awaitingReview';
export type Figures = PipelineSnapshot['steps'][number];

export interface StepModel {
  id: string;
  name: string;
  step: Step;
  kind: StepKind;
  // The live snapshot's figures; null until the first snapshot arrives.
  figures: Figures | null;
  state: StepState;
  openDeadLetters: number;
  // The step's most severe open insight.
  insight: Insight | undefined;
  // Filters: the share of input items that passed, and how many it held back, from the stream counts.
  passRate?: number | undefined;
  filtered?: number | undefined;
}

// Where items enter: a trigger, or a stream imported from another pipeline (spec section 4.12).
export type TriggerModel = { id: string; name: string } & ({ trigger: Trigger } | { imported: Import });

export interface ChannelModel {
  id: string;
  source: string;
  target: string;
  stream: string;
  // Items in the stream: the channel's width.
  volume: number;
  // Items per minute into the target step: the grains' density.
  pace: number;
  // Items waiting at the target step: grains packed against its head.
  queued: number;
}

export interface GraphModel {
  steps: StepModel[];
  triggers: TriggerModel[];
  channels: ChannelModel[];
}

export const stepId = (name: string) => `step:${name}`;
const triggerId = (name: string) => `trigger:${name}`;
const importId = (name: string) => `import:${name}`;

function kindOf(step: Step): StepKind {
  if (!step.builtin) return 'action';
  if (step.action === 'filter') return 'filter';
  if (step.action === 'review') return 'review';
  return 'dedupe';
}

const severityRank: Record<Insight['severity'], number> = { error: 3, warning: 2, info: 1 };

// The graph of a pipeline with its live figures: steps, triggers and imported streams as nodes, one channel per
// stream link from a producer to a consumer. Dead-letter streams and streams nobody reads draw nothing.
export function buildModel({
  pipeline,
  streams,
  snapshot,
  insights,
  deadLetters,
}: {
  pipeline: Pipeline;
  streams: Stream[];
  snapshot: PipelineSnapshot | null;
  insights: Insight[];
  deadLetters: DeadLetterGroup[];
}): GraphModel {
  const volume = new Map(streams.map((s) => [s.name, s.items]));
  const figures = new Map((snapshot?.steps ?? []).map((f) => [f.step, f]));
  const pipelinePaused = pipeline.state === 'paused' || snapshot?.paused === true;

  const steps = pipeline.steps.map((step): StepModel => {
    const fig = figures.get(step.name) ?? null;
    const open = deadLetters.filter((g) => g.step === step.name).reduce((n, g) => n + g.count, 0);
    const insight = insights
      .filter((i) => i.scope === step.name && i.state === 'open')
      .sort((a, b) => severityRank[b.severity] - severityRank[a.severity])[0];
    const kind = kindOf(step);
    let state: StepState = 'active';
    if (pipelinePaused || step.paused || fig?.paused) state = 'paused';
    else if (open > 0 || (fig?.errorRate ?? 0) >= 0.05) state = 'failing';
    else if (insight?.kind === 'slowStep') state = 'slow';
    else if (kind === 'review' && (fig?.awaitingReview ?? 0) > 0) state = 'awaitingReview';
    const model: StepModel = {
      id: stepId(step.name),
      name: step.name,
      step,
      kind,
      figures: fig,
      state,
      openDeadLetters: open,
      insight,
    };
    if (kind === 'filter') {
      const input = step.in.reduce((n, s) => n + (volume.get(s) ?? 0), 0);
      const output = step.out.reduce((n, s) => n + (volume.get(s) ?? 0), 0);
      if (input > 0) {
        model.passRate = Math.min(1, output / input);
        model.filtered = Math.max(0, input - output);
      }
    }
    return model;
  });

  const triggers: TriggerModel[] = [
    ...pipeline.triggers.map((trigger) => ({ id: triggerId(trigger.name), name: trigger.name, trigger })),
    ...(pipeline.imports ?? []).map((imported) => ({ id: importId(imported.name), name: imported.name, imported })),
  ];
  const producers = new Map<string, string[]>();
  const produce = (stream: string, id: string) => producers.set(stream, [...(producers.get(stream) ?? []), id]);
  for (const t of pipeline.triggers) produce(t.stream, triggerId(t.name));
  for (const i of pipeline.imports ?? []) produce(i.name, importId(i.name));
  for (const s of pipeline.steps) for (const out of s.out) produce(out, stepId(s.name));

  const channels: ChannelModel[] = [];
  for (const s of steps) {
    for (const stream of s.step.in) {
      for (const source of producers.get(stream) ?? []) {
        channels.push({
          id: `${source}>${stream}>${s.id}`,
          source,
          target: s.id,
          stream,
          volume: volume.get(stream) ?? 0,
          pace: s.figures?.throughputPerMin ?? 0,
          queued: s.figures?.queued ?? 0,
        });
      }
    }
  }
  return { steps, triggers, channels };
}

// Node boxes, in px: steps are cards, filters funnels with their name above, triggers small tags.
const nodeSize = {
  step: { width: 136, height: 158 },
  filter: { width: 112, height: 158 },
  trigger: { width: 120, height: 52 },
} as const;

export function sizeOf(kind: StepKind | 'trigger') {
  if (kind === 'trigger') return nodeSize.trigger;
  return kind === 'filter' ? nodeSize.filter : nodeSize.step;
}

export interface Layout {
  positions: Map<string, { x: number; y: number }>;
  // Step names in topological order: left to right, then top to bottom (the Tab order).
  order: string[];
  // Arrow-key moves from each step: Right and Left follow channels, Up and Down move within a column.
  moves: Map<string, Partial<Record<'ArrowRight' | 'ArrowLeft' | 'ArrowUp' | 'ArrowDown', string>>>;
}

// Lays the graph out left to right (dagre), and derives the keyboard order from the positions.
export function layout(model: GraphModel): Layout {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: 'LR', nodesep: 36, ranksep: 64, marginx: 24, marginy: 24 });
  g.setDefaultEdgeLabel(() => ({}));
  for (const t of model.triggers) g.setNode(t.id, { ...nodeSize.trigger });
  for (const s of model.steps) g.setNode(s.id, { ...sizeOf(s.kind) });
  for (const c of model.channels) g.setEdge(c.source, c.target);
  dagre.layout(g);

  const positions = new Map<string, { x: number; y: number }>();
  for (const id of g.nodes()) {
    const n = g.node(id);
    positions.set(id, { x: n.x - n.width / 2, y: n.y - n.height / 2 });
  }
  const center = (id: string) => positions.get(id) ?? { x: 0, y: 0 };
  const byPosition = (a: StepModel, b: StepModel) => center(a.id).x - center(b.id).x || center(a.id).y - center(b.id).y;
  const ordered = [...model.steps].sort(byPosition);
  const order = ordered.map((s) => s.name);

  const names = new Map(model.steps.map((s) => [s.id, s.name]));
  const moves: Layout['moves'] = new Map();
  for (const s of ordered) {
    const near = (ids: string[]) =>
      ids
        .filter((id) => names.has(id))
        .sort((a, b) => Math.abs(center(a).y - center(s.id).y) - Math.abs(center(b).y - center(s.id).y))[0];
    const column = ordered.filter((o) => Math.abs(center(o.id).x - center(s.id).x) < 1);
    const index = column.indexOf(s);
    const right = near(model.channels.filter((c) => c.source === s.id).map((c) => c.target));
    const left = near(model.channels.filter((c) => c.target === s.id).map((c) => c.source));
    const move: Partial<Record<'ArrowRight' | 'ArrowLeft' | 'ArrowUp' | 'ArrowDown', string>> = {};
    const set = (key: keyof typeof move, name: string | undefined) => {
      if (name !== undefined) move[key] = name;
    };
    set('ArrowRight', right === undefined ? undefined : names.get(right));
    set('ArrowLeft', left === undefined ? undefined : names.get(left));
    set('ArrowUp', column[index - 1]?.name);
    set('ArrowDown', column[index + 1]?.name);
    moves.set(s.name, move);
  }
  return { positions, order, moves };
}
