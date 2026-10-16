import { ApiError, type Api, type ArtifactLocation } from './api';
import { demoFixtures } from './demo';
import type {
  DeadLetter,
  DeadLetterGroup,
  DeadLetterSelection,
  Delivery,
  Insight,
  Item,
  LineageNode,
  MetricSeries,
  Nodes,
  Pipeline,
  PipelineSnapshot,
  Review,
  Revision,
  Run,
  RunReport,
  Sample,
  StepLessons,
  Stream,
} from './schema';

// A dead letter as the FakeApi keeps it: the record, plus what the server groups by.
export interface FakeDeadLetter extends DeadLetter {
  pipeline: string;
  step: string;
  kind: 'dead' | 'unrouted';
  fingerprint: string;
  // The error with what varies taken out, as the server's DeadLetterGroup.error.
  pattern: string;
}

// An artifact's content: text for text and JSON, a data URL for images and PDFs.
export interface FakeArtifact {
  pipeline: string;
  owner: { item: string } | { delivery: string; attempt: number };
  name: string;
  content: string;
}

// What a FakeApi answers from; demo.ts holds the dataset of tests and ?demo.
export interface Fixtures {
  pipelines: Pipeline[];
  runs: Run[];
  inbox: Review[];
  samples: Sample[];
  nodes: Nodes;
  items: Item[];
  // Every delivery; listRunDeliveries and listItemDeliveries filter them.
  deliveries: Delivery[];
  deadLetters: FakeDeadLetter[];
  // By pipeline.
  streams: Record<string, Stream[]>;
  snapshots: PipelineSnapshot[];
  artifacts: FakeArtifact[];
  // Report figures by run id, over the empty report.
  reports: Record<string, Partial<RunReport>>;
  revisions: Revision[];
  insights: Insight[];
  // By pipeline.
  lessons: Record<string, StepLessons[]>;
  metricNames: string[];
  metrics: (name: string, groupBy: string | undefined, from: Date, to: Date) => MetricSeries[];
  // Review delivery ids whose decision answers 409 with this message.
  conflicts: Record<string, string>;
}

// For ?demo=slow and ?demo=error: every read waits, or fails, so the loading and error states can be seen.
export interface FakeOptions {
  delayMs?: number;
  failReads?: boolean;
}

export interface FakeCall {
  method: string;
  args: unknown[];
}

function report(run: Run, figures: Partial<RunReport> = {}): RunReport {
  return {
    pipeline: run.pipeline,
    run: run.id,
    kind: run.kind,
    state: run.state,
    startedAt: run.startedAt,
    settledAt: run.settledAt ?? null,
    durationSeconds: 0,
    costUsd: run.costUsd,
    items: run.items ?? {},
    criticalPath: { totalSeconds: 0, steps: null },
    steps: null,
    failures: null,
    comparison: null,
    eta: null,
    ...figures,
  };
}

function since(query: { since?: string; from?: string; to?: string }): { from: Date; to: Date } {
  const to = query.to === undefined ? new Date() : new Date(query.to);
  if (query.from !== undefined) return { from: new Date(query.from), to };
  const match = /^(\d+)([mhd])$/.exec(query.since ?? '1h');
  const unit = { m: 60_000, h: 3_600_000, d: 86_400_000 }[(match?.[2] ?? 'h') as 'm' | 'h' | 'd'];
  return { from: new Date(to.getTime() - Number(match?.[1] ?? 1) * unit), to };
}

// In-memory Api: reads answer from the fixtures; actions are recorded in calls, change the FakeApi's own copy of the
// fixtures (a replayed dead letter, a paused step, a decided review, an acknowledged insight), and resolve.
export class FakeApi implements Api {
  readonly calls: FakeCall[] = [];
  private readonly fixtures: Fixtures;

  constructor(
    fixtures: Fixtures = demoFixtures,
    private readonly options: FakeOptions = {},
  ) {
    const { metrics, ...data } = fixtures;
    this.fixtures = { ...structuredClone(data), metrics };
    if (options.delayMs !== undefined || options.failReads) this.slowDown();
  }

  // Wraps every read: waits delayMs, then fails when failReads.
  private slowDown(): void {
    const { delayMs = 0, failReads = false } = this.options;
    const self = this as unknown as Record<string, unknown>;
    for (const key of Object.keys(this)) {
      const read = self[key];
      if (typeof read !== 'function' || !/^(list|get)/.test(key)) continue;
      self[key] = (...args: unknown[]) =>
        new Promise((resolve, reject) =>
          setTimeout(() => {
            if (failReads) reject(new ApiError(503, 'unavailable', 'the demo server is unavailable'));
            else resolve((read as (...a: unknown[]) => unknown)(...args));
          }, delayMs),
        );
    }
  }

  private record<T>(method: string, args: unknown[], result: T): Promise<T> {
    this.calls.push({ method, args });
    return Promise.resolve(result);
  }

  private notFound(what: string): Promise<never> {
    return Promise.reject(new ApiError(404, 'notFound', `${what} not found`));
  }

  private found<T>(value: T | undefined, what: string): Promise<T> {
    return value === undefined ? this.notFound(what) : Promise.resolve(value);
  }

  private pipeline(name: string): Pipeline | undefined {
    return this.fixtures.pipelines.find((p) => p.name === name);
  }

  private run(pipeline: string, id: string): Promise<Run> {
    return this.found(
      this.fixtures.runs.find((r) => r.pipeline === pipeline && r.id === id),
      `run ${pipeline}/${id}`,
    );
  }

  private newRun(pipeline: string, kind: Run['kind']): Run {
    return {
      id: `run_${this.calls.length + 1}`,
      pipeline,
      revision: 1,
      kind,
      triggerKey: '',
      triggerSource: 'manual',
      generation: 0,
      state: 'running',
      startedAt: new Date(0).toISOString(),
      costUsd: 0,
      vars: {},
      varSources: {},
    };
  }

  listPipelines: Api['listPipelines'] = () => Promise.resolve(this.fixtures.pipelines);
  getPipeline: Api['getPipeline'] = (name) => this.found(this.pipeline(name), `pipeline ${name}`);
  listRevisions: Api['listRevisions'] = (p) => Promise.resolve(this.fixtures.revisions.filter((r) => r.pipeline === p));
  getRevision: Api['getRevision'] = (p, revision) => this.notFound(`revision ${p}/${revision}`);
  listStreams: Api['listStreams'] = (p) => Promise.resolve(this.fixtures.streams[p] ?? []);
  listRuns: Api['listRuns'] = (pipeline, { state } = {}) =>
    Promise.resolve({
      items: this.fixtures.runs.filter((r) => r.pipeline === pipeline && (state === undefined || r.state === state)),
    });
  getRun: Api['getRun'] = (p, id) => this.run(p, id);
  getRunReport: Api['getRunReport'] = async (p, id) => report(await this.run(p, id), this.fixtures.reports[id]);
  listRunDeliveries: Api['listRunDeliveries'] = (p, run, { step, state } = {}) =>
    Promise.resolve(this.deliveries((d) => d.runId === run, p, step, state));
  // Pages by position (the cursor is the next index); where is not evaluated in memory.
  listItems: Api['listItems'] = (p, { stream, run, cursor, limit } = {}) => {
    const all = this.fixtures.items.filter(
      (i) =>
        i.pipeline === p && (stream === undefined || i.stream === stream) && (run === undefined || i.runId === run),
    );
    const from = cursor === undefined ? 0 : Number(cursor);
    const to = limit === undefined ? all.length : from + limit;
    return Promise.resolve({ items: all.slice(from, to), ...(to < all.length ? { next: String(to) } : {}) });
  };
  getItem: Api['getItem'] = (p, id) =>
    this.found(
      this.fixtures.items.find((i) => i.pipeline === p && i.id === id),
      `item ${p}/${id}`,
    );
  getLineage: Api['getLineage'] = async (p, id, { depth = 10 } = {}) => {
    const start = await this.getItem(p, id);
    const items = this.fixtures.items;
    const node = ({ id, pipeline, runId, stream, step, key, parents, ts }: Item): LineageNode => ({
      id,
      pipeline,
      runId,
      stream,
      step,
      key,
      parents,
      ts,
    });
    // An item is named by pipeline and id: a copy's parents are in the producer's pipeline (spec section 4.12).
    const ref = (i: Item) => `${i.pipeline}/${i.id}`;
    const parentsIn = (i: Item) => i.parentPipeline ?? i.pipeline;
    let truncated = false;
    // Breadth first from the item, nearest first, depth levels at most.
    const walk = (next: (from: Item) => Item[]) => {
      const out: LineageNode[] = [];
      const seen = new Set([ref(start)]);
      let level = [start];
      for (let d = 0; level.length > 0; d++) {
        const following = level.flatMap(next).filter((i) => !seen.has(ref(i)));
        if (d === depth) {
          truncated ||= following.length > 0;
          break;
        }
        following.forEach((i) => seen.add(ref(i)));
        out.push(...following.map(node));
        level = following;
      }
      return out;
    };
    const up = walk((from) => items.filter((i) => i.pipeline === parentsIn(from) && from.parents.includes(i.id)));
    const down = walk((from) => items.filter((i) => parentsIn(i) === from.pipeline && i.parents.includes(from.id)));
    return { item: id, up, down, truncated };
  };
  listItemDeliveries: Api['listItemDeliveries'] = (p, id, { step, state } = {}) =>
    Promise.resolve(this.deliveries((d) => d.itemId === id, p, step, state));
  getDelivery: Api['getDelivery'] = (p, id) =>
    this.found(
      this.fixtures.deliveries.find((d) => d.pipeline === p && d.id === id),
      `delivery ${p}/${id}`,
    ).then((d) => ({ ...d, attempts: d.attempts ?? [] }));
  listDeadLetters: Api['listDeadLetters'] = (p, { state = 'open' } = {}) =>
    Promise.resolve(
      groupDeadLetters(
        this.fixtures.deadLetters.filter((d) => d.pipeline === p && (state === 'all' || d.state === state)),
      ),
    );
  listLessons: Api['listLessons'] = (p) => Promise.resolve(this.fixtures.lessons[p] ?? []);
  getReview: Api['getReview'] = (p, id) =>
    this.found(
      this.fixtures.inbox.find((r) => r.pipeline === p && r.deliveryId === id),
      `review ${p}/${id}`,
    );
  getSample: Api['getSample'] = (p, id) =>
    this.found(
      this.fixtures.samples.find((s) => s.pipeline === p && s.deliveryId === id),
      `sample ${p}/${id}`,
    );
  listInbox: Api['listInbox'] = ({ pipeline, inbox, step, state = 'open' } = {}) =>
    Promise.resolve({ items: this.fixtures.inbox.filter(matches({ pipeline, inbox, step, state })) });
  listSamples: Api['listSamples'] = ({ pipeline, inbox, step, state } = {}) =>
    Promise.resolve({ items: this.fixtures.samples.filter(matches({ pipeline, inbox, step, state })) });
  listInsights: Api['listInsights'] = ({ pipeline, state } = {}) =>
    Promise.resolve(
      this.fixtures.insights.filter(
        (i) => (pipeline === undefined || i.pipeline === pipeline) && (state === undefined || i.state === state),
      ),
    );
  getMetrics: Api['getMetrics'] = (query) => {
    const { from, to } = since(query);
    return Promise.resolve(this.fixtures.metrics(query.name, query.groupBy, from, to));
  };
  listMetricNames: Api['listMetricNames'] = () => Promise.resolve(this.fixtures.metricNames);
  listNodes: Api['listNodes'] = () => Promise.resolve(this.fixtures.nodes);
  artifactUrl: Api['artifactUrl'] = (location) => this.artifact(location)?.content ?? 'about:blank';
  readArtifact: Api['readArtifact'] = (location) =>
    this.found(this.artifact(location), `artifact ${location.name}`).then((a) => a.content);

  // A pipeline's deliveries of a step and state, without their attempts: those come with one delivery only.
  private deliveries(which: (d: Delivery) => boolean, pipeline: string, step?: string, state?: string): Delivery[] {
    return this.fixtures.deliveries
      .filter(
        (d) =>
          d.pipeline === pipeline &&
          which(d) &&
          (step === undefined || d.step === step) &&
          (state === undefined || d.state === state),
      )
      .map((d) => {
        const copy = { ...d };
        delete copy.attempts;
        return copy;
      });
  }

  private artifact({ pipeline, name, ...owner }: ArtifactLocation): FakeArtifact | undefined {
    return this.fixtures.artifacts.find(
      (a) => a.pipeline === pipeline && a.name === name && JSON.stringify(a.owner) === JSON.stringify(owner),
    );
  }

  // The open dead letters a selection names, as the server reads exactly one of ids, fingerprint and all.
  private selected(pipeline: string, selection: DeadLetterSelection): FakeDeadLetter[] {
    return this.fixtures.deadLetters.filter(
      (d) =>
        d.pipeline === pipeline &&
        d.state === 'open' &&
        (selection.all === true ||
          (selection.ids?.includes(d.id) ?? false) ||
          (selection.fingerprint !== undefined && d.fingerprint === selection.fingerprint)),
    );
  }

  private settle(pipeline: string, selection: DeadLetterSelection, state: 'replayed' | 'discarded') {
    const letters = this.selected(pipeline, selection);
    letters.forEach((d) => (d.state = state));
    const p = this.pipeline(pipeline);
    if (p) p.openDeadLetters = Math.max(0, p.openDeadLetters - letters.length);
    return { count: letters.length };
  }

  private setPaused({ pipeline, step }: { pipeline: string; step?: string }, paused: boolean) {
    const p = this.pipeline(pipeline);
    if (!p) return;
    // Resuming a pipeline enables it too; pausing a disabled one is refused before this.
    if (step === undefined) p.state = paused ? 'paused' : 'active';
    for (const s of p.steps) if (step === undefined || s.name === step) s.paused = paused;
  }

  // A refused action is recorded too, then rejects as the server would.
  private refuse(method: string, args: unknown[], status: number, code: string, message: string): Promise<never> {
    this.calls.push({ method, args });
    return Promise.reject(new ApiError(status, code, message));
  }

  private disabled(pipeline: string): boolean {
    return this.pipeline(pipeline)?.state === 'disabled';
  }

  // A disabled pipeline's triggers do not fire.
  triggerRun: Api['triggerRun'] = (...args) =>
    this.disabled(args[0])
      ? this.refuse('triggerRun', args, 409, 'conflict', `pipeline ${args[0]} is disabled: resume it first`)
      : this.record('triggerRun', args, this.newRun(args[0], 'trigger'));
  cancelRun: Api['cancelRun'] = (...args) => this.record('cancelRun', args, undefined);
  pause: Api['pause'] = (...args) => {
    if (args[0].step === undefined && this.disabled(args[0].pipeline)) {
      return this.refuse('pause', args, 409, 'conflict', `pipeline ${args[0].pipeline} is disabled: resume it first`);
    }
    this.setPaused(args[0], true);
    return this.record('pause', args, undefined);
  };
  disable: Api['disable'] = (...args) => {
    const p = this.pipeline(args[0]);
    if (p) p.state = 'disabled';
    return this.record('disable', args, undefined);
  };
  // Reactivates a revision: the one named, or the newest older than the active one. No new revision is created.
  rollback: Api['rollback'] = (...args) => {
    const [pipeline, { revision = 0 } = {}] = args;
    const p = this.pipeline(pipeline);
    if (!p) return this.refuse('rollback', args, 404, 'notFound', `pipeline ${pipeline} not found`);
    const revisions = this.fixtures.revisions.filter((r) => r.pipeline === pipeline);
    const active = revisions.find((r) => r.active);
    const target =
      revision === 0
        ? revisions.filter((r) => active === undefined || r.id < active.id).sort((a, b) => b.id - a.id)[0]
        : revisions.find((r) => r.id === revision);
    if (target === undefined) {
      return revision === 0
        ? this.refuse('rollback', args, 409, 'conflict', `pipeline ${pipeline} has no revision before r${p.revision}`)
        : this.refuse('rollback', args, 404, 'notFound', `revision ${pipeline}/${revision} not found`);
    }
    if (target.active) {
      return this.refuse('rollback', args, 409, 'conflict', `revision r${target.id} is already active`);
    }
    for (const r of revisions) r.active = r === target;
    p.revision = target.id;
    return this.record('rollback', args, { ...target });
  };
  resume: Api['resume'] = (...args) => {
    this.setPaused(args[0], false);
    return this.record('resume', args, undefined);
  };
  replay: Api['replay'] = (...args) => this.record('replay', args, this.newRun(args[0], 'replay'));
  replayDeadLetters: Api['replayDeadLetters'] = (...args) =>
    this.record('replayDeadLetters', args, this.settle(args[0], args[1], 'replayed'));
  discardDeadLetters: Api['discardDeadLetters'] = (...args) =>
    this.record('discardDeadLetters', args, this.settle(args[0], args[1], 'discarded'));
  // A decided review or sample leaves the inbox.
  decideReview: Api['decideReview'] = async (...args) => {
    const review = await this.getReview(args[0], args[1]);
    const conflict = this.fixtures.conflicts[args[1]];
    if (conflict !== undefined) {
      this.calls.push({ method: 'decideReview', args });
      throw new ApiError(409, 'conflict', conflict);
    }
    const decided = { ...review, ...args[2], state: 'decided' as const };
    review.state = 'decided';
    return this.record('decideReview', args, decided);
  };
  decideSample: Api['decideSample'] = async (...args) => {
    const sample = await this.getSample(args[0], args[1]);
    const decided = { ...sample, ...args[2], state: 'decided' as const };
    sample.state = 'decided';
    return this.record('decideSample', args, decided);
  };
  acknowledgeInsight: Api['acknowledgeInsight'] = (...args) =>
    this.setInsight('acknowledgeInsight', args, 'acknowledged');
  muteInsight: Api['muteInsight'] = (...args) => this.setInsight('muteInsight', args, 'muted');

  private setInsight(method: string, args: [string, string], state: Insight['state']): Promise<void> {
    const insight = this.fixtures.insights.find((i) => i.pipeline === args[0] && i.id === args[1]);
    if (insight) insight.state = state;
    return this.record(method, args, undefined);
  }
}

// The inbox filters of a review or sample list.
function matches(query: {
  pipeline?: string | undefined;
  inbox?: string | undefined;
  step?: string | undefined;
  state?: string | undefined;
}) {
  return (r: { pipeline: string; inbox: string; step: string; state: string }) =>
    (query.state === undefined || r.state === query.state) &&
    (query.pipeline === undefined || r.pipeline === query.pipeline) &&
    (query.inbox === undefined || r.inbox === query.inbox) &&
    (query.step === undefined || r.step === query.step);
}

// Dead letters grouped as the server does: by fingerprint, largest group first, then by step and fingerprint.
function groupDeadLetters(letters: FakeDeadLetter[]): DeadLetterGroup[] {
  const groups = new Map<string, DeadLetterGroup>();
  for (const {
    step,
    kind,
    fingerprint,
    pattern,
    id,
    runId,
    originDelivery,
    stream,
    state,
    error,
    createdAt,
  } of letters) {
    const group = groups.get(fingerprint) ?? {
      fingerprint,
      step,
      kind,
      error: pattern,
      count: 0,
      firstSeen: createdAt,
      lastSeen: createdAt,
      deadLetters: [],
    };
    group.count += 1;
    if (createdAt < group.firstSeen) group.firstSeen = createdAt;
    if (createdAt > group.lastSeen) group.lastSeen = createdAt;
    group.deadLetters.push({ id, runId, originDelivery, stream, state, error, createdAt });
    groups.set(fingerprint, group);
  }
  return [...groups.values()].sort(
    (a, b) => b.count - a.count || a.step.localeCompare(b.step) || a.fingerprint.localeCompare(b.fingerprint),
  );
}
