import type {
  DeadLetterGroup,
  Delivery,
  InboxEntry,
  Insight,
  Item,
  Lesson,
  Lineage,
  MetricSeries,
  MetricsQuery,
  NodeInfo,
  Page,
  Pipeline,
  ReviewDecision,
  Revision,
  Run,
  RunReport,
  SampleDecision,
  Stream,
} from './schema';

export interface ListQuery {
  limit?: number;
  cursor?: string;
}

export interface ItemQuery extends ListQuery {
  stream?: string;
  // A CEL expression, evaluated by the server.
  filter?: string;
}

// A step or a whole pipeline, for pause and resume.
export interface Target {
  pipeline: string;
  step?: string;
}

export interface DeadLetterSelection {
  fingerprint?: string;
  itemIds?: string[];
}

// Everything the UI asks of the server: the read endpoints and actions of M3 Task 2. HttpApi implements it over
// fetch, FakeApi in memory for tests and ?demo.
export interface Api {
  listPipelines(): Promise<Pipeline[]>;
  getPipeline(name: string): Promise<Pipeline>;
  listRevisions(pipeline: string): Promise<Revision[]>;
  listRuns(pipeline: string, query?: ListQuery): Promise<Page<Run>>;
  getRun(id: string): Promise<Run>;
  getRunReport(id: string): Promise<RunReport>;
  listStreams(pipeline: string): Promise<Stream[]>;
  listItems(pipeline: string, query?: ItemQuery): Promise<Page<Item>>;
  getItem(id: string): Promise<Item>;
  getLineage(id: string): Promise<Lineage>;
  listDeliveries(itemId: string): Promise<Delivery[]>;
  listDeadLetters(pipeline: string): Promise<DeadLetterGroup[]>;
  getMetrics(query: MetricsQuery): Promise<MetricSeries[]>;
  listInbox(inbox?: string): Promise<InboxEntry[]>;
  listSamples(inbox?: string): Promise<InboxEntry[]>;
  listInsights(pipeline?: string): Promise<Insight[]>;
  listLessons(pipeline: string): Promise<Lesson[]>;
  listNodes(): Promise<NodeInfo[]>;

  triggerRun(pipeline: string): Promise<Run>;
  cancelRun(id: string): Promise<void>;
  pause(target: Target): Promise<void>;
  resume(target: Target): Promise<void>;
  rollback(pipeline: string, revision: string): Promise<void>;
  replay(itemId: string, step?: string): Promise<void>;
  replayDeadLetters(pipeline: string, selection: DeadLetterSelection): Promise<{ count: number }>;
  discardDeadLetters(pipeline: string, selection: DeadLetterSelection): Promise<{ count: number }>;
  decideReview(reviewId: string, decision: ReviewDecision): Promise<void>;
  decideSample(sampleId: string, decision: SampleDecision): Promise<void>;
  acknowledgeInsight(id: string): Promise<void>;
  muteInsight(id: string, until?: string): Promise<void>;
}

// The server's error body is {error: {code, message}}.
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export const enc = encodeURIComponent;

export type Query = Record<string, string | number | undefined>;

// path?query, leaving out undefined values.
export function withQuery(path: string, query: Query = {}): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) if (value !== undefined) params.set(key, String(value));
  const qs = params.toString();
  return qs === '' ? path : `${path}?${qs}`;
}

// The server-sent event streams of spec 12.1.
export const liveUrls = {
  pipeline: (pipeline: string) => `/api/v1/live/pipelines/${enc(pipeline)}`,
  run: (id: string) => `/api/v1/live/runs/${enc(id)}`,
  item: (id: string) => `/api/v1/live/items/${enc(id)}`,
  logs: (scope: { run?: string; step?: string; item?: string } = {}) => withQuery('/api/v1/live/logs', scope),
};
