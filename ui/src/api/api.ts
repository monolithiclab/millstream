import type { operations } from './openapi';
import type {
  Count,
  DeadLetterGroup,
  DeadLetterSelection,
  Delivery,
  Insight,
  Item,
  ItemPage,
  Lineage,
  MetricSeries,
  Nodes,
  Pipeline,
  ReplayRequest,
  Review,
  ReviewDecision,
  ReviewPage,
  Revision,
  RollbackRequest,
  Run,
  RunPage,
  RunReport,
  Sample,
  SampleDecision,
  SamplePage,
  Session,
  StepLessons,
  Stream,
  TriggerRequest,
  TriggerToken,
} from './schema';

// The query parameters of an operation, as the OpenAPI document declares them.
type QueryOf<Op extends keyof operations> = NonNullable<operations[Op]['parameters']['query']>;

export type RunQuery = QueryOf<'listRuns'>;
export type ItemQuery = QueryOf<'listItems'>;
export type LineageQuery = QueryOf<'getLineage'>;
export type DeliveryQuery = QueryOf<'listItemDeliveries'>;
export type DeadLetterQuery = QueryOf<'listDeadLetters'>;
export type MetricsQuery = QueryOf<'getMetrics'>;
export type MetricNamesQuery = QueryOf<'listMetricNames'>;
export type InboxQuery = QueryOf<'listInbox'>;
export type SampleQuery = QueryOf<'listSamples'>;
export type InsightQuery = QueryOf<'listInsights'>;

// A step or a whole pipeline, for pause and resume.
export interface Target {
  pipeline: string;
  step?: string;
}

// An artifact of an item, or of one attempt of a delivery (an agent's transcript.json).
export type ArtifactLocation = { pipeline: string; name: string } & (
  { item: string } | { delivery: string; attempt: number }
);

export function artifactPath({ pipeline, name, ...owner }: ArtifactLocation): string {
  const base = `/pipelines/${enc(pipeline)}`;
  return 'item' in owner
    ? `${base}/items/${enc(owner.item)}/artifacts/${enc(name)}`
    : `${base}/deliveries/${enc(owner.delivery)}/attempts/${owner.attempt}/artifacts/${enc(name)}`;
}

// keepalive lets a request outlive the page (a decision sent as the tab closes); unlike sendBeacon, it keeps the JSON
// Content-Type the server requires.
export interface SendOptions {
  keepalive?: boolean;
}

// Everything the UI asks of the server, as internal/api/openapi.yaml documents it. Every record but a pipeline is
// scoped by its pipeline. HttpApi implements it over fetch, FakeApi in memory for tests and ?demo.
export interface Api {
  listPipelines(): Promise<Pipeline[]>;
  getPipeline(pipeline: string): Promise<Pipeline>;
  listRevisions(pipeline: string): Promise<Revision[]>;
  getRevision(pipeline: string, revision: number): Promise<Revision>;
  listStreams(pipeline: string): Promise<Stream[]>;
  listRuns(pipeline: string, query?: RunQuery): Promise<RunPage>;
  getRun(pipeline: string, run: string): Promise<Run>;
  getRunReport(pipeline: string, run: string): Promise<RunReport>;
  listRunDeliveries(pipeline: string, run: string, query?: DeliveryQuery): Promise<Delivery[]>;
  listItems(pipeline: string, query?: ItemQuery): Promise<ItemPage>;
  getItem(pipeline: string, item: string): Promise<Item>;
  getLineage(pipeline: string, item: string, query?: LineageQuery): Promise<Lineage>;
  listItemDeliveries(pipeline: string, item: string, query?: DeliveryQuery): Promise<Delivery[]>;
  getDelivery(pipeline: string, delivery: string): Promise<Delivery>;
  listDeadLetters(pipeline: string, query?: DeadLetterQuery): Promise<DeadLetterGroup[]>;
  listLessons(pipeline: string): Promise<StepLessons[]>;
  getReview(pipeline: string, delivery: string): Promise<Review>;
  getSample(pipeline: string, delivery: string): Promise<Sample>;
  listInbox(query?: InboxQuery): Promise<ReviewPage>;
  listSamples(query?: SampleQuery): Promise<SamplePage>;
  listInsights(query?: InsightQuery): Promise<Insight[]>;
  getMetrics(query: MetricsQuery): Promise<MetricSeries[]>;
  listMetricNames(query?: MetricNamesQuery): Promise<string[]>;
  listNodes(): Promise<Nodes>;
  // Where the browser loads an artifact from (an image, a PDF), and its content as text (JSON, text, a transcript).
  artifactUrl(location: ArtifactLocation): string;
  readArtifact(location: ArtifactLocation): Promise<string>;

  triggerRun(pipeline: string, request?: TriggerRequest): Promise<Run>;
  cancelRun(pipeline: string, run: string): Promise<void>;
  pause(target: Target): Promise<void>;
  resume(target: Target): Promise<void>;
  // Stops a pipeline's triggers and holds its deliveries until resume; pausing it meanwhile is a conflict.
  disable(pipeline: string): Promise<void>;
  // Reactivates an earlier revision (the one before the active one by default) and returns it.
  rollback(pipeline: string, request?: RollbackRequest): Promise<Revision>;
  replay(pipeline: string, request: ReplayRequest): Promise<Run>;
  replayDeadLetters(pipeline: string, selection: DeadLetterSelection): Promise<Count>;
  discardDeadLetters(pipeline: string, selection: DeadLetterSelection): Promise<Count>;
  decideReview(pipeline: string, delivery: string, decision: ReviewDecision, options?: SendOptions): Promise<Review>;
  decideSample(pipeline: string, delivery: string, decision: SampleDecision, options?: SendOptions): Promise<Sample>;
  acknowledgeInsight(pipeline: string, insight: string): Promise<void>;
  muteInsight(pipeline: string, insight: string): Promise<void>;
  // A remote node: drain stops leasing to it until it restarts; revoke refuses its id for good and ends its leases.
  drainNode(node: string): Promise<void>;
  revokeNode(node: string): Promise<void>;
  // Replaces a webhook trigger's token and returns the new one, shown once: the old one stops working at once.
  rotateTriggerToken(pipeline: string, trigger: string): Promise<TriggerToken>;

  // Whether the server requires its token, and who decides; unauthorized (401) until signed in on one that does.
  getSession(): Promise<Session>;
  // Signs in with the server's token: the answer sets the session cookie. A wrong token is unauthorized.
  signIn(token: string): Promise<Session>;
  signOut(): Promise<void>;
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

// The server-sent event streams of spec 12.1. Run and item ids are unique within a pipeline only.
export const liveUrls = {
  pipeline: (pipeline: string) => `/api/v1/live/pipelines/${enc(pipeline)}`,
  run: (pipeline: string, run: string) => withQuery(`/api/v1/live/runs/${enc(run)}`, { pipeline }),
  item: (pipeline: string, item: string) => withQuery(`/api/v1/live/items/${enc(item)}`, { pipeline }),
  logs: (scope: { pipeline?: string; run?: string; step?: string; item?: string; delivery?: string } = {}) =>
    withQuery('/api/v1/live/logs', scope),
};
