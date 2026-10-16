import {
  ApiError,
  enc,
  withQuery,
  type Api,
  type Query,
  type DeadLetterSelection,
  type ItemQuery,
  type ListQuery,
  type Target,
} from './api';
import type { MetricsQuery, ReviewDecision, SampleDecision } from './schema';

type Fetch = (input: string, init?: RequestInit) => Promise<Response>;
function targetPath(target: Target): string {
  const base = `/pipelines/${enc(target.pipeline)}`;
  return target.step === undefined ? base : `${base}/steps/${enc(target.step)}`;
}

// The API over fetch. Paths follow M3 Task 2's endpoint list; when internal/api/openapi.yaml lands, its contract
// tests are the reference and any difference is fixed here (and the types in schema.ts are generated).
export class HttpApi implements Api {
  constructor(
    private readonly fetch: Fetch = (input, init) => globalThis.fetch(input, init),
    private readonly base = '/api/v1',
  ) {}

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const init: RequestInit = { method, headers: { Accept: 'application/json' } };
    if (body !== undefined) {
      init.headers = { Accept: 'application/json', 'Content-Type': 'application/json' };
      init.body = JSON.stringify(body);
    }
    const response = await this.fetch(this.base + path, init);
    if (!response.ok) {
      let code = 'http';
      let message = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = (await response.json()) as { error?: { code?: string; message?: string } };
        code = payload.error?.code ?? code;
        message = payload.error?.message ?? message;
      } catch {
        // Not a JSON error body: keep the status line.
      }
      throw new ApiError(response.status, code, message);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  private get<T>(path: string, query?: Query) {
    return this.request<T>('GET', withQuery(path, query));
  }

  private post<T>(path: string, body: unknown = {}) {
    return this.request<T>('POST', path, body);
  }

  listPipelines: Api['listPipelines'] = () => this.get('/pipelines');
  getPipeline: Api['getPipeline'] = (name: string) => this.get(`/pipelines/${enc(name)}`);
  listRevisions: Api['listRevisions'] = (pipeline: string) => this.get(`/pipelines/${enc(pipeline)}/revisions`);
  listRuns: Api['listRuns'] = (pipeline: string, query: ListQuery = {}) =>
    this.get(`/pipelines/${enc(pipeline)}/runs`, { ...query });
  getRun: Api['getRun'] = (id: string) => this.get(`/runs/${enc(id)}`);
  getRunReport: Api['getRunReport'] = (id: string) => this.get(`/runs/${enc(id)}/report`);
  listStreams: Api['listStreams'] = (pipeline: string) => this.get(`/pipelines/${enc(pipeline)}/streams`);
  listItems: Api['listItems'] = (pipeline: string, query: ItemQuery = {}) =>
    this.get(`/pipelines/${enc(pipeline)}/items`, { ...query });
  getItem: Api['getItem'] = (id: string) => this.get(`/items/${enc(id)}`);
  getLineage: Api['getLineage'] = (id: string) => this.get(`/items/${enc(id)}/lineage`);
  listDeliveries: Api['listDeliveries'] = (itemId: string) => this.get(`/items/${enc(itemId)}/deliveries`);
  listDeadLetters: Api['listDeadLetters'] = (pipeline: string) => this.get(`/pipelines/${enc(pipeline)}/dead-letters`);
  getMetrics: Api['getMetrics'] = (query: MetricsQuery) => this.get('/metrics', { ...query });
  listInbox: Api['listInbox'] = (inbox?: string) => this.get('/inbox', { inbox });
  listSamples: Api['listSamples'] = (inbox?: string) => this.get('/inbox/samples', { inbox });
  listInsights: Api['listInsights'] = (pipeline?: string) => this.get('/insights', { pipeline });
  listLessons: Api['listLessons'] = (pipeline: string) => this.get(`/pipelines/${enc(pipeline)}/lessons`);
  listNodes: Api['listNodes'] = () => this.get('/nodes');

  triggerRun: Api['triggerRun'] = (pipeline: string) => this.post(`/pipelines/${enc(pipeline)}/runs`);
  cancelRun: Api['cancelRun'] = (id: string) => this.post(`/runs/${enc(id)}/cancel`);
  pause: Api['pause'] = (target: Target) => this.post(`${targetPath(target)}/pause`);
  resume: Api['resume'] = (target: Target) => this.post(`${targetPath(target)}/resume`);
  rollback: Api['rollback'] = (pipeline: string, revision: string) =>
    this.post(`/pipelines/${enc(pipeline)}/rollback`, { revision });
  replay: Api['replay'] = (itemId: string, step?: string) =>
    this.post(`/items/${enc(itemId)}/replay`, step ? { step } : {});
  replayDeadLetters: Api['replayDeadLetters'] = (pipeline: string, selection: DeadLetterSelection) =>
    this.post(`/pipelines/${enc(pipeline)}/dead-letters/replay`, selection);
  discardDeadLetters: Api['discardDeadLetters'] = (pipeline: string, selection: DeadLetterSelection) =>
    this.post(`/pipelines/${enc(pipeline)}/dead-letters/discard`, selection);
  decideReview: Api['decideReview'] = (reviewId: string, decision: ReviewDecision) =>
    this.post(`/inbox/${enc(reviewId)}/decision`, decision);
  decideSample: Api['decideSample'] = (sampleId: string, decision: SampleDecision) =>
    this.post(`/inbox/samples/${enc(sampleId)}/decision`, decision);
  acknowledgeInsight: Api['acknowledgeInsight'] = (id: string) => this.post(`/insights/${enc(id)}/acknowledge`);
  muteInsight: Api['muteInsight'] = (id: string, until?: string) =>
    this.post(`/insights/${enc(id)}/mute`, until ? { until } : {});
}
