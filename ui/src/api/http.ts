import { ApiError, artifactPath, enc, withQuery, type Api, type Query, type SendOptions, type Target } from './api';

type Fetch = (input: string, init?: RequestInit) => Promise<Response>;

const pipelinePath = (pipeline: string) => `/pipelines/${enc(pipeline)}`;

function targetPath(target: Target): string {
  const base = pipelinePath(target.pipeline);
  return target.step === undefined ? base : `${base}/steps/${enc(target.step)}`;
}

// The API over fetch, following internal/api/openapi.yaml. Every POST sends a JSON body, {} when it has none.
export class HttpApi implements Api {
  constructor(
    private readonly fetch: Fetch = (input, init) => globalThis.fetch(input, init),
    private readonly base = '/api/v1',
  ) {}

  private async request<T>(method: string, path: string, body?: unknown, options: SendOptions = {}): Promise<T> {
    const init: RequestInit = { method, headers: { Accept: 'application/json' } };
    if (options.keepalive) init.keepalive = true;
    if (body !== undefined) {
      init.headers = { Accept: 'application/json', 'Content-Type': 'application/json' };
      init.body = JSON.stringify(body);
    }
    const response = await this.send(path, init);
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  // fetch, turning an error status into an ApiError from the server's {error: {code, message}} body.
  private async send(path: string, init: RequestInit): Promise<Response> {
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
    return response;
  }

  private get<T>(path: string, query?: Query) {
    return this.request<T>('GET', withQuery(path, query));
  }

  private post<T>(path: string, body: unknown = {}, options?: SendOptions) {
    return this.request<T>('POST', path, body, options);
  }

  listPipelines: Api['listPipelines'] = () => this.get('/pipelines');
  getPipeline: Api['getPipeline'] = (p) => this.get(pipelinePath(p));
  listRevisions: Api['listRevisions'] = (p) => this.get(`${pipelinePath(p)}/revisions`);
  getRevision: Api['getRevision'] = (p, revision) => this.get(`${pipelinePath(p)}/revisions/${revision}`);
  listStreams: Api['listStreams'] = (p) => this.get(`${pipelinePath(p)}/streams`);
  listRuns: Api['listRuns'] = (p, query = {}) => this.get(`${pipelinePath(p)}/runs`, query);
  getRun: Api['getRun'] = (p, run) => this.get(`${pipelinePath(p)}/runs/${enc(run)}`);
  getRunReport: Api['getRunReport'] = (p, run) => this.get(`${pipelinePath(p)}/runs/${enc(run)}/report`);
  listRunDeliveries: Api['listRunDeliveries'] = (p, run, query = {}) =>
    this.get(`${pipelinePath(p)}/runs/${enc(run)}/deliveries`, query);
  listItems: Api['listItems'] = (p, query = {}) => this.get(`${pipelinePath(p)}/items`, query);
  getItem: Api['getItem'] = (p, item) => this.get(`${pipelinePath(p)}/items/${enc(item)}`);
  getLineage: Api['getLineage'] = (p, item, query = {}) =>
    this.get(`${pipelinePath(p)}/items/${enc(item)}/lineage`, query);
  listItemDeliveries: Api['listItemDeliveries'] = (p, item, query = {}) =>
    this.get(`${pipelinePath(p)}/items/${enc(item)}/deliveries`, query);
  listItemAudits: Api['listItemAudits'] = (p, item) => this.get(`${pipelinePath(p)}/items/${enc(item)}/audits`);
  getDelivery: Api['getDelivery'] = (p, delivery) => this.get(`${pipelinePath(p)}/deliveries/${enc(delivery)}`);
  listDeadLetters: Api['listDeadLetters'] = (p, query = {}) => this.get(`${pipelinePath(p)}/dead-letters`, query);
  listLessons: Api['listLessons'] = (p) => this.get(`${pipelinePath(p)}/lessons`);
  getReview: Api['getReview'] = (p, delivery) => this.get(`${pipelinePath(p)}/reviews/${enc(delivery)}`);
  getSample: Api['getSample'] = (p, delivery) => this.get(`${pipelinePath(p)}/samples/${enc(delivery)}`);
  listInbox: Api['listInbox'] = (query = {}) => this.get('/inbox', query);
  listSamples: Api['listSamples'] = (query = {}) => this.get('/inbox/samples', query);
  listInsights: Api['listInsights'] = (query = {}) => this.get('/insights', query);
  listProposals: Api['listProposals'] = (query = {}) => this.get('/proposals', query);
  getProposal: Api['getProposal'] = (proposal) => this.get(`/proposals/${enc(proposal)}`);
  getMetrics: Api['getMetrics'] = (query) => this.get('/metrics', query);
  listMetricNames: Api['listMetricNames'] = (query = {}) => this.get('/metrics/names', query);
  listNodes: Api['listNodes'] = () => this.get('/nodes');
  artifactUrl: Api['artifactUrl'] = (location) => this.base + artifactPath(location);
  readArtifact: Api['readArtifact'] = async (location) =>
    (await this.send(artifactPath(location), { method: 'GET' })).text();

  triggerRun: Api['triggerRun'] = (p, request = {}) => this.post(`${pipelinePath(p)}/runs`, request);
  cancelRun: Api['cancelRun'] = (p, run) => this.post(`${pipelinePath(p)}/runs/${enc(run)}/cancel`);
  pause: Api['pause'] = (target) => this.post(`${targetPath(target)}/pause`);
  resume: Api['resume'] = (target) => this.post(`${targetPath(target)}/resume`);
  disable: Api['disable'] = (p) => this.post(`${pipelinePath(p)}/pause`, { triggers: true });
  rollback: Api['rollback'] = (p, request = {}) => this.post(`${pipelinePath(p)}/rollback`, request);
  replay: Api['replay'] = (p, request) => this.post(`${pipelinePath(p)}/replay`, request);
  replayDeadLetters: Api['replayDeadLetters'] = (p, selection) =>
    this.post(`${pipelinePath(p)}/dead-letters/replay`, selection);
  discardDeadLetters: Api['discardDeadLetters'] = (p, selection) =>
    this.post(`${pipelinePath(p)}/dead-letters/discard`, selection);
  decideReview: Api['decideReview'] = (p, delivery, decision, options) =>
    this.post(`${pipelinePath(p)}/reviews/${enc(delivery)}/decision`, decision, options);
  decideSample: Api['decideSample'] = (p, delivery, decision, options) =>
    this.post(`${pipelinePath(p)}/samples/${enc(delivery)}/decision`, decision, options);
  acknowledgeInsight: Api['acknowledgeInsight'] = (p, insight) =>
    this.post(`${pipelinePath(p)}/insights/${enc(insight)}/acknowledge`);
  muteInsight: Api['muteInsight'] = (p, insight) => this.post(`${pipelinePath(p)}/insights/${enc(insight)}/mute`);
  acceptProposal: Api['acceptProposal'] = (proposal, decision) =>
    this.post(`/proposals/${enc(proposal)}/accept`, decision);
  rejectProposal: Api['rejectProposal'] = (proposal, decision) =>
    this.post(`/proposals/${enc(proposal)}/reject`, decision);
  revalidateProposal: Api['revalidateProposal'] = (proposal) => this.post(`/proposals/${enc(proposal)}/revalidate`);
  drainNode: Api['drainNode'] = (node) => this.post(`/nodes/${enc(node)}/drain`);
  revokeNode: Api['revokeNode'] = (node) => this.post(`/nodes/${enc(node)}/revoke`);
  rotateTriggerToken: Api['rotateTriggerToken'] = (p, trigger) =>
    this.post(`${pipelinePath(p)}/triggers/${enc(trigger)}/token`);

  getSession: Api['getSession'] = () => this.get('/session');
  signIn: Api['signIn'] = (token) => this.post('/session', { token });
  signOut: Api['signOut'] = () => this.request('DELETE', '/session');
}
