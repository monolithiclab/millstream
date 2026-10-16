import { describe, expect, it, vi } from 'vitest';
import { ApiError, liveUrls, type Api } from './api';
import { HttpApi } from './http';

function setup(response: () => Response = () => Response.json([])) {
  const fetch = vi.fn<(input: string, init?: RequestInit) => Promise<Response>>(() => Promise.resolve(response()));
  return { api: new HttpApi(fetch), fetch };
}

type Case = [string, (api: Api) => Promise<unknown>, string, string, unknown?];

const P = '/api/v1/pipelines/p';

const cases: Case[] = [
  ['listPipelines', (a) => a.listPipelines(), 'GET', '/api/v1/pipelines'],
  ['getPipeline', (a) => a.getPipeline('a b'), 'GET', '/api/v1/pipelines/a%20b'],
  ['listRevisions', (a) => a.listRevisions('p'), 'GET', `${P}/revisions`],
  ['getRevision', (a) => a.getRevision('p', 13), 'GET', `${P}/revisions/13`],
  ['listStreams', (a) => a.listStreams('p'), 'GET', `${P}/streams`],
  [
    'listRuns',
    (a) => a.listRuns('p', { state: 'running', limit: 20, cursor: 'c' }),
    'GET',
    `${P}/runs?state=running&limit=20&cursor=c`,
  ],
  ['getRun', (a) => a.getRun('p', 'r'), 'GET', `${P}/runs/r`],
  ['getRunReport', (a) => a.getRunReport('p', 'r'), 'GET', `${P}/runs/r/report`],
  [
    'listRunDeliveries',
    (a) => a.listRunDeliveries('p', 'r', { step: 's', state: 'dead' }),
    'GET',
    `${P}/runs/r/deliveries?step=s&state=dead`,
  ],
  [
    'listItems',
    (a) => a.listItems('p', { stream: 's', run: 'r', where: 'item.score >= 70' }),
    'GET',
    `${P}/items?stream=s&run=r&where=item.score+%3E%3D+70`,
  ],
  ['getItem', (a) => a.getItem('p', 'it_1'), 'GET', `${P}/items/it_1`],
  ['getLineage', (a) => a.getLineage('p', 'it_1', { depth: 3 }), 'GET', `${P}/items/it_1/lineage?depth=3`],
  ['listItemDeliveries', (a) => a.listItemDeliveries('p', 'it_1'), 'GET', `${P}/items/it_1/deliveries`],
  ['getDelivery', (a) => a.getDelivery('p', 'd'), 'GET', `${P}/deliveries/d`],
  ['listDeadLetters', (a) => a.listDeadLetters('p'), 'GET', `${P}/dead-letters`],
  ['listDeadLetters state', (a) => a.listDeadLetters('p', { state: 'all' }), 'GET', `${P}/dead-letters?state=all`],
  ['listLessons', (a) => a.listLessons('p'), 'GET', `${P}/lessons`],
  ['getReview', (a) => a.getReview('p', 'd'), 'GET', `${P}/reviews/d`],
  ['getSample', (a) => a.getSample('p', 'd'), 'GET', `${P}/samples/d`],
  ['listInbox', (a) => a.listInbox(), 'GET', '/api/v1/inbox'],
  [
    'listInbox query',
    (a) => a.listInbox({ pipeline: 'p', inbox: 'analysts', state: 'open' }),
    'GET',
    '/api/v1/inbox?pipeline=p&inbox=analysts&state=open',
  ],
  ['listSamples', (a) => a.listSamples({ step: 's' }), 'GET', '/api/v1/inbox/samples?step=s'],
  [
    'listInsights',
    (a) => a.listInsights({ pipeline: 'p', state: 'open' }),
    'GET',
    '/api/v1/insights?pipeline=p&state=open',
  ],
  [
    'getMetrics',
    (a) => a.getMetrics({ name: 'throughput', groupBy: 'step', since: '1h' }),
    'GET',
    '/api/v1/metrics?name=throughput&groupBy=step&since=1h',
  ],
  ['listMetricNames', (a) => a.listMetricNames({ pipeline: 'p' }), 'GET', '/api/v1/metrics/names?pipeline=p'],
  ['listNodes', (a) => a.listNodes(), 'GET', '/api/v1/nodes'],
  ['triggerRun', (a) => a.triggerRun('p'), 'POST', `${P}/runs`, {}],
  [
    'triggerRun request',
    (a) => a.triggerRun('p', { trigger: 't', payload: { a: 1 }, requestId: 'q' }),
    'POST',
    `${P}/runs`,
    { trigger: 't', payload: { a: 1 }, requestId: 'q' },
  ],
  ['cancelRun', (a) => a.cancelRun('p', 'r'), 'POST', `${P}/runs/r/cancel`, {}],
  ['pause pipeline', (a) => a.pause({ pipeline: 'p' }), 'POST', `${P}/pause`, {}],
  ['resume step', (a) => a.resume({ pipeline: 'p', step: 's' }), 'POST', `${P}/steps/s/resume`, {}],
  [
    'replay',
    (a) => a.replay('p', { step: 'score', since: '7d' }),
    'POST',
    `${P}/replay`,
    { step: 'score', since: '7d' },
  ],
  [
    'replayDeadLetters',
    (a) => a.replayDeadLetters('p', { fingerprint: 'f', migrate: true }),
    'POST',
    `${P}/dead-letters/replay`,
    { fingerprint: 'f', migrate: true },
  ],
  [
    'discardDeadLetters',
    (a) => a.discardDeadLetters('p', { ids: ['i'] }),
    'POST',
    `${P}/dead-letters/discard`,
    { ids: ['i'] },
  ],
  [
    'decideReview',
    (a) => a.decideReview('p', 'd', { decision: 'reject', reason: 'duplicate' }),
    'POST',
    `${P}/reviews/d/decision`,
    { decision: 'reject', reason: 'duplicate' },
  ],
  [
    'decideSample',
    (a) => a.decideSample('p', 'd', { decision: 'agree' }),
    'POST',
    `${P}/samples/d/decision`,
    { decision: 'agree' },
  ],
  ['acknowledgeInsight', (a) => a.acknowledgeInsight('p', 'in'), 'POST', `${P}/insights/in/acknowledge`, {}],
  ['muteInsight', (a) => a.muteInsight('p', 'in'), 'POST', `${P}/insights/in/mute`, {}],
];

describe('HttpApi', () => {
  it.each(cases)('%s', async (_name, call, method, url, body) => {
    const { api, fetch } = setup();
    await call(api);
    const [input, init] = fetch.mock.calls[0]!;
    expect(input).toBe(url);
    expect(init?.method).toBe(method);
    expect(init?.body === undefined ? undefined : JSON.parse(init.body as string)).toEqual(body);
    // Every POST is JSON, even without a body of its own.
    if (method === 'POST') expect(init?.headers).toMatchObject({ 'Content-Type': 'application/json' });
  });

  it('returns the decoded body, and nothing for 204', async () => {
    expect(await setup(() => Response.json([{ name: 'p' }])).api.listPipelines()).toEqual([{ name: 'p' }]);
    expect(await setup(() => new Response(null, { status: 204 })).api.cancelRun('p', 'r')).toBeUndefined();
  });

  it('turns the error body into an ApiError', async () => {
    const { api } = setup(() =>
      Response.json({ error: { code: 'conflict', message: 'already decided' } }, { status: 409 }),
    );
    await expect(api.decideReview('p', 'd', { decision: 'approve' })).rejects.toEqual(
      new ApiError(409, 'conflict', 'already decided'),
    );
  });

  it('keeps the status line when the error body is not JSON', async () => {
    const { api } = setup(() => new Response('boom', { status: 502, statusText: 'Bad Gateway' }));
    const error = await api.listNodes().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 502, code: 'http', message: 'Bad Gateway' });
  });

  it('locates artifacts and reads them as text', async () => {
    const { api, fetch } = setup(() => new Response('{"a": 1}'));
    expect(api.artifactUrl({ pipeline: 'p', item: 'it 1', name: 'photo.jpg' })).toBe(
      `${P}/items/it%201/artifacts/photo.jpg`,
    );
    expect(await api.readArtifact({ pipeline: 'p', delivery: 'dl_1', attempt: 2, name: 'transcript.json' })).toBe(
      '{"a": 1}',
    );
    expect(fetch).toHaveBeenCalledWith(`${P}/deliveries/dl_1/attempts/2/artifacts/transcript.json`, { method: 'GET' });
    const missing = setup(() =>
      Response.json({ error: { code: 'notFound', message: 'no such artifact' } }, { status: 404 }),
    );
    await expect(missing.api.readArtifact({ pipeline: 'p', item: 'i', name: 'x' })).rejects.toEqual(
      new ApiError(404, 'notFound', 'no such artifact'),
    );
  });

  it('uses the global fetch by default', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(Response.json([]));
    await new HttpApi().listNodes();
    expect(fetch).toHaveBeenCalledWith('/api/v1/nodes', expect.objectContaining({ method: 'GET' }));
  });
});

describe('liveUrls', () => {
  it('names the event streams of spec 12.1', () => {
    expect(liveUrls.pipeline('a/b')).toBe('/api/v1/live/pipelines/a%2Fb');
    expect(liveUrls.run('p', 'r')).toBe('/api/v1/live/runs/r?pipeline=p');
    expect(liveUrls.item('a b', 'i')).toBe('/api/v1/live/items/i?pipeline=a+b');
    expect(liveUrls.logs()).toBe('/api/v1/live/logs');
    expect(liveUrls.logs({ pipeline: 'p', run: 'r', step: 's', item: 'i', delivery: 'd' })).toBe(
      '/api/v1/live/logs?pipeline=p&run=r&step=s&item=i&delivery=d',
    );
  });
});
