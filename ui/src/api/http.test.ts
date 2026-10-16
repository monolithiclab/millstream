import { describe, expect, it, vi } from 'vitest';
import { ApiError, liveUrls, type Api } from './api';
import { HttpApi } from './http';

function setup(response: () => Response = () => Response.json([])) {
  const fetch = vi.fn<(input: string, init?: RequestInit) => Promise<Response>>(() => Promise.resolve(response()));
  return { api: new HttpApi(fetch), fetch };
}

type Case = [string, (api: Api) => Promise<unknown>, string, string, unknown?];

const metrics = { metric: 'throughput', groupBy: 'step', from: 'a', to: 'b' } as const;

const cases: Case[] = [
  ['listPipelines', (a) => a.listPipelines(), 'GET', '/api/v1/pipelines'],
  ['getPipeline', (a) => a.getPipeline('a b'), 'GET', '/api/v1/pipelines/a%20b'],
  ['listRevisions', (a) => a.listRevisions('p'), 'GET', '/api/v1/pipelines/p/revisions'],
  ['listRuns', (a) => a.listRuns('p', { limit: 20, cursor: 'c' }), 'GET', '/api/v1/pipelines/p/runs?limit=20&cursor=c'],
  ['getRun', (a) => a.getRun('r'), 'GET', '/api/v1/runs/r'],
  ['getRunReport', (a) => a.getRunReport('r'), 'GET', '/api/v1/runs/r/report'],
  ['listStreams', (a) => a.listStreams('p'), 'GET', '/api/v1/pipelines/p/streams'],
  [
    'listItems',
    (a) => a.listItems('p', { stream: 's', filter: 'score >= 70' }),
    'GET',
    '/api/v1/pipelines/p/items?stream=s&filter=score+%3E%3D+70',
  ],
  ['getItem', (a) => a.getItem('it_1'), 'GET', '/api/v1/items/it_1'],
  ['getLineage', (a) => a.getLineage('it_1'), 'GET', '/api/v1/items/it_1/lineage'],
  ['listDeliveries', (a) => a.listDeliveries('it_1'), 'GET', '/api/v1/items/it_1/deliveries'],
  ['listDeadLetters', (a) => a.listDeadLetters('p'), 'GET', '/api/v1/pipelines/p/dead-letters'],
  ['getMetrics', (a) => a.getMetrics(metrics), 'GET', '/api/v1/metrics?metric=throughput&groupBy=step&from=a&to=b'],
  ['listInbox', (a) => a.listInbox(), 'GET', '/api/v1/inbox'],
  ['listInbox inbox', (a) => a.listInbox('analysts'), 'GET', '/api/v1/inbox?inbox=analysts'],
  ['listSamples', (a) => a.listSamples(), 'GET', '/api/v1/inbox/samples'],
  ['listInsights', (a) => a.listInsights('p'), 'GET', '/api/v1/insights?pipeline=p'],
  ['listLessons', (a) => a.listLessons('p'), 'GET', '/api/v1/pipelines/p/lessons'],
  ['listNodes', (a) => a.listNodes(), 'GET', '/api/v1/nodes'],
  ['triggerRun', (a) => a.triggerRun('p'), 'POST', '/api/v1/pipelines/p/runs', {}],
  ['cancelRun', (a) => a.cancelRun('r'), 'POST', '/api/v1/runs/r/cancel', {}],
  ['pause pipeline', (a) => a.pause({ pipeline: 'p' }), 'POST', '/api/v1/pipelines/p/pause', {}],
  ['resume step', (a) => a.resume({ pipeline: 'p', step: 's' }), 'POST', '/api/v1/pipelines/p/steps/s/resume', {}],
  ['rollback', (a) => a.rollback('p', 'r13'), 'POST', '/api/v1/pipelines/p/rollback', { revision: 'r13' }],
  ['replay', (a) => a.replay('it_1'), 'POST', '/api/v1/items/it_1/replay', {}],
  ['replay step', (a) => a.replay('it_1', 'score'), 'POST', '/api/v1/items/it_1/replay', { step: 'score' }],
  [
    'replayDeadLetters',
    (a) => a.replayDeadLetters('p', { fingerprint: 'f' }),
    'POST',
    '/api/v1/pipelines/p/dead-letters/replay',
    { fingerprint: 'f' },
  ],
  [
    'discardDeadLetters',
    (a) => a.discardDeadLetters('p', { itemIds: ['i'] }),
    'POST',
    '/api/v1/pipelines/p/dead-letters/discard',
    { itemIds: ['i'] },
  ],
  [
    'decideReview',
    (a) => a.decideReview('rv', { decision: 'reject', reason: 'duplicate' }),
    'POST',
    '/api/v1/inbox/rv/decision',
    { decision: 'reject', reason: 'duplicate' },
  ],
  [
    'decideSample',
    (a) => a.decideSample('sm', { agree: true }),
    'POST',
    '/api/v1/inbox/samples/sm/decision',
    { agree: true },
  ],
  ['acknowledgeInsight', (a) => a.acknowledgeInsight('in'), 'POST', '/api/v1/insights/in/acknowledge', {}],
  ['muteInsight', (a) => a.muteInsight('in'), 'POST', '/api/v1/insights/in/mute', {}],
  ['muteInsight until', (a) => a.muteInsight('in', 'x'), 'POST', '/api/v1/insights/in/mute', { until: 'x' }],
];

describe('HttpApi', () => {
  it.each(cases)('%s', async (_name, call, method, url, body) => {
    const { api, fetch } = setup();
    await call(api);
    const [input, init] = fetch.mock.calls[0]!;
    expect(input).toBe(url);
    expect(init?.method).toBe(method);
    expect(init?.body === undefined ? undefined : JSON.parse(init.body as string)).toEqual(body);
  });

  it('returns the decoded body, and nothing for 204', async () => {
    expect(await setup(() => Response.json([{ name: 'p' }])).api.listPipelines()).toEqual([{ name: 'p' }]);
    expect(await setup(() => new Response(null, { status: 204 })).api.cancelRun('r')).toBeUndefined();
  });

  it('turns the error body into an ApiError', async () => {
    const { api } = setup(() =>
      Response.json({ error: { code: 'conflict', message: 'already decided' } }, { status: 409 }),
    );
    await expect(api.decideReview('rv', { decision: 'approve' })).rejects.toEqual(
      new ApiError(409, 'conflict', 'already decided'),
    );
  });

  it('keeps the status line when the error body is not JSON', async () => {
    const { api } = setup(() => new Response('boom', { status: 502, statusText: 'Bad Gateway' }));
    const error = await api.listNodes().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 502, code: 'http', message: 'Bad Gateway' });
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
    expect(liveUrls.run('r')).toBe('/api/v1/live/runs/r');
    expect(liveUrls.item('i')).toBe('/api/v1/live/items/i');
    expect(liveUrls.logs()).toBe('/api/v1/live/logs');
    expect(liveUrls.logs({ run: 'r', step: 's' })).toBe('/api/v1/live/logs?run=r&step=s');
  });
});
