import { describe, expect, it, vi } from 'vitest';
import { ApiError } from './api';
import { DEMO_NOW, demoFixtures, shiftTimes } from './demo';
import { FakeApi } from './fake';

describe('FakeApi', () => {
  it('answers reads from its fixtures', async () => {
    const api = new FakeApi();
    expect((await api.listPipelines()).map((p) => p.name)).toEqual(demoFixtures.pipelines.map((p) => p.name));
    expect((await api.getPipeline('listings-daily')).revision).toBe(14);
    expect((await api.listRuns('listings-daily')).items).toHaveLength(4);
    expect((await api.getRun('listings-daily', 'run_0600')).state).toBe('running');
    expect(await api.getRunReport('listings-daily', 'run_0600')).toMatchObject({ run: 'run_0600', settledAt: null });
    expect((await api.getRunReport('listings-daily', 'run_0600_1014')).comparison).toBeNull();
    expect((await api.listInbox()).items).toHaveLength(8);
    expect((await api.listInbox({ inbox: 'other' })).items).toHaveLength(0);
    expect((await api.listInbox({ state: 'decided' })).items).toHaveLength(0);
    expect((await api.getReview('listings-daily', 'dl_4b2e')).display).toMatch(/^T3 · 68 m² · Paris 19e\n/);
    expect((await api.listSamples({ state: 'open', inbox: 'analysts' })).items).toHaveLength(1);
    expect((await api.listSamples({ pipeline: 'other' })).items).toHaveLength(0);
    expect((await api.listNodes()).nodes).toHaveLength(1);
    await expect(api.getLineage('p', 'it_1')).rejects.toBeInstanceOf(ApiError);
    expect(await api.listRunDeliveries('listings-daily', 'run_0600', { step: 'score', state: 'dead' })).toHaveLength(2);
    expect((await api.listRevisions('listings-daily')).map((r) => r.id)).toEqual([14, 13]);
    expect((await api.listInsights({ pipeline: 'listings-daily', state: 'open' })).map((i) => i.kind)).toEqual([
      'slowStep',
      'failureCluster',
    ]);
    expect((await api.listLessons('listings-daily')).map((l) => l.step)).toEqual(['score', 'enrich']);
    expect(await api.listMetricNames()).toContain('delivery_latency_seconds');
    const series = await api.getMetrics({ name: 'delivery_latency_seconds', groupBy: 'step', since: '24h' });
    expect(series.map((s) => s.group)).toEqual(['enrich', 'score', 'dedupe']);
    const window = await api.getMetrics({ name: 'm', from: '2026-10-15T00:00:00Z', to: '2026-10-15T02:00:00Z' });
    expect(window[0]!.points[0]!.t).toBe('2026-10-15T00:00:00.000Z');
    expect((await api.getMetrics({ name: 'm', groupBy: 'reason' })).map((s) => s.group)).toEqual(['a', 'b']);
    for (const empty of [
      api.listRevisions('p'),
      api.listStreams('p'),
      api.listRunDeliveries('p', 'r'),
      api.listItemDeliveries('p', 'i'),
      api.listDeadLetters('p'),
      api.listInsights({ pipeline: 'p' }),
      api.listLessons('p'),
    ]) {
      expect(await empty).toEqual([]);
    }
    expect(await api.listItems('p')).toEqual({ items: [] });
  });

  it('rejects unknown ids with a 404 ApiError', async () => {
    const api = new FakeApi();
    for (const read of [
      api.getPipeline('nope'),
      api.getRevision('p', 1),
      api.getRun('listings-daily', 'nope'),
      api.getRun('photo-tagging', 'run_0600'),
      api.getItem('p', 'nope'),
      api.getDelivery('p', 'nope'),
      api.getReview('p', 'nope'),
      api.getSample('p', 'nope'),
    ]) {
      await expect(read).rejects.toBeInstanceOf(ApiError);
    }
  });

  it('records actions', async () => {
    const api = new FakeApi();
    expect(await api.triggerRun('p')).toMatchObject({ pipeline: 'p', kind: 'trigger' });
    await api.cancelRun('p', 'r');
    await api.pause({ pipeline: 'p' });
    await api.resume({ pipeline: 'p', step: 's' });
    expect(await api.replay('p', { step: 'score' })).toMatchObject({ pipeline: 'p', kind: 'replay' });
    expect(await api.replayDeadLetters('p', { ids: ['a', 'b'] })).toEqual({ count: 0 });
    expect(await api.discardDeadLetters('p', { fingerprint: 'f' })).toEqual({ count: 0 });
    expect(
      await api.decideReview('listings-daily', 'dl_4b2e', { decision: 'reject', reason: 'overpriced' }),
    ).toMatchObject({ state: 'decided', decision: 'reject', reason: 'overpriced' });
    await expect(api.decideSample('p', 'sm', { decision: 'disagree' })).rejects.toBeInstanceOf(ApiError);
    await api.acknowledgeInsight('p', 'in');
    await api.muteInsight('p', 'in');
    await expect(api.decideReview('listings-daily', 'dl_7c1d', { decision: 'approve' })).rejects.toMatchObject({
      status: 409,
      message: expect.stringContaining('already decided') as unknown,
    });
    expect(api.calls.map((c) => c.method)).toEqual([
      'triggerRun',
      'cancelRun',
      'pause',
      'resume',
      'replay',
      'replayDeadLetters',
      'discardDeadLetters',
      'decideReview',
      'acknowledgeInsight',
      'muteInsight',
      'decideReview',
    ]);
    expect((await api.listInbox()).items.map((r) => r.deliveryId)).not.toContain('dl_4b2e');
  });

  it('keeps insight states per instance', async () => {
    const api = new FakeApi();
    await api.acknowledgeInsight('listings-daily', 'in_slow_enrich');
    await api.muteInsight('listings-daily', 'in_fail_score');
    expect((await api.listInsights({ state: 'open' })).map((i) => i.id)).toEqual([]);
    expect((await new FakeApi().listInsights({ state: 'open' })).map((i) => i.id)).toHaveLength(2);
  });

  it('delays or fails every read on demand', async () => {
    vi.useFakeTimers();
    const slow = new FakeApi(demoFixtures, { delayMs: 500 });
    const pending = slow.listPipelines();
    await vi.advanceTimersByTimeAsync(500);
    expect(await pending).toHaveLength(demoFixtures.pipelines.length);
    const failing = new FakeApi(demoFixtures, { failReads: true }).listNodes();
    const caught = failing.catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(0);
    expect(await caught).toBeInstanceOf(ApiError);
  });

  it('shifts demo times to the present', () => {
    const offset = Date.parse('2026-10-20T06:31:00Z') - Date.parse(DEMO_NOW);
    const shifted = shiftTimes({ at: DEMO_NOW, list: ['2026-10-15T06:00:00Z', 'plain'], n: 1, none: null }, offset);
    expect(shifted).toEqual({
      at: '2026-10-20T06:31:00.000Z',
      list: ['2026-10-20T06:00:00.000Z', 'plain'],
      n: 1,
      none: null,
    });
  });

  it('decides a sample it holds', async () => {
    const { pipeline, deliveryId, runId, step, inbox, display, parkedAt, item } = demoFixtures.inbox[0]!;
    const sample = {
      pipeline,
      deliveryId,
      runId,
      step,
      inbox,
      display,
      item,
      state: 'open',
      createdAt: parkedAt,
    } as const;
    const api = new FakeApi({ ...demoFixtures, samples: [sample] });
    expect(await api.decideSample(pipeline, deliveryId, { decision: 'agree' })).toMatchObject({
      state: 'decided',
      decision: 'agree',
    });
  });

  it('serves the graph, item and dead letters screens', async () => {
    const api = new FakeApi();
    expect((await api.listStreams('listings-daily')).map((s) => s.name)).toContain('scored');
    expect(await api.listItems('listings-daily', { stream: 'score.dlq' })).toMatchObject({
      items: [{ id: 'it_dl_9d02' }, { id: 'it_dl_c41e' }],
    });
    expect(await api.getLineage('listings-daily', 'it_1a8d4f02', { depth: 1 })).toMatchObject({
      down: [{ id: 'it_51c0e8d4' }],
      truncated: true,
    });
    expect(await api.listItemDeliveries('listings-daily', 'it_51c0e8d4', { step: 'score', state: 'done' })).toEqual([
      expect.not.objectContaining({ attempts: expect.anything() }),
    ]);
    expect((await api.getDelivery('listings-daily', 'dl_4b2e')).attempts).toEqual([]);
    expect(await api.listInsights({ pipeline: 'listings-daily', state: 'open' })).toHaveLength(2);
    expect(await api.listDeadLetters('listings-daily', { state: 'all' })).toHaveLength(4);
    const text = await api.readArtifact({ pipeline: 'listings-daily', item: 'it_7f3a9c21', name: 'notes.json' });
    expect(JSON.parse(text)).toMatchObject({ visit: '2026-10-17' });
    expect(api.artifactUrl({ pipeline: 'listings-daily', item: 'it_7f3a9c21', name: 'nope' })).toBe('about:blank');
    await expect(api.readArtifact({ pipeline: 'p', delivery: 'd', attempt: 1, name: 'x' })).rejects.toBeInstanceOf(
      ApiError,
    );
  });

  it('holds an item for every delivery and every insight sample, so links between screens open', async () => {
    const api = new FakeApi();
    const { deliveries, insights } = demoFixtures;
    const ids = [...deliveries.map((d) => d.itemId), ...insights.flatMap((i) => i.evidence.samples as string[])];
    for (const id of new Set(ids)) expect((await api.getItem('listings-daily', id)).id).toBe(id);
    for (const review of demoFixtures.inbox) {
      expect(await api.getItem(review.pipeline, review.item.id)).toEqual(review.item);
    }
  });

  it('settles dead letters and pauses steps in its own copy of the fixtures', async () => {
    const api = new FakeApi();
    expect(await api.replayDeadLetters('listings-daily', { fingerprint: 'fp3c9a0d17' })).toEqual({ count: 2 });
    expect(await api.discardDeadLetters('listings-daily', { all: true })).toEqual({ count: 1 });
    expect(await api.listDeadLetters('listings-daily')).toEqual([]);
    expect((await api.getPipeline('listings-daily')).openDeadLetters).toBe(0);
    await api.pause({ pipeline: 'listings-daily', step: 'score' });
    expect((await api.getPipeline('listings-daily')).steps.find((s) => s.name === 'score')?.paused).toBe(true);
    await api.pause({ pipeline: 'listings-daily' });
    expect((await api.getPipeline('listings-daily')).state).toBe('paused');
    await api.resume({ pipeline: 'nope' });
    expect(demoFixtures.deadLetters.filter((d) => d.state === 'open')).toHaveLength(3);
  });
});
