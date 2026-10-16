import { describe, expect, it } from 'vitest';
import { ApiError } from './api';
import { demoFixtures, FakeApi } from './fake';

describe('FakeApi', () => {
  it('answers reads from its fixtures', async () => {
    const api = new FakeApi();
    expect((await api.listPipelines()).map((p) => p.name)).toEqual(demoFixtures.pipelines.map((p) => p.name));
    expect((await api.getPipeline('listings-daily')).revision).toBe(14);
    expect((await api.listRuns('listings-daily')).items).toHaveLength(1);
    expect((await api.getRun('listings-daily', 'run_0600')).state).toBe('running');
    expect(await api.getRunReport('listings-daily', 'run_0600')).toMatchObject({ run: 'run_0600', settledAt: null });
    expect((await api.listInbox()).items).toHaveLength(1);
    expect((await api.listInbox({ inbox: 'other' })).items).toHaveLength(0);
    expect((await api.listInbox({ state: 'decided' })).items).toHaveLength(0);
    expect((await api.getReview('listings-daily', 'dl_4b2e')).display).toBe('T3 · 68 m² · Paris 19e');
    expect(await api.listSamples()).toEqual({ items: [] });
    expect((await api.listNodes()).nodes).toHaveLength(1);
    expect(await api.getLineage('p', 'it_1')).toEqual({ item: 'it_1', up: [], down: [], truncated: false });
    for (const empty of [
      api.listRevisions('p'),
      api.listStreams('p'),
      api.listRunDeliveries('p', 'r'),
      api.listItemDeliveries('p', 'i'),
      api.listDeadLetters('p'),
      api.getMetrics({ name: 'm' }),
      api.listMetricNames(),
      api.listInsights(),
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
    expect(await api.replayDeadLetters('p', { ids: ['a', 'b'] })).toEqual({ count: 2 });
    expect(await api.discardDeadLetters('p', { fingerprint: 'f' })).toEqual({ count: 0 });
    expect(
      await api.decideReview('listings-daily', 'dl_4b2e', { decision: 'reject', reason: 'overpriced' }),
    ).toMatchObject({ state: 'decided', decision: 'reject', reason: 'overpriced' });
    await expect(api.decideSample('p', 'sm', { decision: 'disagree' })).rejects.toBeInstanceOf(ApiError);
    await api.acknowledgeInsight('p', 'in');
    await api.muteInsight('p', 'in');
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
    ]);
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
});
