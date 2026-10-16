import { describe, expect, it } from 'vitest';
import { ApiError } from './api';
import { demoFixtures, FakeApi } from './fake';

describe('FakeApi', () => {
  it('answers reads from its fixtures', async () => {
    const api = new FakeApi();
    expect((await api.listPipelines()).map((p) => p.name)).toEqual(demoFixtures.pipelines.map((p) => p.name));
    expect((await api.getPipeline('listings-daily')).revision).toBe('r14');
    expect((await api.listRuns('listings-daily')).total).toBe(1);
    expect((await api.getRunReport('run_0600')).run.id).toBe('run_0600');
    expect(await api.listInbox()).toHaveLength(1);
    expect(await api.listSamples()).toHaveLength(0);
    expect(await api.listNodes()).toHaveLength(1);
    expect(await api.getLineage('it_1')).toEqual({ item: 'it_1', parents: [], children: [] });
    for (const empty of [
      api.listRevisions('p'),
      api.listStreams('p'),
      api.listDeliveries('i'),
      api.listDeadLetters('p'),
      api.getMetrics({ metric: 'm', from: 'a', to: 'b' }),
      api.listInsights(),
      api.listLessons('p'),
    ]) {
      expect(await empty).toEqual([]);
    }
    expect(await api.listItems('p')).toEqual({ items: [], total: 0 });
  });

  it('rejects unknown ids with a 404 ApiError', async () => {
    const api = new FakeApi();
    for (const read of [api.getPipeline('nope'), api.getRun('nope'), api.getItem('nope')]) {
      await expect(read).rejects.toBeInstanceOf(ApiError);
    }
  });

  it('records actions', async () => {
    const api = new FakeApi();
    expect((await api.triggerRun('p')).pipeline).toBe('p');
    await api.cancelRun('r');
    await api.pause({ pipeline: 'p' });
    await api.resume({ pipeline: 'p', step: 's' });
    await api.rollback('p', 'r1');
    await api.replay('i');
    expect(await api.replayDeadLetters('p', { itemIds: ['a', 'b'] })).toEqual({ count: 2 });
    expect(await api.discardDeadLetters('p', { fingerprint: 'f' })).toEqual({ count: 0 });
    await api.decideReview('rv', { decision: 'approve' });
    await api.decideSample('sm', { agree: false, reason: 'x' });
    await api.acknowledgeInsight('in');
    await api.muteInsight('in');
    expect(api.calls.map((c) => c.method)).toEqual([
      'triggerRun',
      'cancelRun',
      'pause',
      'resume',
      'rollback',
      'replay',
      'replayDeadLetters',
      'discardDeadLetters',
      'decideReview',
      'decideSample',
      'acknowledgeInsight',
      'muteInsight',
    ]);
  });
});
