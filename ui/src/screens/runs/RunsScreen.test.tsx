import { act, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { FakeEventSource } from '../../live/fakeEventSource';
import { renderApp } from '../../test/render';
import { LIVE_REFRESH_MS } from '../../live/useCoalescedRefresh';

const RUN = '/p/listings-daily/runs/run_0600';

describe('RunsScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  it('lists runs newest first as run bars with state, duration and cost', async () => {
    renderApp({ route: '/p/listings-daily/runs' });
    const list = await screen.findByRole('list', { name: 'Runs, newest first' });
    const rows = within(list).getAllByRole('link');
    expect(rows).toHaveLength(4);
    expect(rows[0]).toHaveTextContent('Run 06:00 · Thu, Oct 15Running31 min · $9.84');
    expect(rows[0]).toHaveAttribute('href', RUN);
    expect(rows[1]).toHaveTextContent('Succeeded52 min · $13.90');
    expect(rows[2]).toHaveTextContent('Completed with errors');
    expect(rows[3]).toHaveTextContent('replay');
  });

  it('filters by state through the URL and says when nothing matches', async () => {
    const { user, location } = renderApp({ route: '/p/listings-daily/runs' });
    await screen.findByRole('list', { name: 'Runs, newest first' });
    await user.click(screen.getByRole('button', { name: 'Waiting for review' }));
    expect(location()).toBe('/p/listings-daily/runs?state=waitingReview');
    expect(await screen.findByText('No run in this state')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'All' }));
    expect(location()).toBe('/p/listings-daily/runs');
  });

  it('teaches when a pipeline has no run yet', async () => {
    renderApp({ route: '/p/rentals-weekly/runs' });
    expect(await screen.findByText('No runs yet')).toBeInTheDocument();
  });

  it('pages with the cursor and appends', async () => {
    const api = new FakeApi();
    const real = api.listRuns;
    api.listRuns = async (p, { cursor, ...query } = {}) => {
      const all = (await real(p, query)).items;
      // The second page repeats the first page's last run, as when a run moves while paging: shown once.
      return cursor === undefined ? { items: all.slice(0, 2), next: 'c2' } : { items: all.slice(1) };
    };
    const { user } = renderApp({ route: '/p/listings-daily/runs', api });
    const list = await screen.findByRole('list', { name: 'Runs, newest first' });
    expect(within(list).getAllByRole('link')).toHaveLength(2);
    await user.click(screen.getByRole('button', { name: 'Show more runs' }));
    expect(await screen.findByText(/replay/)).toBeInTheDocument();
    expect(within(list).getAllByRole('link')).toHaveLength(4);
    expect(screen.queryByRole('button', { name: 'Show more runs' })).not.toBeInTheDocument();
  });

  it('says when the next page fails', async () => {
    const api = new FakeApi();
    const real = api.listRuns;
    api.listRuns = async (p, { cursor, ...query } = {}) => {
      if (cursor !== undefined) throw new ApiError(500, 'internal', 'cursor expired');
      return { items: (await real(p, query)).items.slice(0, 1), next: 'c1' };
    };
    const { user } = renderApp({ route: '/p/listings-daily/runs', api });
    await user.click(await screen.findByRole('button', { name: 'Show more runs' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('cursor expired');
  });

  it('fires a trigger by hand and says the run started', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ route: '/p/listings-daily/runs', api });
    await screen.findByRole('list', { name: 'Runs, newest first' });
    await user.click(screen.getByRole('button', { name: 'Run daily now' }));
    expect(api.calls).toEqual([{ method: 'triggerRun', args: ['listings-daily', { trigger: 'daily' }] }]);
    expect(await screen.findByText(/^Run \d\d:\d\d · .+ started$/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Run daily now' })).toBeEnabled();
  });

  it('offers no button for a webhook, and says when a firing fails', async () => {
    const api = new FakeApi();
    const pipeline = await api.getPipeline('listings-daily');
    api.getPipeline = () =>
      Promise.resolve({
        ...pipeline,
        triggers: [...pipeline.triggers, { name: 'hook', kind: 'webhook', stream: 'trigger.hook' }],
      });
    api.triggerRun = () => Promise.reject(new ApiError(503, 'unavailable', 'no engine'));
    const { user } = renderApp({ route: '/p/listings-daily/runs', api });
    await user.click(await screen.findByRole('button', { name: 'Run daily now' }));
    expect(screen.queryByRole('button', { name: 'Run hook now' })).not.toBeInTheDocument();
    expect(await screen.findByRole('alert')).toHaveTextContent('daily could not be fired: no engine');
  });

  it('retries after an error', async () => {
    let fail = true;
    const api = new FakeApi();
    const real = api.listRuns;
    api.listRuns = (...args) => (fail ? Promise.reject(new ApiError(500, 'internal', 'store closed')) : real(...args));
    const { user } = renderApp({ route: '/p/listings-daily/runs', api });
    expect(await screen.findByRole('alert')).toHaveTextContent('store closed');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('list', { name: 'Runs, newest first' })).toBeInTheDocument();
  });
});

describe('RunScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  async function openRun(route = RUN, api = new FakeApi(), language: 'en' | 'fr' = 'en') {
    const view = renderApp({ route, api, language });
    await screen.findByRole('region', { name: language === 'fr' ? 'Chronologie' : 'Timeline' });
    return view;
  }

  it('names the run and shows its state, figures and neighbours', async () => {
    await openRun();
    expect(screen.getByRole('heading', { level: 1, name: 'Run 06:00 · Thu, Oct 15' })).toBeInTheDocument();
    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(
      screen.getByText(/Trigger daily · revision r14 · items: 2,034 · deliveries: 52 · \$9.84/),
    ).toBeInTheDocument();
    const neighbours = screen.getByRole('navigation', { name: 'Neighbouring runs' });
    expect(within(neighbours).getByRole('link')).toHaveTextContent('← Run 06:00 · Wed, Oct 14');
  });

  it('draws a Gantt row per step with a spoken summary, now and the ETA', async () => {
    await openRun();
    const timeline = screen.getByRole('region', { name: 'Timeline' });
    expect(within(timeline).getAllByRole('img')).toHaveLength(9);
    expect(
      within(timeline).getByRole('img', { name: /^fetch: started 06:00, drained 06:\d\d; working 100%/ }),
    ).toBeInTheDocument();
    expect(
      within(timeline).getByRole('img', { name: /^score: started 06:06, still running.*dead letters: 2$/ }),
    ).toBeInTheDocument();
    expect(
      within(timeline).getByRole('img', { name: /^analyst-review: .*waiting on people 100%/ }),
    ).toBeInTheDocument();
    expect(within(timeline).getByRole('img', { name: 'publish: not started' })).toBeInTheDocument();
    expect(within(timeline).getByRole('button', { name: 'publish' })).toBeDisabled();
    expect(timeline).toHaveTextContent('now 06:31');
    expect(timeline).toHaveTextContent('ETA 06:58');
  });

  it('expands a step into its density strip and lists the items of a bar', async () => {
    const { user } = await openRun();
    const step = screen.getByRole('button', { name: 'score' });
    await user.click(step);
    expect(step).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText(/score: deliveries over the run \(11\)/)).toBeInTheDocument();
    const bars = screen.getAllByRole('button', { name: /deliveries$/ });
    await user.click(bars[0]!);
    expect(bars[0]).toHaveAttribute('aria-pressed', 'true');
    const items = screen.getByRole('list', { name: 'Items in this bar' });
    expect(within(items).getAllByRole('link')[0]).toHaveAttribute(
      'href',
      expect.stringMatching(/^\/p\/listings-daily\/items\/it_/) as string,
    );
    await user.click(bars[0]!);
    expect(screen.queryByRole('list', { name: 'Items in this bar' })).not.toBeInTheDocument();
    await user.click(step);
    expect(step).toHaveAttribute('aria-expanded', 'false');
  });

  it('reports what the run waits on, its critical path, slowest steps, failures and cost', async () => {
    await openRun();
    const report = screen.getByRole('complementary', { name: 'Run report' });
    expect(within(report).getByRole('link', { name: /In the review inbox: 8/ })).toHaveAttribute(
      'href',
      '/inbox?pipeline=listings-daily',
    );
    expect(report).toHaveTextContent('score: 1 queued');
    expect(report).toHaveTextContent('ETA 06:58, from recent runs and current queues.');
    expect(report).toHaveTextContent('analyst-review+ 6 min, people');
    expect(report).toHaveTextContent('p95 4 min 50 s · 10 deliveries · $7.62');
    expect(within(report).getByRole('link', { name: /^×2score · schema/ })).toHaveAttribute(
      'href',
      '/p/listings-daily/dead-letters',
    );
    expect(report).toHaveTextContent('recovered on retry: 1');
    expect(report).toHaveTextContent('Cost against the last 10 runs');
    expect(report).toHaveTextContent('-30% against the median');
    const figures = within(report).getByRole('table', { name: 'This run against the median of recent runs' });
    expect(figures).toHaveTextContent('Duration31 min52 min');
  });

  it('refreshes from live events at most once a second', async () => {
    const api = new FakeApi();
    const getRun = vi.spyOn(api, 'getRun');
    await openRun(RUN, api);
    const source = FakeEventSource.latest();
    expect(source.url).toBe('/api/v1/live/runs/run_0600?pipeline=listings-daily');
    act(() => {
      source.open();
      source.emit('state', '{}', '1');
      source.emit('state', '{}', '2');
    });
    expect(getRun).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(LIVE_REFRESH_MS));
    expect(getRun).toHaveBeenCalledTimes(2);
  });

  it('cancels a running run after confirming, focused on keeping it', async () => {
    const api = new FakeApi();
    const { user } = await openRun(RUN, api);
    await user.click(screen.getByRole('button', { name: 'Cancel run…' }));
    const dialog = screen.getByRole('dialog', { name: 'Cancel Run 06:00 · Thu, Oct 15?' });
    expect(within(dialog).getByRole('button', { name: 'Keep running' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Keep running' }));
    expect(api.calls).toEqual([]);
    await user.click(screen.getByRole('button', { name: 'Cancel run…' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Cancel the run' }));
    expect(api.calls).toEqual([{ method: 'cancelRun', args: ['listings-daily', 'run_0600'] }]);
  });

  it('says when cancelling fails', async () => {
    const api = new FakeApi();
    api.cancelRun = () => Promise.reject(new ApiError(409, 'conflict', 'run already settled'));
    const { user } = await openRun(RUN, api);
    await user.click(screen.getByRole('button', { name: 'Cancel run…' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Cancel the run' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('The run could not be cancelled. run already settled');
  });

  it('shows a settled run without live updates, cancel or projection', async () => {
    await openRun('/p/listings-daily/runs/run_0600_1014');
    expect(screen.getByText('Succeeded')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel run…' })).not.toBeInTheDocument();
    expect(screen.queryByText('What the run is waiting on')).not.toBeInTheDocument();
    expect(screen.getByText('No failure in this run.')).toBeInTheDocument();
    expect(screen.getByText('No critical path yet.')).toBeInTheDocument();
    expect(screen.getByText(/This run cost \$13.90. There are no earlier runs/)).toBeInTheDocument();
    expect(FakeEventSource.instances.filter((s) => s.url.includes('/live/runs/'))).toEqual([]);
    expect(within(screen.getByRole('navigation', { name: 'Neighbouring runs' })).getAllByRole('link')).toHaveLength(2);
  });

  it('says when a run has neither queue nor ETA', async () => {
    const run = { ...demoFixtures.runs[0]!, id: 'run_new', startedAt: DEMO_NOW, items: {} };
    await openRun('/p/listings-daily/runs/run_new', new FakeApi({ ...demoFixtures, runs: [run] }));
    expect(screen.getByText('Nothing is queued or parked.')).toBeInTheDocument();
    expect(screen.getByText(/No ETA yet/)).toBeInTheDocument();
  });

  it('shows an unknown run as an error with retry', async () => {
    renderApp({ route: '/p/listings-daily/runs/nope' });
    expect(await screen.findByRole('alert')).toHaveTextContent('run listings-daily/nope not found');
    expect(screen.getByRole('heading', { level: 1, name: 'Run' })).toBeInTheDocument();
  });

  it('speaks French', async () => {
    await openRun(RUN, new FakeApi(), 'fr');
    expect(screen.getByRole('heading', { level: 1, name: 'Run de 06:00 · jeu. 15 oct.' })).toBeInTheDocument();
    expect(screen.getByText(/items : 2 034/)).toBeInTheDocument();
    expect(screen.getByRole('complementary', { name: 'Rapport du run' })).toHaveTextContent(/Fin prévue à 06:58/);
  });
});
