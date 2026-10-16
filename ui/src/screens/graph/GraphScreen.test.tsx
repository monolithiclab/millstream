import { act, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { PipelineSnapshot } from '../../api/schema';
import { FakeEventSource } from '../../live/fakeEventSource';
import { mockMatchMedia } from '../../test/flow';
import { renderApp } from '../../test/render';
import { withImport } from '../../test/imports';
import { LIVE_REFRESH_MS } from '../../live/useCoalescedRefresh';
import { PAINT_MS } from './GraphScreen';

beforeEach(() => mockMatchMedia(() => false));

const snapshot = demoFixtures.snapshots[0]!;

function withFigures(change: (step: PipelineSnapshot['steps'][number]) => void, seq: number): PipelineSnapshot {
  const next = structuredClone(snapshot);
  next.seq = seq;
  next.steps.forEach(change);
  return next;
}

// The pipeline's live stream, which the graph shares with the shell's live mark.
function graphStream(): FakeEventSource {
  const streams = FakeEventSource.instances.filter((s) => s.url === '/api/v1/live/pipelines/listings-daily');
  return streams.at(-1)!;
}

function emit(data: PipelineSnapshot) {
  act(() => graphStream().emit('snapshot', JSON.stringify(data)));
}

const step = (name: string) => screen.findByRole('button', { name: new RegExp(`^${name},`) });

describe('pipeline graph', () => {
  it('shares the pipeline stream with the shell: one connection, open while either needs it', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    const open = () =>
      FakeEventSource.instances.filter((s) => s.url === '/api/v1/live/pipelines/listings-daily' && !s.closed);
    expect(open()).toHaveLength(1);
    act(() => graphStream().open());
    expect(screen.getAllByRole('status').some((s) => s.textContent === 'Live')).toBe(true);
    // Leaving for a workspace screen drops both subscribers: the connection closes.
    await user.click(screen.getByRole('link', { name: 'Inbox' }));
    expect(open()).toHaveLength(0);
  });

  it('shows a skeleton, then every step as a button in topological order, with the table alternative', async () => {
    renderApp({ route: '/p/listings-daily/graph' });
    expect(screen.getByTestId('loading')).toBeInTheDocument();
    await step('fetch');
    const names = screen
      .getAllByRole('button')
      .map((b) => b.getAttribute('data-step'))
      .filter((n) => n !== null);
    expect(names).toEqual([
      'fetch',
      'parse',
      'dedupe',
      'geocode',
      'enrich',
      'score',
      'analyst-review',
      'render',
      'publish',
    ]);
    // Before the first snapshot, figures wait.
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(
      'score, Action step, Failing, — no figures yet',
    );
    const table = screen.getByRole('table', { name: 'Steps and their live figures' });
    expect(within(table).getAllByRole('row')).toHaveLength(10);
    expect(within(table).getByRole('rowheader', { name: /dedupe Dedupe/ })).toBeInTheDocument();
    // Channels name their stream for screen readers.
    expect(
      screen.getByRole('img', { name: 'Stream enriched from enrich to score: 1,102 items, 0 waiting' }),
    ).toBeInTheDocument();
  });

  it('paints live snapshots at most every 250 ms, the newest of each window', async () => {
    renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    vi.useFakeTimers();
    act(() => graphStream().open());
    emit(snapshot);
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(
      'score, Action step, Failing, 12 in flight',
    );
    emit(withFigures((s) => (s.inFlight = s.step === 'score' ? 5 : s.inFlight), 2));
    emit(withFigures((s) => (s.inFlight = s.step === 'score' ? 7 : s.inFlight), 3));
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(/12 in flight/);
    act(() => vi.advanceTimersByTime(PAINT_MS));
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(/7 in flight/);
    // The table follows the same snapshot.
    const row = within(screen.getByRole('table')).getByRole('row', { name: /^score/ });
    expect(row).toHaveTextContent('186');
    // Queued grains and their count pack against the head of score.
    expect(within(screen.getByTestId('channel-enriched')).getByText('186')).toBeInTheDocument();
  });

  it('outlines failing steps, badges insights and dead letters, and switches overlays without moving nodes', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    emit(snapshot);
    expect(screen.getByText('2 dead')).toBeInTheDocument();
    expect(screen.getByText('1 dead')).toBeInTheDocument();
    expect(within(screen.getByTestId('rf__node-step:enrich')).getByText('Slow')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^enrich,/ })).toHaveAccessibleName(/^enrich, Action step, Slow/);
    expect(screen.getByRole('button', { name: /^publish,/ })).toHaveAccessibleName(/^publish, Action step, Paused/);
    expect(screen.getByRole('button', { name: /^analyst-review,/ })).toHaveAccessibleName(
      'analyst-review, Review step, Awaiting review, 8 to review',
    );
    const before = screen.getByTestId('rf__node-step:score').style.transform;
    await user.click(screen.getByRole('button', { name: 'Errors' }));
    expect(screen.getByRole('button', { name: 'Errors' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(/1.1% error rate$/);
    await user.click(screen.getByRole('button', { name: 'Latency' }));
    expect(screen.getByRole('button', { name: /^geocode,/ })).toHaveAccessibleName(/4\.1\ss p95 latency$/);
    await user.click(screen.getByRole('button', { name: 'Queues' }));
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(/186 waiting$/);
    expect(screen.getByTestId('rf__node-step:score').style.transform).toBe(before);
  });

  it('moves between steps with arrow keys, opens the panel with Enter and closes it with Esc', async () => {
    const { user, location } = renderApp({ route: '/p/listings-daily/graph' });
    const fetch = await step('fetch');
    act(() => fetch.focus());
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('button', { name: /^parse,/ })).toHaveFocus();
    await user.keyboard('{ArrowLeft}');
    expect(fetch).toHaveFocus();
    await user.keyboard('{ArrowLeft}{ArrowUp}{ArrowDown}');
    expect(fetch).toHaveFocus();
    await user.keyboard('{ArrowRight}{Enter}');
    expect(location()).toBe('/p/listings-daily/graph?step=parse');
    const panel = screen.getByRole('complementary', { name: 'parse' });
    expect(within(panel).getByRole('heading', { level: 2, name: 'parse' })).toHaveFocus();
    expect(screen.getByRole('button', { name: /^parse,/ })).toHaveAttribute('aria-expanded', 'true');
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('complementary')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^parse,/ })).toHaveFocus();
    expect(location()).toBe('/p/listings-daily/graph');
  });

  it('opens the panel from ?step with the figures, the insight and the recent failures', async () => {
    renderApp({ route: '/p/listings-daily/graph?step=score' });
    const panel = await screen.findByRole('complementary', { name: 'score' });
    emit(snapshot);
    expect(within(panel).getByText('Action step · agent.score · revision r14')).toBeInTheDocument();
    expect(within(panel).getByText('Insight · Failure cluster')).toBeInTheDocument();
    expect(within(panel).getByText('In flight').nextElementSibling).toHaveTextContent('12');
    expect(within(panel).getByText('Waiting').nextElementSibling).toHaveTextContent('186');
    expect(within(panel).getByText('2 open dead letters')).toBeInTheDocument();
    expect(within(panel).getAllByRole('listitem')).toHaveLength(2);
    expect(within(panel).getByRole('link', { name: 'All dead letters' })).toHaveAttribute(
      'href',
      '/p/listings-daily/dead-letters',
    );
    expect(within(panel).getByRole('link', { name: 'Open its items' })).toHaveAttribute(
      'href',
      '/p/listings-daily/items?step=score',
    );
  });

  it('pauses and resumes a step, and says when it could not', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/p/listings-daily/graph?step=parse' });
    const panel = await screen.findByRole('complementary', { name: 'parse' });
    await user.click(within(panel).getByRole('button', { name: 'Pause step' }));
    expect(api.calls.at(-1)).toEqual({ method: 'pause', args: [{ pipeline: 'listings-daily', step: 'parse' }] });
    expect(await screen.findByText('parse paused: its deliveries wait until you resume it.')).toBeInTheDocument();
    await user.click(await within(panel).findByRole('button', { name: 'Resume step' }));
    expect(api.calls.at(-1)?.method).toBe('resume');

    vi.spyOn(api, 'pause').mockRejectedValueOnce(new ApiError(503, 'unavailable', 'the engine is switching over.'));
    await user.click(await within(panel).findByRole('button', { name: 'Pause step' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Couldn’t pause parse: the engine is switching over. Nothing changed.');
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('parse paused: its deliveries wait until you resume it.')).toBeInTheDocument();
  });

  it('replays a recent failure from the panel after a confirmation that states the consequence', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/p/listings-daily/graph?step=fetch' });
    const panel = await screen.findByRole('complementary', { name: 'fetch' });
    await user.click(within(panel).getByRole('button', { name: 'Replay' }));
    const dialog = screen.getByRole('dialog', { name: 'Replay 1 item at fetch?' });
    expect(dialog).toHaveTextContent('They go back into fetch’s queue now, in their own runs.');
    expect(within(dialog).getByRole('button', { name: 'Replay 1 item' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Replay 1 item' }));
    expect(await screen.findByText('1 item replayed')).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({
      method: 'replayDeadLetters',
      args: ['listings-daily', { ids: ['it_dl_e2f8'] }],
    });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await waitFor(() => expect(within(panel).getByText('No open dead letters at this step.')).toBeInTheDocument());
  });

  it('discards a recent failure from the panel after a confirmation that it is for good', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/p/listings-daily/graph?step=fetch' });
    const panel = await screen.findByRole('complementary', { name: 'fetch' });
    await user.click(within(panel).getByRole('button', { name: 'Discard' }));
    const dialog = screen.getByRole('dialog', { name: 'Discard 1 dead letter?' });
    expect(dialog).toHaveTextContent('They leave the open dead letters for good: nothing replays them later.');
    await user.click(within(dialog).getByRole('button', { name: 'Discard 1 dead letter' }));
    expect(await screen.findByText('1 dead letter discarded')).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({
      method: 'discardDeadLetters',
      args: ['listings-daily', { ids: ['it_dl_e2f8'] }],
    });
    await waitFor(() => expect(within(panel).getByText('No open dead letters at this step.')).toBeInTheDocument());
  });

  it('opens a review step paused by its budget, with nothing waiting, and more unrouted failures than it lists', async () => {
    const fixtures = structuredClone({ ...demoFixtures, metrics: undefined });
    const review = fixtures.pipelines[0]!.steps.find((s) => s.name === 'analyst-review')!;
    Object.assign(review, { paused: true, pauseCause: 'budget.day' });
    const unrouted = [1, 2, 3, 4, 5].map((n) => ({
      ...demoFixtures.deadLetters[0]!,
      id: `it_unrouted_${n}`,
      step: 'analyst-review',
      kind: 'unrouted' as const,
      fingerprint: 'fp_unrouted',
      pattern: 'no route matched the output',
      error: 'no route matched the output',
      createdAt: `2026-10-15T06:0${n}:00Z`,
    }));
    fixtures.deadLetters.push(...unrouted);
    const { user } = renderApp({
      api: new FakeApi({ ...fixtures, metrics: demoFixtures.metrics }),
      route: '/p/listings-daily/graph?step=analyst-review',
    });
    const panel = await screen.findByRole('complementary', { name: 'analyst-review' });
    emit(snapshot);
    expect(within(panel).getByText('Paused by budget.day')).toBeInTheDocument();
    expect(within(panel).getByRole('button', { name: 'Resume step' })).toBeInTheDocument();
    expect(within(panel).getByText('To review').nextElementSibling).toHaveTextContent(/^\d+$/);
    expect(within(panel).getByText('Waiting').nextElementSibling).toHaveTextContent('0');
    expect(within(panel).getByText('Waiting').nextElementSibling).toHaveClass('text-ink');
    expect(within(panel).getByText('Errors').nextElementSibling).toHaveClass('text-ink');
    const failures = await within(panel).findAllByText('Unrouted');
    expect(failures).toHaveLength(3);
    expect(within(panel).getByText('2 more in the dead letters.')).toBeInTheDocument();
    // Esc closes the confirmation opened from the panel first, then the panel.
    await user.click(within(panel).getAllByRole('button', { name: 'Discard' })[0]!);
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('complementary', { name: 'analyst-review' })).toBeInTheDocument();
  });

  it('lets grains drift, holds them in a hidden tab, and freezes them under reduced motion', async () => {
    const view = renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    emit(snapshot);
    const grains = () => view.container.querySelectorAll('[data-motion]');
    expect(grains().length).toBeGreaterThan(0);
    for (const g of grains()) {
      expect(g).toHaveAttribute('data-motion', 'moving');
      expect(g).toHaveClass('ms-grains-moving');
    }
    view.unmount();

    mockMatchMedia((query) => query.includes('reduce'));
    const still = renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    emit(snapshot);
    const frozen = still.container.querySelectorAll('[data-motion]');
    expect(frozen.length).toBeGreaterThan(0);
    for (const g of frozen) {
      expect(g).toHaveAttribute('data-motion', 'still');
      expect(g).not.toHaveClass('ms-grains-moving');
      expect(g.getAttribute('style') ?? '').not.toMatch(/animation/);
    }
    // Packed queues stay.
    expect(within(still.getByTestId('channel-enriched')).getByText('186')).toBeInTheDocument();
  });

  it('pauses grain drift while the tab is hidden', async () => {
    const view = renderApp({ route: '/p/listings-daily/graph' });
    await step('score');
    emit(snapshot);
    const hidden = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    act(() => document.dispatchEvent(new Event('visibilitychange')));
    for (const g of view.container.querySelectorAll('[data-motion]')) expect(g).toHaveClass('ms-grains-paused');
    hidden.mockRestore();
  });

  it('shows the minimap above 12 steps only', async () => {
    renderApp({ route: '/p/city-permits/graph' });
    await step('ingest');
    expect(screen.getByLabelText('Minimap')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^keep-good, Filter step/ })).toBeInTheDocument();
  });

  it('has no minimap at 12 steps or fewer, and shows the run so far', async () => {
    renderApp({ route: '/p/listings-daily/graph' });
    await step('fetch');
    expect(screen.queryByLabelText('Minimap')).not.toBeInTheDocument();
    const run = await screen.findByRole('region', { name: 'Run 06:00 so far' });
    expect(within(run).getByRole('link', { name: '1,632 raw' })).toHaveAttribute(
      'href',
      '/p/listings-daily/items?stream=raw&run=run_0600',
    );
    expect(within(run).getByText('$9.84')).toBeInTheDocument();
  });

  it('picks up a run that starts while the graph is open: new events refetch what the REST reads show', async () => {
    const api = new FakeApi();
    const real = await api.getPipeline('listings-daily');
    let started = false;
    let reads = 0;
    api.getPipeline = () => {
      reads += 1;
      const { lastRun, ...idle } = real;
      return Promise.resolve(started && lastRun ? { ...idle, lastRun } : idle);
    };
    renderApp({ route: '/p/listings-daily/graph', api });
    await step('score');
    vi.useFakeTimers();
    act(() => graphStream().open());
    emit(snapshot);
    expect(screen.queryByRole('region', { name: 'Run 06:00 so far' })).not.toBeInTheDocument();
    // A snapshot that reflects no new event refetches nothing.
    act(() => vi.advanceTimersByTime(LIVE_REFRESH_MS + PAINT_MS));
    emit(snapshot);
    act(() => vi.advanceTimersByTime(LIVE_REFRESH_MS + PAINT_MS));
    const before = reads;
    // The run is fired elsewhere: its events advance the snapshots' seq, and the graph reads again, once a window.
    started = true;
    emit({ ...snapshot, seq: snapshot.seq + 1 });
    act(() => vi.advanceTimersByTime(PAINT_MS));
    emit({ ...snapshot, seq: snapshot.seq + 2 });
    await act(async () => vi.advanceTimersByTimeAsync(LIVE_REFRESH_MS));
    expect(reads).toBe(before + 1);
    vi.useRealTimers();
    expect(await screen.findByRole('region', { name: 'Run 06:00 so far' })).toBeInTheDocument();
  });

  it('shows an error with Retry, and an empty graph', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'getPipeline').mockRejectedValueOnce(new ApiError(500, 'internal', 'database is locked'));
    const { user } = renderApp({ api, route: '/p/listings-daily/graph' });
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The graph could not be loaded.');
    expect(alert).toHaveTextContent('database is locked');
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    await step('fetch');

    const empty = new FakeApi({
      ...demoFixtures,
      pipelines: [{ ...demoFixtures.pipelines[0]!, steps: [], triggers: [] }],
    });
    renderApp({ api: empty, route: '/p/listings-daily/graph' });
    expect(await screen.findByText('No steps')).toBeInTheDocument();
  });

  it('draws an imported stream as an entry labelled with its producer, in both languages', async () => {
    const { unmount } = renderApp({ api: new FakeApi(withImport()), route: '/p/deal-flow/graph' });
    await step('notify');
    const entry = screen.getByRole('group', { name: 'Stream approved imported from listings-daily' });
    expect(entry).toHaveTextContent('from listings-daily · scored');
    unmount();
    renderApp({ api: new FakeApi(withImport()), route: '/p/deal-flow/graph', language: 'fr' });
    await step('notify');
    expect(screen.getByRole('group', { name: 'Flux approved importé de listings-daily' })).toHaveTextContent(
      'de listings-daily · scored',
    );
  });

  it('speaks French', async () => {
    renderApp({ route: '/p/listings-daily/graph', language: 'fr' });
    await screen.findByRole('button', { name: /^score,/ });
    emit(snapshot);
    expect(screen.getByRole('button', { name: /^score,/ })).toHaveAccessibleName(
      'score, étape Action, En échec, 12 en cours',
    );
    expect(screen.getByRole('button', { name: 'Erreurs' })).toBeInTheDocument();
    expect(screen.getByText('2 morts')).toBeInTheDocument();
  });
});
