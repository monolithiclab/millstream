import { act, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeEventSource } from '../../live/fakeEventSource';
import { renderApp } from '../../test/render';
import { MAX_LINES, MAX_PAUSED_LINES, PAINT_MS } from '../../common/LogTail';

const line = (ts: string, level: string, message: string, step = 'score') =>
  JSON.stringify({ ts, level, message, pipeline: 'p', runId: 'r', step, itemId: '', deliveryId: '', attempt: 1 });
const source = () => FakeEventSource.instances.filter((s) => s.url.startsWith('/api/v1/live/logs')).at(-1)!;
const tail = () => screen.getByRole('log', { name: 'Live log tail' });
const paint = () => act(() => vi.advanceTimersByTimeAsync(PAINT_MS));

describe('LogsScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  it('tails the live log, painting in batches, and marks a gap with its window', async () => {
    renderApp({ route: '/logs' });
    expect(source().url).toBe('/api/v1/live/logs');
    expect(tail()).toHaveTextContent('Waiting for log lines…');
    act(() => {
      source().open();
      source().emit('log', line('2026-10-15T06:21:03.950Z', 'warn', 'retry 2/2'), '3');
      source().emit('gap', '{"lastEventId":"3"}');
      source().emit('log', line('2026-10-15T06:21:19.402Z', 'info', 'done in 2.1 s'), '9');
      source().emit('log', 'not json', '10');
    });
    expect(tail()).toHaveTextContent('Waiting for log lines…');
    await paint();
    const rows = within(tail()).getAllByRole('paragraph');
    expect(rows).toHaveLength(3);
    expect(rows[0]).toHaveTextContent(/^\d\d:21:03\.950WARNscore · retry 2\/2$/);
    expect(rows[1]).toHaveTextContent(
      /^Connection lost \d\d:21:03\.950 → \d\d:21:19\.402 · lines from this window may be missing$/,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Live');
  });

  it('marks a gap with no line before it, and keeps the last lines only', async () => {
    renderApp({ route: '/logs' });
    act(() => {
      source().emit('gap', '{}');
      for (let i = 0; i < MAX_LINES + 5; i++) source().emit('log', line('2026-10-15T06:00:00Z', 'info', `n${i}`));
    });
    await paint();
    const rows = within(tail()).getAllByRole('paragraph');
    expect(rows).toHaveLength(MAX_LINES);
    expect(rows[0]).toHaveTextContent('n5');
  });

  it('says a gap is open until the next line', async () => {
    renderApp({ route: '/logs' });
    act(() => source().emit('gap', '{}'));
    await paint();
    expect(tail()).toHaveTextContent('Connection lost: lines from this moment may be missing');
  });

  it('pauses scrolling, counts new lines, and resumes from the pill', async () => {
    const { user } = renderApp({ route: '/logs' });
    act(() => source().emit('log', line('2026-10-15T06:00:00Z', 'info', 'first')));
    await paint();
    await user.click(screen.getByRole('button', { name: 'Pause scroll' }));
    expect(screen.getByRole('button', { name: 'Resume scroll' })).toHaveAttribute('aria-pressed', 'true');
    act(() => {
      source().emit('log', line('2026-10-15T06:00:01Z', 'error', 'second'));
      source().emit('log', line('2026-10-15T06:00:02Z', 'info', 'third'));
    });
    await paint();
    expect(tail()).not.toHaveTextContent('second');
    await user.click(screen.getByRole('button', { name: '2 new · show' }));
    expect(tail()).toHaveTextContent('second');
    expect(screen.getByRole('button', { name: 'Pause scroll' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Clear' }));
    expect(tail()).toHaveTextContent('Waiting for log lines…');
  });

  it('keeps a paused tail’s lines past the cap, buffering and counting what it drops', async () => {
    const { user } = renderApp({ route: '/logs' });
    act(() => {
      source().emit('log', line('2026-10-15T06:00:00Z', 'info', 'kept-a'));
      source().emit('log', line('2026-10-15T06:00:00Z', 'info', 'kept-b'));
    });
    await paint();
    await user.click(screen.getByRole('button', { name: 'Pause scroll' }));
    act(() => {
      for (let i = 0; i < MAX_PAUSED_LINES + 1; i++)
        source().emit('log', line('2026-10-15T06:00:01Z', 'info', `p${i}`));
    });
    await paint();
    // What was shown stays; the three oldest arrivals made room under the cap.
    expect(tail()).toHaveTextContent('kept-a');
    const pill = screen.getByRole('button', { name: `${MAX_PAUSED_LINES - 2} new, 3 dropped · show` });
    await user.click(pill);
    const rows = within(tail()).getAllByRole('paragraph');
    expect(rows).toHaveLength(MAX_LINES);
    expect(rows.at(-1)).toHaveTextContent(`p${MAX_PAUSED_LINES}`);
  });

  it('clears the lines still waiting to be painted too', async () => {
    const { user } = renderApp({ route: '/logs' });
    act(() => source().emit('log', line('2026-10-15T06:00:00Z', 'info', 'queued')));
    await user.click(screen.getByRole('button', { name: 'Clear' }));
    await paint();
    expect(tail()).toHaveTextContent('Waiting for log lines…');
  });

  it('keeps the live mark, Pause and Clear with the tail, after the filters', () => {
    renderApp({ route: '/logs' });
    const item = screen.getByRole('textbox', { name: 'Item' });
    const pause = screen.getByRole('button', { name: 'Pause scroll' });
    expect(item.compareDocumentPosition(pause) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(pause.parentElement).toContainElement(screen.getByRole('button', { name: 'Clear' }));
    expect(pause.parentElement).not.toContainElement(tail());
  });

  it('filters by pipeline, run, step and item in the URL, starting a new tail', async () => {
    const { user, location } = renderApp({ route: '/logs?run=run_0600' });
    expect(source().url).toBe('/api/v1/live/logs?run=run_0600');
    act(() => source().emit('log', line('2026-10-15T06:00:00Z', 'info', 'old')));
    await paint();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Pipeline' }), 'listings-daily');
    expect(location()).toBe('/logs?pipeline=listings-daily&run=run_0600');
    expect(source().url).toBe('/api/v1/live/logs?pipeline=listings-daily&run=run_0600');
    expect(tail()).not.toHaveTextContent('old');
    await user.type(screen.getByRole('textbox', { name: 'Step' }), 'score{Enter}');
    expect(location()).toBe('/logs?pipeline=listings-daily&run=run_0600&step=score');
    const item = screen.getByRole('textbox', { name: 'Item' });
    await user.type(item, 'it_7f3a');
    await user.tab();
    expect(location()).toBe('/logs?pipeline=listings-daily&run=run_0600&step=score&item=it_7f3a');
    await user.clear(screen.getByRole('textbox', { name: 'Run' }));
    await user.tab();
    expect(location()).toBe('/logs?pipeline=listings-daily&step=score&item=it_7f3a');
  });

  it('speaks French', () => {
    renderApp({ route: '/logs', language: 'fr' });
    expect(screen.getByRole('heading', { level: 1, name: 'Journaux' })).toBeInTheDocument();
    expect(screen.getByRole('log', { name: 'Journal en direct' })).toHaveTextContent(
      'En attente de lignes de journal…',
    );
  });
});
