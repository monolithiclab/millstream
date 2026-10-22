import { act, screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { Audit, Lineage } from '../../api/schema';
import { FakeEventSource } from '../../live/fakeEventSource';
import { LIVE_REFRESH_MS } from '../../live/useCoalescedRefresh';
import { mockMatchMedia } from '../../test/flow';
import { renderApp } from '../../test/render';
import { withImport } from '../../test/imports';
import { lineageRows } from './ItemParts';
import { itemTitle, journeyOf } from './ItemScreen';
import { messagesOf } from './Transcript';

const route = '/p/listings-daily/items/it_7f3a9c21';

describe('item detail', () => {
  it('shows what the item is, where it stands, and its journey through its ancestors', async () => {
    renderApp({ route });
    expect(screen.getByTestId('loading')).toBeInTheDocument();
    expect(await screen.findByRole('heading', { level: 1, name: 'T3 lumineux, cour calme' })).toBeInTheDocument();
    expect(screen.getAllByText('it_7f3a9c21').length).toBeGreaterThan(0);
    expect(screen.getByText('Awaiting review at analyst-review')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'run_0600' })).toHaveAttribute('href', '/p/listings-daily/runs/run_0600');
    expect(screen.getByRole('link', { name: 'Review now' })).toHaveAttribute(
      'href',
      '/inbox?pipeline=listings-daily&delivery=dl_4b2e',
    );
    const journey = screen.getByRole('region', { name: 'Journey' });
    const tab = await within(journey).findByRole('tab', { name: 'Journey · 3 steps' });
    expect(tab).toHaveAttribute('aria-selected', 'true');
    const rows = within(journey).getAllByRole('button', { expanded: false });
    expect(rows.map((r) => r.textContent)).toEqual([
      expect.stringContaining('enrich'),
      expect.stringContaining('score'),
      expect.stringContaining('analyst-review'),
    ]);
    expect(rows[1]).toHaveTextContent('Done · 2 attempts');
    expect(rows[2]).toHaveTextContent('Awaiting review · in the review inbox');
    expect(screen.getByText('Total cost').parentElement).toHaveTextContent('Total cost $0.01');
  });

  it('opens a delivery on its attempts, their errors and the agent transcript as a conversation', async () => {
    const { user } = renderApp({ route });
    const journey = await screen.findByRole('region', { name: 'Journey' });
    const score = await within(journey).findByRole('button', { name: /score/ });
    await user.click(score);
    expect(score).toHaveAttribute('aria-expanded', 'true');
    expect(await within(journey).findByText('Attempt 1 · Failed · will retry')).toBeInTheDocument();
    expect(within(journey).getByText('attempt timed out after 30 s')).toBeInTheDocument();
    expect(await within(journey).findByText('Tool call')).toBeInTheDocument();
    expect(within(journey).getByText('get_comparables')).toBeInTheDocument();
    expect(within(journey).getByText('→ 14 comparables · median €9,120/m² · p25 €8,400/m²')).toBeInTheDocument();
    expect(within(journey).getByText('I will compare it with recent sales nearby.')).toBeInTheDocument();
    expect(within(journey).getByText('Final output · valid')).toBeInTheDocument();
    expect(within(journey).getByText('in 2,310 tok · out 184 tok')).toBeInTheDocument();
    await user.click(score);
    expect(within(journey).queryByText('Tool call')).not.toBeInTheDocument();
  });

  it('shows a dead letter’s failed journey open, with the invalid output', async () => {
    renderApp({ route: '/p/listings-daily/items/it_dl_9d02' });
    const journey = await screen.findByRole('region', { name: 'Journey' });
    const dead = await within(journey).findByRole('button', { name: /score/, expanded: true });
    expect(dead).toHaveTextContent(/Dead · schema: output.schema/);
    expect(await within(journey).findByText('Final output · invalid')).toBeInTheDocument();
    expect(within(journey).getAllByRole('button', { name: /Show full error/ }).length).toBeGreaterThan(0);
    // The dead letter's own item has nothing waiting: no step reads its stream, so it stands done there.
    // Its state is a chip in the title row.
    const row = screen.getByRole('heading', { level: 1, name: 'it_dl_9d02' }).parentElement;
    expect(row).toHaveTextContent(/Done/);
    expect(row).not.toHaveTextContent(/Pending/);
  });

  it('shows the payload collapsible with Copy, the artifacts by type, and the lineage up and down', async () => {
    const { user } = renderApp({ route });
    const payload = await screen.findByRole('region', { name: 'Payload' });
    expect(payload).toHaveTextContent('in scored');
    expect(payload).toHaveTextContent('"photos": [');
    await user.click(within(payload).getByRole('button', { name: 'Collapse "photos"' }));
    expect(within(payload).getByText('[ 3 items ]')).toBeInTheDocument();
    await user.click(within(payload).getByRole('button', { name: 'Expand all' }));
    await user.click(within(payload).getByRole('button', { name: 'Collapse all' }));
    expect(within(payload).getByText('[ 2 items ]')).toBeInTheDocument();
    await user.click(within(payload).getByRole('button', { name: 'Copy JSON' }));
    // user-event stands in for the clipboard.
    expect(await navigator.clipboard.readText()).toContain('"priceEur": 412000');
    expect(within(payload).getByRole('button', { name: 'Copied' })).toBeInTheDocument();

    const artifacts = screen.getByRole('region', { name: 'Artifacts' });
    expect(within(artifacts).getByRole('img', { name: 'living.svg' })).toHaveAttribute(
      'src',
      expect.stringMatching(/^data:image\/svg\+xml/),
    );
    expect(artifacts.querySelector('object[type="application/pdf"]')).toHaveAttribute(
      'data',
      expect.stringMatching(/^data:application\/pdf/),
    );
    await user.click(within(artifacts).getAllByRole('button', { name: 'Preview' })[0]!);
    expect(await within(artifacts).findByText(/"agent": "Agence du Canal"/)).toBeInTheDocument();
    // An artifact the API cannot serve: an error with Retry, not a crash.
    await user.click(within(artifacts).getByRole('button', { name: 'Preview' }));
    expect(await within(artifacts).findByRole('alert')).toHaveTextContent('The artifact could not be loaded.');

    const lineage = screen.getByRole('region', { name: 'Lineage' });
    const links = within(lineage).getAllByRole('link');
    expect(links.map((l) => l.textContent)).toEqual(['it_1a8d4f02', 'it_51c0e8d4']);
    expect(links[0]).toHaveAttribute('href', '/p/listings-daily/items/it_1a8d4f02');
    expect(lineage).toHaveTextContent('this item · score · scored');
    expect(lineage).toHaveTextContent('2 up:');
  });

  it('links a parent in another pipeline, reached through an imported stream, with its pipeline', async () => {
    const api = new FakeApi(withImport());
    const reads = vi.spyOn(api, 'listItemDeliveries');
    renderApp({ api, route: '/p/deal-flow/items/it_c0a1e5d9' });
    const lineage = await screen.findByRole('region', { name: 'Lineage' });
    const parent = within(lineage).getByRole('link', { name: 'it_7f3a9c21' });
    expect(parent).toHaveAttribute('href', '/p/listings-daily/items/it_7f3a9c21');
    expect(parent.parentElement).toHaveTextContent('in listings-daily');
    // The parent's deliveries are read in its own pipeline.
    await waitFor(() => expect(reads).toHaveBeenCalledWith('listings-daily', 'it_7f3a9c21'));
    expect(reads).not.toHaveBeenCalledWith('deal-flow', 'it_7f3a9c21');
  });

  it('lists a copy in another pipeline among the children, in French', async () => {
    renderApp({ api: new FakeApi(withImport()), route, language: 'fr' });
    const lineage = await screen.findByRole('region', { name: 'Lignée' });
    const copy = within(lineage).getByRole('link', { name: 'it_c0a1e5d9' });
    expect(copy).toHaveAttribute('href', '/p/deal-flow/items/it_c0a1e5d9');
    expect(copy.parentElement).toHaveTextContent('dans deal-flow');
  });

  it('follows live state events, keeps the raw events, and tails the item’s logs', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const api = new FakeApi();
    const { user } = renderApp({ api, route });
    const journey = await screen.findByRole('region', { name: 'Journey' });
    const stream = FakeEventSource.instances.find((s) => s.url.startsWith('/api/v1/live/items/it_7f3a9c21'))!;
    expect(stream.url).toBe('/api/v1/live/items/it_7f3a9c21?pipeline=listings-daily');
    const reads = vi.spyOn(api, 'listItemDeliveries');
    act(() => {
      stream.open();
      stream.emit(
        'state',
        JSON.stringify({
          seq: 41,
          ts: '2026-10-15T06:20:00Z',
          pipeline: 'listings-daily',
          kind: 'reviewDecided',
          data: { decision: 'approve' },
        }),
        '41',
      );
      stream.emit('state', 'not json');
      stream.emit(
        'state',
        JSON.stringify({ seq: 40, ts: '2026-10-15T06:19:00Z', pipeline: 'listings-daily', kind: 'leased', data: {} }),
        '40',
      );
    });
    // A burst of events reloads the item once, after the coalescing window.
    expect(reads).not.toHaveBeenCalled();
    await act(() => vi.advanceTimersByTimeAsync(LIVE_REFRESH_MS));
    expect(reads).toHaveBeenCalledTimes(1);
    await user.click(within(journey).getByRole('tab', { name: 'Raw events' }));
    expect(within(journey).getByText('reviewDecided')).toBeInTheDocument();
    // Arrow keys move between tabs.
    await user.keyboard('{ArrowLeft}');
    const logsTab = within(journey).getByRole('tab', { name: 'Logs' });
    expect(logsTab).toHaveFocus();
    expect(logsTab).toHaveAttribute('aria-selected', 'true');
    const tail = FakeEventSource.latest();
    expect(tail.url).toBe('/api/v1/live/logs?pipeline=listings-daily&item=it_7f3a9c21');
    // The shared log tail (common/LogTail, tested with the logs screen) over the item's scope.
    expect(within(journey).getByRole('log')).toHaveTextContent('Waiting for log lines…');
  });

  it('closes the item’s stream once nothing of it is waiting, running or in review', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const api = new FakeApi();
    const real = api.listItemDeliveries;
    let settled = false;
    api.listItemDeliveries = async (p, item, query) => {
      const list = await real(p, item, query);
      return settled
        ? list.map((d) => ({ ...d, state: d.state === 'awaitingReview' ? ('done' as const) : d.state }))
        : list;
    };
    renderApp({ api, route });
    await screen.findByRole('region', { name: 'Journey' });
    const stream = FakeEventSource.instances.find((s) => s.url.startsWith('/api/v1/live/items/'))!;
    expect(stream.closed).toBe(false);
    settled = true;
    act(() => {
      stream.open();
      stream.emit(
        'state',
        JSON.stringify({
          seq: 50,
          ts: '2026-10-15T06:30:00Z',
          pipeline: 'listings-daily',
          kind: 'reviewDecided',
          data: {},
        }),
        '50',
      );
    });
    await act(() => vi.advanceTimersByTimeAsync(LIVE_REFRESH_MS));
    await waitFor(() => expect(stream.closed).toBe(true));
    expect(FakeEventSource.instances.filter((s) => s.url.startsWith('/api/v1/live/items/') && !s.closed)).toEqual([]);
  });

  it('keeps each raw event once across a reconnection that resumes from the last event id', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const api = new FakeApi();
    const { user } = renderApp({ api, route });
    const journey = await screen.findByRole('region', { name: 'Journey' });
    const streams = () => FakeEventSource.instances.filter((s) => s.url.startsWith('/api/v1/live/items/'));
    const event = (seq: number, kind: string) =>
      JSON.stringify({ seq, ts: '2026-10-15T06:20:00Z', pipeline: 'listings-daily', kind, data: {} });
    act(() => {
      streams()[0]!.open();
      streams()[0]!.emit('state', event(41, 'leased'), '41');
      streams()[0]!.emit('state', event(42, 'reviewRequested'), '42');
      streams()[0]!.fail();
    });
    const reads = vi.spyOn(api, 'listItemDeliveries');
    await act(() => vi.advanceTimersByTimeAsync(1000));
    const second = streams()[1]!;
    expect(second.url).toBe('/api/v1/live/items/it_7f3a9c21?pipeline=listings-daily&lastEventId=42');
    act(() => second.open());
    // The reconnection reloads the item through REST; the raw events come only from the stream.
    await waitFor(() => expect(reads).toHaveBeenCalled());
    act(() => {
      // A server that replays the event at the cursor is not shown twice.
      second.emit('state', event(42, 'reviewRequested'), '42');
      second.emit('state', event(43, 'reviewDecided'), '43');
    });
    await user.click(within(journey).getByRole('tab', { name: 'Raw events' }));
    const kinds = within(journey)
      .getAllByRole('listitem')
      .map((li) => li.querySelector('.font-bold')?.textContent);
    expect(kinds).toEqual(['reviewDecided', 'reviewRequested', 'leased']);
  });

  it('moves the payload and the lineage into tabs at 375 px, in French', async () => {
    // Below the md breakpoint.
    mockMatchMedia(() => false);
    const { user } = renderApp({ route, language: 'fr' });
    const journey = await screen.findByRole('region', { name: 'Parcours' });
    const tabs = within(journey)
      .getAllByRole('tab')
      .map((tab) => tab.textContent);
    expect(tabs).toEqual(['Parcours · 3 étapes', 'Données', 'Lignée', 'Journaux']);
    expect(screen.queryByRole('region', { name: 'Données' })).not.toBeInTheDocument();
    expect(screen.getByText('En attente de revue à analyst-review')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Passer en revue' })).toBeInTheDocument();
    expect(screen.getByText('Coût total').parentElement).toHaveTextContent(/Coût total 0,01\s\$/);
    await user.click(within(journey).getByRole('tab', { name: 'Données' }));
    expect(screen.getByRole('region', { name: 'Données' })).toHaveTextContent('412000');
    await user.click(within(journey).getByRole('tab', { name: 'Lignée' }));
    expect(screen.getByRole('region', { name: 'Lignée' })).toHaveTextContent('cet item · score · scored');
  });

  it('shows an expired item’s state and notice in place of its payload and artifacts, its lineage marked', async () => {
    renderApp({ route: '/p/listings-daily/items/it_3b7e2c90' });
    expect(await screen.findByRole('heading', { level: 1, name: 'homesite-2198034' })).toBeInTheDocument();
    expect(screen.getAllByText('Expired')).toHaveLength(1);
    const payload = screen.getByRole('region', { name: 'Payload' });
    expect(payload).toHaveTextContent(
      'This item’s payload expired under its stream’s retention. Its envelope and lineage are kept.',
    );
    expect(within(payload).queryByRole('button', { name: 'Copy JSON' })).not.toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Artifacts' })).not.toBeInTheDocument();
    const lineage = screen.getByRole('region', { name: 'Lineage' });
    const child = within(lineage).getByRole('link', { name: 'it_8c4d1f6e' });
    expect(child.parentElement).toHaveTextContent('Erased');
  });

  it('names an erased item by its id and says what the purge took', async () => {
    renderApp({ route: '/p/listings-daily/items/it_8c4d1f6e' });
    expect(await screen.findByRole('heading', { level: 1, name: 'it_8c4d1f6e' })).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Payload' })).toHaveTextContent(
      'This item was erased by a purge. Its envelope and lineage are kept; its payload, key and artifacts are gone.',
    );
    expect(screen.queryByRole('region', { name: 'Artifacts' })).not.toBeInTheDocument();
    const lineage = screen.getByRole('region', { name: 'Lineage' });
    expect(within(lineage).getByRole('link', { name: 'it_3b7e2c90' }).parentElement).toHaveTextContent('Expired');
  });

  it('says an erased item’s notice in the payload tab at 375 px, in French', async () => {
    mockMatchMedia(() => false);
    const { user } = renderApp({ route: '/p/listings-daily/items/it_8c4d1f6e', language: 'fr' });
    const journey = await screen.findByRole('region', { name: 'Parcours' });
    expect(screen.getByText('Effacé')).toBeInTheDocument();
    await user.click(within(journey).getByRole('tab', { name: 'Données' }));
    expect(screen.getByRole('region', { name: 'Données' })).toHaveTextContent(
      /Cet item a été effacé par une purge\. Son enveloppe et sa lignée sont conservées\s; ses données, sa clé/,
    );
  });

  it('says which action produced the item and marks the delivery its fallback ran, with no audits section', async () => {
    renderApp({ route });
    await screen.findByRole('heading', { level: 1, name: 'T3 lumineux, cour calme' });
    expect(screen.getByText('Produced by').parentElement).toHaveTextContent('Produced by fallback');
    const journey = screen.getByRole('region', { name: 'Journey' });
    expect(await within(journey).findByRole('button', { name: /score\s*fallback/ })).toBeInTheDocument();
    expect(within(journey).getByRole('button', { name: /enrich/ })).not.toHaveTextContent('fallback');
    expect(screen.queryByRole('heading', { name: 'Audits' })).not.toBeInTheDocument();
  });

  it('lists an item’s audits: one agreeing, with both actions’ outputs', async () => {
    renderApp({ route: '/p/listings-daily/items/it_score_0' });
    const audits = await screen.findByRole('region', { name: 'Audits' });
    expect(within(audits).getAllByRole('listitem')).toHaveLength(1);
    expect(within(audits).getByRole('heading', { level: 3, name: 'score' })).toBeInTheDocument();
    expect(within(audits).getByText('Agrees')).toBeInTheDocument();
    expect(within(audits).getByText('Done')).toBeInTheDocument();
    expect(within(audits).getByText('Cost').parentElement).toHaveTextContent('Cost $0.01');
    expect(within(audits).getByText('dl_score_0')).toBeInTheDocument();
    // The producedBy badge is only on a step with a fallback's outputs.
    expect(screen.queryByText('Produced by')).not.toBeInTheDocument();
  });

  it('shows a disagreeing audit’s two outputs side by side, in French', async () => {
    renderApp({ route: '/p/listings-daily/items/it_score_1', language: 'fr' });
    const audits = await screen.findByRole('region', { name: 'Audits' });
    expect(within(audits).getByText('Diverge')).toBeInTheDocument();
    const primary = within(audits).getByRole('region', { name: 'Sorties de l’action principale' });
    const fallback = within(audits).getByRole('region', { name: 'Sorties de l’action de repli' });
    expect(within(primary).getByRole('heading', { level: 4 })).toBeInTheDocument();
    expect(primary).toHaveTextContent('"score": 58');
    expect(fallback).toHaveTextContent('"score": 81');
    expect(fallback).toHaveTextContent('"key": "montreuil-t4-81"');
  });

  it('shows a failed audit’s error and no fallback outputs, and a pending one not compared yet', async () => {
    const base = demoFixtures.audits[0]!;
    const failed: Audit = {
      ...base,
      id: 'au_failed',
      state: 'failed',
      fallback: null,
      agreement: null,
      error: 'fallback timed out after 120 s',
    };
    const pending: Audit = { ...base, id: 'au_pending', state: 'pending', fallback: null, agreement: null };
    const api = new FakeApi({ ...demoFixtures, audits: [failed, pending] });
    renderApp({ api, route: '/p/listings-daily/items/it_score_0' });
    const audits = await screen.findByRole('region', { name: 'Audits' });
    const [first, second] = within(audits).getAllByRole('listitem');
    expect(first).toHaveTextContent('Failed');
    expect(within(first!).getByText('fallback timed out after 120 s')).toBeInTheDocument();
    expect(within(first!).getByRole('region', { name: 'Fallback outputs' })).toHaveTextContent('No outputs.');
    expect(within(first!).queryByText('Not compared yet')).not.toBeInTheDocument();
    expect(within(first!).queryByText(/Agrees|Disagrees/)).not.toBeInTheDocument();
    expect(within(second!).getByText('Not compared yet')).toBeInTheDocument();
    expect(within(second!).queryByRole('button', { name: 'Copy error' })).not.toBeInTheDocument();
  });

  it('shows an error with Retry for an item that does not exist', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/p/listings-daily/items/nope' });
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The item could not be loaded.');
    expect(alert).toHaveTextContent('item listings-daily/nope not found');
    vi.spyOn(api, 'getItem').mockResolvedValueOnce(demoFixtures.items[0]!);
    vi.spyOn(api, 'getLineage').mockResolvedValueOnce({ item: 'nope', up: [], down: [], truncated: false });
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('heading', { level: 1, name: 'homesite-2210417' })).toBeInTheDocument();
    expect(screen.getByText('No parents and no children: this item stands alone.')).toBeInTheDocument();
    expect(screen.getByText('No artifacts.')).toBeInTheDocument();
    expect(screen.getByText('No step has read this item yet.')).toBeInTheDocument();
  });
});

describe('item helpers', () => {
  it('names an item by its title, name, key or id', () => {
    const base = demoFixtures.items[0]!;
    expect(itemTitle({ ...base, payload: { name: 'Studio' } })).toBe('Studio');
    expect(itemTitle({ ...base, payload: 3, key: 'k' })).toBe('k');
    expect(itemTitle({ ...base, payload: null, key: '' })).toBe(base.id);
  });

  it('builds the journey from the deliveries that produced the lineage', () => {
    const item = { ...demoFixtures.items[0]!, id: 'c', parents: ['b'], step: 'two' };
    const lineage: Lineage = {
      item: 'c',
      up: [
        { id: 'b', pipeline: 'p', runId: 'r', stream: 's', step: 'one', key: '', parents: ['a'], ts: '' },
        { id: 'a', pipeline: 'p', runId: 'r', stream: 't', step: 't', key: '', parents: [], ts: '' },
      ],
      down: [],
      truncated: false,
    };
    const d = (id: string, itemId: string, step: string, createdAt: string) =>
      ({ id, itemId, step, createdAt }) as never;
    const journey = journeyOf(
      item,
      lineage,
      [[d('d1', 'b', 'two', '2'), d('dx', 'b', 'other', '1')], [d('d0', 'a', 'one', '1')]],
      [d('d2', 'c', 'three', '3')],
    );
    expect(journey.map((x: { id: string }) => x.id)).toEqual(['d0', 'd1', 'd2']);
    expect(lineageRows(lineage, item.parents).map((r) => [r.node?.id ?? 'self', r.level])).toEqual([
      ['a', -2],
      ['b', -1],
      ['self', 0],
    ]);
  });

  it('reads transcripts of either harness as a conversation', () => {
    expect(
      messagesOf({
        turns: [
          { role: 'system', text: 'rules' },
          { role: 'user', text: 'score it' },
          { role: 'assistant', text: 'done', data: { content: [{ type: 'text', text: 'done' }] } },
          {
            role: 'assistant',
            data: {
              content: [
                { type: 'text', text: 'more' },
                { type: 'tool_result', content: [{ text: 'r' }] },
              ],
            },
          },
          { role: 'user', text: 'again' },
          { role: 'result', data: {} },
        ],
      }),
    ).toEqual([
      { kind: 'system', text: 'rules' },
      { kind: 'input', text: 'score it' },
      { kind: 'response', text: 'done' },
      { kind: 'response', text: 'more' },
      { kind: 'toolResult', text: 'r' },
      { kind: 'user', text: 'again' },
    ]);
    expect(messagesOf({ prompt: 'p' })).toEqual([{ kind: 'input', text: 'p' }]);
  });
});
