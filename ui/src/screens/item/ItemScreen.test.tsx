import { act, screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { Lineage } from '../../api/schema';
import { FakeEventSource } from '../../live/fakeEventSource';
import { mockMatchMedia } from '../../test/flow';
import { renderApp } from '../../test/render';
import { lineageRows } from './ItemParts';
import { itemTitle, journeyOf } from './ItemScreen';
import { messagesOf } from './Transcript';

const route = '/p/listings-daily/items/it_7f3a9c21';

describe('item detail', () => {
  it('shows what the item is, where it stands, and its journey through its ancestors', async () => {
    renderApp({ route });
    expect(screen.getByTestId('skeleton')).toBeInTheDocument();
    expect(await screen.findByRole('heading', { level: 1, name: 'T3 lumineux, cour calme' })).toBeInTheDocument();
    expect(screen.getAllByText('it_7f3a9c21').length).toBeGreaterThan(0);
    expect(screen.getByText('Awaiting review at analyst-review')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'run_0600' })).toHaveAttribute(
      'href',
      '/p/listings-daily/runs?run=run_0600',
    );
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
    // The dead letter's own item has nothing waiting: it stands done at its stream.
    expect(screen.getByRole('heading', { level: 1, name: 'it_dl_9d02' })).toBeInTheDocument();
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

  it('follows live state events, keeps the raw events, and tails the item’s logs', async () => {
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
    });
    await waitFor(() => expect(reads).toHaveBeenCalled());
    await user.click(within(journey).getByRole('tab', { name: 'Raw events' }));
    expect(within(journey).getByText('reviewDecided')).toBeInTheDocument();
    // Arrow keys move between tabs.
    await user.keyboard('{ArrowLeft}');
    const logsTab = within(journey).getByRole('tab', { name: 'Logs' });
    expect(logsTab).toHaveFocus();
    expect(logsTab).toHaveAttribute('aria-selected', 'true');
    const tail = FakeEventSource.latest();
    expect(tail.url).toBe('/api/v1/live/logs?pipeline=listings-daily&item=it_7f3a9c21');
    expect(within(journey).getByText('No log lines yet.')).toBeInTheDocument();
    act(() => {
      tail.open();
      tail.emit(
        'log',
        JSON.stringify({ ts: '2026-10-15T06:21:00Z', level: 'warn', message: 'retrying', step: 'score' }),
        '7',
      );
      tail.emit('gap', JSON.stringify({ lastEventId: '7' }));
      tail.emit('log', 'not json');
    });
    const log = within(journey).getByRole('log');
    expect(log).toHaveTextContent('WARN');
    expect(log).toHaveTextContent('score · retrying');
    expect(log).toHaveTextContent('Lines missed while reconnecting');
    await user.click(within(journey).getByRole('button', { name: 'Pause scroll' }));
    expect(within(journey).getByRole('button', { name: 'Pause scroll' })).toHaveAttribute('aria-pressed', 'true');
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
        { id: 'b', runId: 'r', stream: 's', step: 'one', key: '', parents: ['a'], ts: '' },
        { id: 'a', runId: 'r', stream: 't', step: 't', key: '', parents: [], ts: '' },
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
