import { act, screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures, FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';
import { groupsIn, headline } from './DeadLettersScreen';

const route = '/p/listings-daily/dead-letters';
const group = (step: string) => screen.getByRole('region', { name: new RegExp(`^Group at ${step}:`) });

describe('dead letters', () => {
  it('groups open dead letters by fingerprint, largest first, with counts, step, times and the error', async () => {
    renderApp({ route });
    expect(screen.getByTestId('skeleton')).toBeInTheDocument();
    await screen.findByText(/Grouped by error fingerprint/);
    expect(screen.getByText(/Grouped by error fingerprint/)).toHaveTextContent(
      'Grouped by error fingerprint · 2 groups · 3 items',
    );
    expect(screen.getByRole('button', { name: 'Open 3' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'Replayed 1' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Discarded 1' })).toBeInTheDocument();
    const regions = screen.getAllByRole('region', { name: /^Group at/ });
    expect(regions.map((r) => r.getAttribute('aria-label')?.split(':')[0])).toEqual([
      'Group at score',
      'Group at fetch',
    ]);
    const score = group('score');
    expect(score).toHaveTextContent('Dead · 2 items');
    expect(within(score).getByRole('link', { name: 'score' })).toHaveAttribute(
      'href',
      '/p/listings-daily/graph?step=score',
    );
    expect(score).toHaveTextContent('fingerprint fp3c9a0d');
    expect(within(score).getByRole('button', { name: /Show full error/ })).toBeInTheDocument();
    expect(within(score).getAllByRole('row')).toHaveLength(3);
    expect(within(score).getByRole('link', { name: 'it_dl_9d02' })).toHaveAttribute(
      'href',
      '/p/listings-daily/items/it_dl_9d02',
    );
    expect(group('fetch')).toHaveTextContent('Dead · 1 item');
  });

  it('replays a group after a confirmation that states the consequence', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route });
    await screen.findByText(/Grouped by error fingerprint/);
    await user.click(within(group('score')).getByRole('button', { name: 'Replay 2' }));
    const dialog = screen.getByRole('dialog', { name: 'Replay 2 items at score?' });
    expect(dialog).toHaveTextContent('They go back into score’s queue now, in their own runs.');
    expect(dialog).toHaveTextContent('They run on their run’s revision.');
    expect(dialog).toHaveTextContent('If they fail the same way, they come back here as new dead letters.');
    // Every run is on the active revision: no migrate option.
    expect(within(dialog).queryByRole('checkbox')).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole('button', { name: 'Replay 2 items' }));
    expect(await screen.findByText('2 items replayed')).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({
      method: 'replayDeadLetters',
      args: ['listings-daily', { fingerprint: 'fp3c9a0d17' }],
    });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Open 1' })).toBeInTheDocument());
    expect(screen.queryByRole('region', { name: /^Group at score/ })).not.toBeInTheDocument();
  });

  it('offers migrate when a run is on an older revision, and sends it', async () => {
    const fixtures = structuredClone(demoFixtures);
    fixtures.runs[0]!.revision = 12;
    const api = new FakeApi(fixtures);
    const { user } = renderApp({ api, route });
    await screen.findByText(/Grouped by error fingerprint/);
    await user.click(within(group('fetch')).getByRole('button', { name: 'Replay 1' }));
    const dialog = screen.getByRole('dialog', { name: 'Replay 1 item at fetch?' });
    const migrate = await within(dialog).findByRole('checkbox', {
      name: 'Move their runs to the active revision r14 first',
    });
    expect(migrate).toHaveAccessibleDescription('They ran on r12. Fixing a route usually needs it.');
    await user.click(migrate);
    expect(dialog).toHaveTextContent('Their runs move to the active revision r14 first.');
    await user.click(within(dialog).getByRole('button', { name: 'Replay 1 item' }));
    expect(api.calls.at(-1)).toEqual({
      method: 'replayDeadLetters',
      args: ['listings-daily', { fingerprint: 'fp81b2e44c', migrate: true }],
    });
  });

  it('discards a selection after a destructive confirmation that opens on Cancel', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route });
    await screen.findByText(/Grouped by error fingerprint/);
    await user.click(screen.getByRole('checkbox', { name: 'Select it_dl_9d02' }));
    await user.click(screen.getByRole('checkbox', { name: 'Select it_dl_e2f8' }));
    expect(screen.getByRole('checkbox', { name: 'Select the group’s 2' })).toBePartiallyChecked();
    await user.click(screen.getByRole('button', { name: 'Discard selection (2)' }));
    const dialog = screen.getByRole('dialog', { name: 'Discard 2 dead letters?' });
    expect(dialog).toHaveTextContent('They leave the open dead letters for good: nothing replays them later.');
    expect(dialog).toHaveTextContent('Their runs record the loss and finish with errors.');
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Discard 2 dead letters' }));
    expect(await screen.findByText('2 dead letters discarded')).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({
      method: 'discardDeadLetters',
      args: ['listings-daily', { ids: ['it_dl_9d02', 'it_dl_e2f8'] }],
    });
    await user.click(screen.getByRole('button', { name: /^Discarded 3/ }));
    expect(screen.getByRole('region', { name: /^Group at fetch/ })).toBeInTheDocument();
    // Settled dead letters have no actions.
    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
  });

  it('replays or discards one, or all, and selects a whole group', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route });
    await screen.findByText(/Grouped by error fingerprint/);
    await user.click(screen.getByRole('button', { name: 'Replay it_dl_c41e' }));
    await user.click(screen.getByRole('button', { name: 'Replay 1 item' }));
    expect(api.calls.at(-1)?.args[1]).toEqual({ ids: ['it_dl_c41e'] });

    await user.click(await screen.findByRole('button', { name: 'Discard all 2' }));
    const dialog = screen.getByRole('dialog', { name: 'Discard 2 dead letters?' });
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();

    await user.click(within(group('fetch')).getByRole('checkbox', { name: 'Select the group’s 1' }));
    expect(screen.getByRole('button', { name: 'Replay selection (1)' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Clear selection' }));
    await user.click(screen.getByRole('button', { name: 'Replay all 2' }));
    expect(screen.getByRole('dialog', { name: 'Replay 2 items?' })).toHaveTextContent(
      'They go back into their steps’ queues now, in their own runs.',
    );
    await user.click(screen.getByRole('button', { name: 'Replay 2 items' }));
    expect(api.calls.at(-1)?.args[1]).toEqual({ all: true });
    expect(await screen.findByText('No open dead letters')).toBeInTheDocument();
  });

  it('takes the lists-and-tables keys: j, k, x, ⇧R', async () => {
    const { user } = renderApp({ route });
    await screen.findByText(/Grouped by error fingerprint/);
    const first = screen.getByRole('checkbox', { name: 'Select it_dl_9d02' });
    act(() => first.focus());
    await user.keyboard('j');
    expect(screen.getByRole('checkbox', { name: 'Select it_dl_c41e' })).toHaveFocus();
    await user.keyboard('x');
    expect(screen.getByRole('checkbox', { name: 'Select it_dl_c41e' })).toBeChecked();
    await user.keyboard('k');
    expect(first).toHaveFocus();
    await user.keyboard('{Shift>}R{/Shift}');
    expect(screen.getByRole('dialog', { name: 'Replay 1 item at score?' })).toBeInTheDocument();
    await user.keyboard('{Escape}');
    await user.keyboard('{Shift>}D{/Shift}');
    expect(screen.getByRole('dialog', { name: 'Discard 1 dead letter?' })).toBeInTheDocument();
  });

  it('says when an action failed, with Retry', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'replayDeadLetters').mockRejectedValueOnce(
      new ApiError(409, 'conflict', 'run run_0600 is cancelled.'),
    );
    const { user } = renderApp({ api, route });
    await screen.findByText(/Grouped by error fingerprint/);
    await user.click(within(group('fetch')).getByRole('button', { name: 'Replay 1' }));
    await user.click(screen.getByRole('button', { name: 'Replay 1 item' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Couldn’t replay 1: run run_0600 is cancelled. Nothing changed.');
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('1 item replayed')).toBeInTheDocument();
  });

  it('shows the empty state, and an error with Retry', async () => {
    const api = new FakeApi({ ...demoFixtures, deadLetters: [] });
    renderApp({ api, route });
    expect(await screen.findByText('No open dead letters')).toBeInTheDocument();

    const failing = new FakeApi();
    vi.spyOn(failing, 'listDeadLetters').mockRejectedValueOnce(new ApiError(500, 'internal', 'boom'));
    const { user } = renderApp({ api: failing, route });
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The dead letters could not be loaded.');
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findAllByText(/Grouped by error fingerprint/)).toHaveLength(1);
  });

  it('speaks French', async () => {
    renderApp({ route, language: 'fr' });
    expect(await screen.findByText(/Groupées par empreinte/)).toHaveTextContent(
      'Groupées par empreinte d’erreur · 2 groupes · 3 items',
    );
    expect(screen.getByRole('button', { name: 'Ouvertes 3' })).toBeInTheDocument();
    expect(screen.getByText('Morts · 2 items')).toBeInTheDocument();
  });

  it('regroups per state and shortens headlines', () => {
    const groups = [
      {
        fingerprint: 'f',
        step: 's',
        kind: 'dead' as const,
        error: 'e',
        count: 2,
        firstSeen: '1',
        lastSeen: '2',
        deadLetters: [
          {
            id: 'a',
            runId: 'r',
            originDelivery: 'd',
            stream: 's.dlq',
            state: 'open' as const,
            error: 'e',
            createdAt: '3',
          },
          {
            id: 'b',
            runId: 'r',
            originDelivery: 'd',
            stream: 's.dlq',
            state: 'replayed' as const,
            error: 'e',
            createdAt: '4',
          },
        ],
      },
    ];
    expect(groupsIn(groups, 'open')).toMatchObject([{ count: 1, firstSeen: '3', lastSeen: '3' }]);
    expect(groupsIn(groups, 'discarded')).toEqual([]);
    expect(headline('x'.repeat(200))).toHaveLength(140);
    expect(headline('first line\nsecond')).toBe('first line');
  });
});
