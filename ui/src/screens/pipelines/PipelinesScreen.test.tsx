import { screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';

const row = (name: string) => screen.getByRole('row', { name: new RegExp(`^${name}`) });

describe('pipelines list', () => {
  it('shows a skeleton, then each pipeline with its state, revision, last run and dead letters', async () => {
    renderApp({ route: '/pipelines' });
    expect(screen.getByTestId('loading')).toBeInTheDocument();
    const table = await screen.findByRole('table');
    expect(within(table).getAllByRole('row')).toHaveLength(5);
    expect(screen.getByText(/4 pipelines/)).toHaveTextContent('4 pipelines · 1 running · 3 open dead letters');

    const listings = row('listings-daily');
    expect(within(listings).getByRole('link', { name: 'listings-daily' })).toHaveAttribute(
      'href',
      '/p/listings-daily/graph',
    );
    expect(listings).toHaveTextContent('Active');
    expect(listings).toHaveTextContent('9 steps');
    expect(listings).toHaveTextContent('r14');
    expect(within(listings).getByRole('link', { name: 'Running' })).toHaveAttribute(
      'href',
      '/p/listings-daily/runs/run_0600',
    );
    expect(within(listings).getByRole('link', { name: '3 open dead letters' })).toHaveAttribute(
      'href',
      '/p/listings-daily/dead-letters',
    );
    expect(row('photo-tagging')).toHaveTextContent(/Succeeded.*22 min/);
    expect(row('rentals-weekly')).toHaveTextContent('Paused');
    expect(row('rentals-weekly')).toHaveTextContent('No run yet');
    // Items and cost today are not in the API: no such columns.
    expect(screen.queryByRole('columnheader', { name: /today/i })).not.toBeInTheDocument();
  });

  it('says pipelines change through their files behind the help toggle, not beside the counts', async () => {
    const { user } = renderApp({ route: '/pipelines' });
    await screen.findByRole('table');
    expect(screen.queryByText(/Pipelines are files/)).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'About this page' }));
    expect(screen.getByText(/Pipelines are files/)).toHaveTextContent(
      'Pipelines are files: edit their package, then millstream apply',
    );
    expect(screen.getByText('millstream apply').tagName).toBe('CODE');
  });

  it('pauses after a confirmation that states the consequence, opening on its primary action', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    await user.click(within(row('listings-daily')).getByRole('button', { name: 'Pause listings-daily' }));
    const dialog = screen.getByRole('dialog', { name: 'Pause listings-daily?' });
    expect(dialog).toHaveTextContent('No step starts a new delivery: they wait, pending, until you resume.');
    expect(dialog).toHaveTextContent('Triggers still fire; their items wait with the rest.');
    expect(within(dialog).getByRole('button', { name: 'Pause listings-daily' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Pause listings-daily' }));
    expect(
      await screen.findByText('listings-daily paused: its deliveries wait until you resume it.'),
    ).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({ method: 'pause', args: [{ pipeline: 'listings-daily' }] });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await waitFor(() => expect(row('listings-daily')).toHaveTextContent('Paused'));
  });

  it('resumes, cancels, and reports a failure with Retry', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    await user.click(within(row('rentals-weekly')).getByRole('button', { name: 'Resume rentals-weekly' }));
    const dialog = screen.getByRole('dialog', { name: 'Resume rentals-weekly?' });
    expect(dialog).toHaveTextContent('Pauses a budget made lift too; a step paused by hand stays paused.');
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(api.calls).toEqual([]);

    vi.spyOn(api, 'resume').mockRejectedValueOnce(new ApiError(503, 'unavailable', 'the engine is switching over.'));
    await user.click(within(row('rentals-weekly')).getByRole('button', { name: 'Resume rentals-weekly' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Resume rentals-weekly' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Couldn’t resume rentals-weekly: the engine is switching over. Nothing changed.');
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('rentals-weekly resumed.')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByText('rentals-weekly resumed.')).not.toBeInTheDocument();
  });

  it('shows a disabled pipeline with its own badge, offering Resume, which enables it, and Roll back', async () => {
    const pipelines = demoFixtures.pipelines.map((p) =>
      p.name === 'rentals-weekly' ? { ...p, state: 'disabled' as const } : p,
    );
    const api = new FakeApi({ ...demoFixtures, pipelines });
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    const rentals = row('rentals-weekly');
    expect(rentals).toHaveTextContent('Disabled');
    expect(within(rentals).queryByRole('button', { name: 'Disable rentals-weekly' })).not.toBeInTheDocument();
    expect(within(rentals).getByRole('button', { name: 'Roll back rentals-weekly' })).toBeInTheDocument();
    await user.click(within(rentals).getByRole('button', { name: 'Resume rentals-weekly' }));
    const dialog = screen.getByRole('dialog', { name: 'Resume rentals-weekly?' });
    expect(dialog).toHaveTextContent('Its triggers fire again from their next scheduled time');
    await user.click(within(dialog).getByRole('button', { name: 'Resume rentals-weekly' }));
    expect(await screen.findByText('rentals-weekly resumed.')).toBeInTheDocument();
    await waitFor(() => expect(row('rentals-weekly')).toHaveTextContent('Active'));
  });

  it('disables after a confirmation that opens on Cancel and states the consequence', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    const trigger = within(row('listings-daily')).getByRole('button', { name: 'Disable listings-daily' });
    await user.click(trigger);
    let dialog = screen.getByRole('dialog', { name: 'Disable listings-daily?' });
    expect(dialog).toHaveTextContent('scheduled times pass as skipped and are not caught up');
    expect(dialog).toHaveTextContent('Queued deliveries wait, pending, until you resume.');
    expect(dialog).toHaveTextContent('Runs in flight keep their leases and finish what they hold.');
    expect(dialog).toHaveTextContent('Resume undoes it.');
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
    expect(api.calls).toEqual([]);

    await user.click(trigger);
    dialog = screen.getByRole('dialog', { name: 'Disable listings-daily?' });
    await user.click(within(dialog).getByRole('button', { name: 'Disable listings-daily' }));
    expect(
      await screen.findByText(
        'listings-daily disabled: its triggers are off and its deliveries wait until you resume it.',
      ),
    ).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({ method: 'disable', args: ['listings-daily'] });
    await waitFor(() => expect(row('listings-daily')).toHaveTextContent('Disabled'));
    expect(within(row('listings-daily')).getByRole('button', { name: 'Resume listings-daily' })).toBeInTheDocument();
  });

  it('reports a refused disable', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'disable').mockRejectedValueOnce(new ApiError(409, 'conflict', 'the pipeline is being applied.'));
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    await user.click(within(row('photo-tagging')).getByRole('button', { name: 'Disable photo-tagging' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Disable photo-tagging' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Couldn’t disable photo-tagging: the pipeline is being applied. Nothing changed.',
    );
    expect(row('photo-tagging')).toHaveTextContent('Active');
  });

  it('rolls back to the previous revision by default, or to the one picked, after stating the consequence', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    const trigger = within(row('listings-daily')).getByRole('button', { name: 'Roll back listings-daily' });
    await user.click(trigger);
    const dialog = await screen.findByRole('dialog', { name: 'Roll back listings-daily?' });
    const group = within(dialog).getByRole('group', { name: 'Revision to reactivate in place of r14' });
    const radios = within(group).getAllByRole('radio');
    expect(radios.map((r) => r.closest('label')?.textContent)).toEqual([
      expect.stringMatching(/^r13 · applied .+ · 51c0e8d4b3$/),
      expect.stringMatching(/^r12 · applied .+ · c7e19a05f2geocode: switch to the BAN API$/),
    ]);
    expect(radios[0]).toBeChecked();
    expect(dialog).toHaveTextContent('Runs started from now on use r13.');
    expect(dialog).toHaveTextContent('Runs in flight finish on r14.');
    expect(dialog).toHaveTextContent('No new revision is created: r14 stays in the history, to roll forward to.');
    expect(within(dialog).getByRole('button', { name: 'Roll back to r13' })).toHaveFocus();

    await user.click(radios[1]!);
    expect(radios[1]).toBeChecked();
    expect(dialog).toHaveTextContent('Runs started from now on use r12.');
    await user.click(within(dialog).getByRole('button', { name: 'Roll back to r12' }));
    expect(
      await screen.findByText('listings-daily rolled back to r12: runs started from now on use it.'),
    ).toBeInTheDocument();
    expect(api.calls.at(-1)).toEqual({ method: 'rollback', args: ['listings-daily', { revision: 12 }] });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
    await waitFor(() => expect(row('listings-daily')).toHaveTextContent('r12'));
  });

  it('explains that a pipeline with one revision has nothing to roll back to', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    await user.click(within(row('photo-tagging')).getByRole('button', { name: 'Roll back photo-tagging' }));
    const dialog = await screen.findByRole('dialog', { name: 'Roll back photo-tagging?' });
    expect(dialog).toHaveTextContent(
      'photo-tagging has only its active revision, r2: there is nothing to roll back to. Applying a package creates a new one.',
    );
    expect(within(dialog).queryByRole('radio')).not.toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: 'Roll back' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toHaveFocus();
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(api.calls).toEqual([]);
  });

  it('reports a failed rollback, and revisions that could not be read with Retry', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'rollback').mockRejectedValueOnce(new ApiError(409, 'conflict', 'revision r13 is already active.'));
    const { user } = renderApp({ api, route: '/pipelines' });
    await screen.findByRole('table');
    await user.click(within(row('listings-daily')).getByRole('button', { name: 'Roll back listings-daily' }));
    await user.click(await screen.findByRole('button', { name: 'Roll back to r13' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Couldn’t roll back listings-daily: revision r13 is already active. Nothing changed.',
    );
    expect(row('listings-daily')).toHaveTextContent('r14');

    vi.spyOn(api, 'listRevisions').mockRejectedValueOnce(new ApiError(503, 'unavailable', 'database is locked'));
    await user.click(within(row('listings-daily')).getByRole('button', { name: 'Roll back listings-daily' }));
    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent(
        'Couldn’t read the revisions of listings-daily: database is locked',
      ),
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await user.click(within(screen.getByRole('alert')).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('dialog', { name: 'Roll back listings-daily?' })).toBeInTheDocument();
  });

  it('teaches when there are no pipelines, and retries after an error', async () => {
    renderApp({ api: new FakeApi({ ...demoFixtures, pipelines: [] }), route: '/pipelines' });
    expect(await screen.findByText('No pipelines yet')).toBeInTheDocument();
    expect(screen.getByText('millstream run ./pipelines/listings-daily')).toBeInTheDocument();
  });

  it('shows an error with Retry', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'listPipelines').mockRejectedValue(new ApiError(500, 'internal', 'database is locked'));
    const { user } = renderApp({ api, route: '/pipelines' });
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The pipelines could not be loaded.');
    vi.mocked(api.listPipelines).mockResolvedValue(demoFixtures.pipelines);
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('table')).toBeInTheDocument();
  });

  it('speaks French with its typography', async () => {
    const { user } = renderApp({ route: '/pipelines', language: 'fr' });
    await screen.findByRole('table');
    expect(screen.getByText(/4 pipelines/)).toHaveTextContent('4 pipelines · 1 en cours · 3 lettres mortes ouvertes');
    await user.click(within(row('listings-daily')).getByRole('button', { name: 'Mettre en pause listings-daily' }));
    expect(screen.getByRole('dialog', { name: 'Mettre listings-daily en pause ?' })).toBeInTheDocument();
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Annuler' }));
    await user.click(within(row('listings-daily')).getByRole('button', { name: 'Désactiver listings-daily' }));
    expect(screen.getByRole('dialog', { name: 'Désactiver listings-daily ?' })).toBeInTheDocument();
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Annuler' }));
    await user.click(
      within(row('listings-daily')).getByRole('button', { name: 'Restaurer une révision de listings-daily' }),
    );
    const dialog = await screen.findByRole('dialog', { name: 'Restaurer une révision de listings-daily ?' });
    expect(dialog).toHaveTextContent('Les exécutions lancées désormais utilisent r13.');
  });
});
