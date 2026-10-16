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
  });
});
