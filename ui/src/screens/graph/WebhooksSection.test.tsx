import { screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { mockMatchMedia } from '../../test/flow';
import { renderApp } from '../../test/render';

beforeEach(() => mockMatchMedia(() => false));

const URL = `${window.location.origin}/hooks/listings-daily/listing`;

async function webhooks() {
  return screen.findByRole('region', { name: 'Webhooks' });
}

describe('webhooks section', () => {
  it('lists each webhook trigger with its full URL to copy and its rate', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/graph' });
    const section = await webhooks();
    expect(within(section).getByRole('heading', { name: 'listing' })).toBeInTheDocument();
    expect(within(section).getByText(URL)).toBeInTheDocument();
    expect(within(section).getByText('Up to 60/min')).toBeInTheDocument();
    expect(section).toHaveTextContent('as a bearer token in the Authorization header, or as the ?token= query');
    expect(section).toHaveTextContent('millstream apply shows each new token once.');
    // The cron trigger has no URL.
    expect(within(section).queryByRole('heading', { name: 'daily' })).not.toBeInTheDocument();

    await user.click(within(section).getByRole('button', { name: 'Copy URL' }));
    expect(await navigator.clipboard.readText()).toBe(URL);
    expect(within(section).getByRole('button', { name: 'Copied' })).toBeInTheDocument();
  });

  it('rotates a token after the consequences, then shows the new one once', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ api, route: '/p/listings-daily/graph' });
    const section = await webhooks();
    const rotate = within(section).getByRole('button', { name: 'Rotate token of listing' });

    // Cancel changes nothing.
    await user.click(rotate);
    let confirm = screen.getByRole('dialog', { name: 'Rotate the token of listing?' });
    expect(within(confirm).getByRole('button', { name: 'Cancel' })).toHaveFocus();
    await user.click(within(confirm).getByRole('button', { name: 'Cancel' }));
    expect(api.calls.some((c) => c.method === 'rotateTriggerToken')).toBe(false);

    await user.click(rotate);
    confirm = screen.getByRole('dialog', { name: 'Rotate the token of listing?' });
    expect(confirm).toHaveTextContent('A new token is generated for listing.');
    expect(confirm).toHaveTextContent('The current token stops working immediately');
    expect(confirm).toHaveTextContent('Every service that calls this webhook must be updated');
    expect(confirm).toHaveTextContent('The new token is shown only once');
    await user.click(within(confirm).getByRole('button', { name: 'Rotate token' }));

    const dialog = await screen.findByRole('dialog', { name: 'New token for listing' });
    expect(api.calls.filter((c) => c.method === 'rotateTriggerToken')).toEqual([
      { method: 'rotateTriggerToken', args: ['listings-daily', 'listing'] },
    ]);
    expect(within(dialog).getByTestId('trigger-token')).toHaveTextContent('mswh_listing_0001');
    expect(dialog).toHaveTextContent('Store it now: it will not be shown again.');
    expect(dialog).toHaveTextContent(`POST ${URL}`);
    expect(dialog).toHaveTextContent('Authorization: Bearer mswh_listing_0001');
    const copy = within(dialog).getByRole('button', { name: 'Copy token' });
    expect(copy).toHaveFocus();
    await user.click(copy);
    expect(await navigator.clipboard.readText()).toBe('mswh_listing_0001');

    await user.click(within(dialog).getByRole('button', { name: 'Done' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(rotate).toHaveFocus();
    expect(screen.queryByText('mswh_listing_0001')).not.toBeInTheDocument();
  });

  it('says when the rotation failed, the current token still working, with Retry', async () => {
    const api = new FakeApi();
    vi.spyOn(api, 'rotateTriggerToken').mockRejectedValueOnce(new ApiError(500, 'internal', 'database is locked.'));
    const { user } = renderApp({ api, route: '/p/listings-daily/graph' });
    const section = await webhooks();
    await user.click(within(section).getByRole('button', { name: 'Rotate token of listing' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Rotate token' }));

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(
      'Couldn’t rotate the token of listing: database is locked. The current token still works.',
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await user.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('dialog', { name: 'New token for listing' })).toBeInTheDocument();
  });

  it('is left out for a pipeline without webhook triggers', async () => {
    const api = new FakeApi({
      ...demoFixtures,
      pipelines: demoFixtures.pipelines.map((p) => ({
        ...p,
        triggers: p.triggers.filter((t) => t.kind !== 'webhook'),
      })),
    });
    renderApp({ api, route: '/p/listings-daily/graph' });
    await screen.findByRole('button', { name: /^fetch,/ });
    expect(screen.queryByRole('region', { name: 'Webhooks' })).not.toBeInTheDocument();
  });

  it('speaks French', async () => {
    renderApp({ route: '/p/listings-daily/graph', language: 'fr' });
    const section = await webhooks();
    expect(within(section).getByText('Jusqu’à 60/min')).toBeInTheDocument();
    expect(within(section).getByRole('button', { name: 'Renouveler le jeton de listing' })).toBeInTheDocument();
  });
});
