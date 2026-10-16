import { screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { Item } from '../../api/schema';
import { renderApp } from '../../test/render';

const route = '/p/listings-daily/items';
const listed = () => within(screen.getByRole('list', { name: 'Items, oldest first' })).getAllByRole('link');

function item(id: string, payload: object = {}): Item {
  return {
    id,
    pipeline: 'listings-daily',
    runId: 'run_0600',
    stream: 'scored',
    step: 'score',
    key: id,
    parents: [],
    generation: 1,
    ts: '2026-10-16T06:00:00Z',
    payload,
    artifacts: [],
  };
}

describe('items list', () => {
  it('lists the items with a skeleton first, each row opening the item', async () => {
    const { user, location } = renderApp({ route });
    expect(screen.getByRole('heading', { level: 1, name: 'Streams and items' })).toBeInTheDocument();
    expect(screen.getByTestId('loading')).toBeInTheDocument();
    const first = demoFixtures.items.find((i) => i.pipeline === 'listings-daily')!;
    await waitFor(() => expect(listed().length).toBeGreaterThan(0));
    await user.click(listed()[0]!);
    expect(location()).toBe(`/p/listings-daily/items/${first.id}`);
  });

  it('picks a stream from the pipeline’s streams, with their counts, and keeps it in the URL', async () => {
    const api = new FakeApi();
    const listItems = vi.spyOn(api, 'listItems');
    const { user, location } = renderApp({ route, api });
    const picker = await screen.findByRole('combobox', { name: 'Stream' });
    await screen.findByRole('option', { name: 'scored (402)' });
    await user.selectOptions(picker, 'scored');
    expect(location()).toBe(`${route}?stream=scored`);
    await waitFor(() => expect(listItems).toHaveBeenLastCalledWith('listings-daily', { stream: 'scored', limit: 50 }));
  });

  it('applies run and where from the URL and the form, and clears them', async () => {
    const api = new FakeApi();
    const listItems = vi.spyOn(api, 'listItems');
    const { user, location } = renderApp({ route: `${route}?run=run_0600`, api });
    expect(await screen.findByRole('textbox', { name: 'Run' })).toHaveValue('run_0600');
    await user.type(screen.getByRole('textbox', { name: 'Where (CEL)' }), 'item.priceEur < 300000{Enter}');
    expect(location()).toBe(`${route}?run=run_0600&where=item.priceEur+%3C+300000`);
    await waitFor(() =>
      expect(listItems).toHaveBeenLastCalledWith('listings-daily', {
        run: 'run_0600',
        where: 'item.priceEur < 300000',
        limit: 50,
      }),
    );
    await user.click(screen.getByRole('button', { name: 'Clear filters' }));
    expect(location()).toBe(route);
    expect(screen.getByRole('textbox', { name: 'Where (CEL)' })).toHaveValue('');
  });

  it('says a bad expression at the field, with the server’s message', async () => {
    const api = new FakeApi();
    const real = api.listItems;
    api.listItems = (p, query = {}) =>
      query.where === 'item.price >'
        ? Promise.reject(new ApiError(400, 'invalid', 'where: Syntax error: mismatched input'))
        : real(p, query);
    renderApp({ route: `${route}?where=${encodeURIComponent('item.price >')}`, api });
    const where = await screen.findByRole('textbox', { name: 'Where (CEL)' });
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('This expression doesn’t work: where: Syntax error: mismatched input');
    expect(where).toHaveAttribute('aria-invalid', 'true');
    expect(where).toHaveAccessibleDescription(/Syntax error/);
    // No Retry: retrying the same expression fails the same way.
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument();
  });

  it('shows another failure with Retry', async () => {
    const api = new FakeApi();
    const real = api.listItems;
    let fail = true;
    api.listItems = (p, query) => (fail ? Promise.reject(new Error('server down')) : real(p, query));
    const { user } = renderApp({ route, api });
    expect(await screen.findByRole('alert')).toHaveTextContent('server down');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(listed().length).toBeGreaterThan(0));
  });

  it('pages with the cursor, appending without duplicates', async () => {
    const api = new FakeApi();
    api.listItems = (_p, { cursor } = {}) =>
      Promise.resolve(
        cursor === undefined
          ? { items: [item('it_a', { title: 'A' }), item('it_b', { title: 'B' })], next: 'c2' }
          : { items: [item('it_b', { title: 'B' }), item('it_c', { title: 'C' })] },
      );
    const { user } = renderApp({ route, api });
    await waitFor(() => expect(listed()).toHaveLength(2));
    expect(screen.getByText('2 items shown')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Show more items' }));
    await waitFor(() =>
      expect(listed().map((a) => a.textContent)).toEqual([...'ABC'].map((t) => expect.stringContaining(t))),
    );
    expect(screen.queryByRole('button', { name: 'Show more items' })).not.toBeInTheDocument();
  });

  it('reads on past a filtered page that came back empty but has more to read', async () => {
    const api = new FakeApi();
    api.listItems = (_p, { cursor } = {}) =>
      Promise.resolve(cursor === undefined ? { items: [], next: 'c5000' } : { items: [item('it_x', { title: 'X' })] });
    const { user } = renderApp({ route: `${route}?where=item.rare`, api });
    expect(await screen.findByText('No match in the items read so far')).toBeInTheDocument();
    expect(screen.getByText(/more may match further on/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Search further' }));
    await waitFor(() => expect(listed()).toHaveLength(1));
    expect(screen.getByText('1 item shown')).toBeInTheDocument();
  });

  it('marks an expired or erased item in its row, named by its key or id', async () => {
    const api = new FakeApi();
    api.listItems = () =>
      Promise.resolve({
        items: [
          { ...item('it_a', { title: 'A' }), payload: null, key: 'k-a', tombstone: 'expired' },
          { ...item('it_b'), payload: null, key: '', tombstone: 'erased' },
          item('it_c', { title: 'C' }),
        ],
      });
    renderApp({ route, api });
    await waitFor(() => expect(listed()).toHaveLength(3));
    const [expired, erased, kept] = listed();
    expect(within(expired!).getByText('k-a')).toBeInTheDocument();
    expect(within(expired!).getByText('Expired')).toBeInTheDocument();
    expect(within(erased!).getByText('Erased')).toBeInTheDocument();
    expect(erased).toHaveTextContent('it_b');
    expect(kept).not.toHaveTextContent(/Expired|Erased/);
  });

  it('says why the list is empty, filtered or not', async () => {
    const api = new FakeApi();
    api.listItems = () => Promise.resolve({ items: [] });
    renderApp({ route: '/p/listings-daily/items?stream=pdf&where=false', api });
    expect(await screen.findByText('No item matches these filters')).toBeInTheDocument();
  });

  it('pages the in-memory API by cursor', async () => {
    const api = new FakeApi();
    const first = await api.listItems('listings-daily', { limit: 2 });
    expect(first.items).toHaveLength(2);
    const second = await api.listItems('listings-daily', { limit: 2, cursor: first.next! });
    expect(second.items[0]?.id).not.toBe(first.items[0]?.id);
  });

  it('is in French', async () => {
    renderApp({ route, language: 'fr' });
    expect(await screen.findByRole('combobox', { name: 'Flux' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Appliquer' })).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByRole('list', { name: 'Items, du plus ancien au plus récent' })).toBeInTheDocument(),
    );
  });
});
