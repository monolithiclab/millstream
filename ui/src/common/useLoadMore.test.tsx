import { act, renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { appendById, useLoadMore } from './useLoadMore';

interface Page {
  items: { id: string }[];
}

function setup(read: (cursor: string) => Promise<Page>) {
  let data: Page = { items: [{ id: 'a' }] };
  const mutate = vi.fn((change: (d: Page) => Page) => {
    data = change(data);
  });
  const hook = renderHook(
    ({ key }) =>
      useLoadMore<Page, string>({
        key,
        mutate,
        read,
        merge: (current, page) => ({ items: appendById(current.items, page.items) }),
      }),
    { initialProps: { key: 'k1' } },
  );
  return { ...hook, data: () => data };
}

describe('useLoadMore', () => {
  it('merges the next page, deduplicated by id', async () => {
    const { result, data } = setup(() => Promise.resolve({ items: [{ id: 'a' }, { id: 'b' }] }));
    await act(() => result.current.loadMore('c'));
    expect(data().items.map((i) => i.id)).toEqual(['a', 'b']);
    expect(result.current).toMatchObject({ loadingMore: false, moreError: null });
  });

  it('drops a page that arrives after the key changed, and keeps errors to their key', async () => {
    let resolve: (page: Page) => void = () => undefined;
    const { result, rerender, data } = setup(() => new Promise((r) => (resolve = r)));
    let pending: Promise<void> = Promise.resolve();
    act(() => {
      pending = result.current.loadMore('c');
    });
    expect(result.current.loadingMore).toBe(true);
    rerender({ key: 'k2' });
    expect(result.current.loadingMore).toBe(false);
    await act(async () => {
      resolve({ items: [{ id: 'stale' }] });
      await pending;
    });
    expect(data().items.map((i) => i.id)).toEqual(['a']);
  });

  it('reports a failed page', async () => {
    const { result } = setup(() => Promise.reject(new Error('cursor expired')));
    await act(() => result.current.loadMore('c'));
    expect(result.current.moreError).toBe('cursor expired');
  });
});
