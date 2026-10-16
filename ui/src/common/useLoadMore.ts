import { useCallback, useState } from 'react';
import { useLatest } from '../useLatest';
import { errorMessage } from './Notice';

// The next page of a paged list, merged into the list a useResource holds: `read` fetches the page after `cursor`,
// `merge` folds it in (deduplicating what an earlier page already showed). A page that arrives after the key changed
// (a new filter) belongs to the old list and is dropped; the busy flag and the error are the current key's.
export function useLoadMore<T, C>({
  key,
  mutate,
  read,
  merge,
}: {
  key: string;
  mutate: (change: (data: T) => T) => void;
  read: (cursor: C) => Promise<T>;
  merge: (current: T, page: T) => T;
}) {
  const latest = useLatest({ key, read, merge, mutate });
  const [state, setState] = useState<{ key: string; loading: boolean; error: string | null }>({
    key,
    loading: false,
    error: null,
  });

  const loadMore = useCallback(
    async (cursor: C) => {
      const asked = latest.current.key;
      const settle = (error: string | null) =>
        setState((s) => (s.key === asked ? { key: asked, loading: false, error } : s));
      setState({ key: asked, loading: true, error: null });
      try {
        const page = await latest.current.read(cursor);
        if (latest.current.key !== asked) return;
        latest.current.mutate((current) => latest.current.merge(current, page));
        settle(null);
      } catch (error) {
        settle(errorMessage(error));
      }
    },
    [latest],
  );

  const current = state.key === key ? state : { loading: false, error: null };
  return { loadMore, loadingMore: current.loading, moreError: current.error };
}

// Appends a page of records by id, keeping the first copy of each: a record that moved between pages (a run that
// changed state while paging) is shown once.
export function appendById<R extends { id: string }>(current: R[], page: R[]): R[] {
  const known = new Set(current.map((r) => r.id));
  return [...current, ...page.filter((r) => !known.has(r.id))];
}
