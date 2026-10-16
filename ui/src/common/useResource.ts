import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Api } from '../api/api';
import { useApi } from '../api/context';
import { useLatest } from '../useLatest';

export type QueryState<T> = { status: 'loading' } | { status: 'ready'; data: T } | { status: 'error'; error: Error };

// A read's state and how to read it again after an error: show loading and read again.
export type Query<T> = QueryState<T> & { reload: () => void };

export type Resource<T> = Query<T> & {
  // Read again in the background (a live event, an action's outcome): the data stays on screen until the answer,
  // so nothing flashes, and a failed refresh keeps it.
  refresh: () => void;
  // Changes the data in place (an optimistic edit), when there is data for the current key.
  mutate: (change: (data: T) => T) => void;
};

const loading = { status: 'loading' } as const;

const asError = (error: unknown) => (error instanceof Error ? error : new Error(String(error)));

// One API read per key: read again when the key changes, on reload or on refresh. A null key reads nothing and
// stays loading (a read that waits on another).
export function useResource<T>(key: string | null, read: (api: Api) => Promise<T>): Resource<T> {
  const api = useApi();
  const readRef = useLatest(read);
  const [state, setState] = useState<{ key: string | null; value: QueryState<T> }>({ key, value: loading });
  const [nonce, setNonce] = useState(0);
  const silent = useRef(false);

  useEffect(() => {
    if (key === null) return;
    let current = true;
    const quiet = silent.current;
    silent.current = false;
    readRef.current(api).then(
      (data) => current && setState({ key, value: { status: 'ready', data } }),
      (error: unknown) => {
        if (!current) return;
        setState((previous) =>
          quiet && previous.key === key && previous.value.status === 'ready'
            ? previous
            : { key, value: { status: 'error', error: asError(error) } },
        );
      },
    );
    return () => {
      current = false;
    };
  }, [api, key, nonce, readRef]);

  const reload = useCallback(() => {
    setState((previous) => ({ key: previous.key, value: loading }));
    setNonce((n) => n + 1);
  }, []);
  const refresh = useCallback(() => {
    silent.current = true;
    setNonce((n) => n + 1);
  }, []);
  const mutate = useCallback(
    (change: (data: T) => T) =>
      setState((s) =>
        s.key === key && s.value.status === 'ready'
          ? { key, value: { status: 'ready', data: change(s.value.data) } }
          : s,
      ),
    [key],
  );

  // The same object until something changes, so it can be a hook dependency.
  const value = state.key === key ? state.value : loading;
  return useMemo(() => ({ ...value, reload, refresh, mutate }), [value, reload, refresh, mutate]);
}
