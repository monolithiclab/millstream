import { useCallback, useEffect, useRef, useState } from 'react';
import type { Api } from '../../api/api';
import { useApi, type QueryState } from '../../api/context';
import { useLatest } from '../../useLatest';

export interface Resource<T> {
  state: QueryState<T>;
  // After an error: show loading and read again.
  reload: () => void;
  // Read again in the background (a live event, a decision): the current data stays until the answer, and a failed
  // refresh keeps it.
  refresh: () => void;
  // Changes the data in place (an optimistic edit), when there is data for the current key.
  mutate: (change: (data: T) => T) => void;
}

const loading = { status: 'loading' } as const;

// One API read per key, with retry and silent refresh; a null key reads nothing and stays loading.
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
            : { key, value: { status: 'error', error: error instanceof Error ? error : new Error(String(error)) } },
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

  return { state: state.key === key ? state.value : loading, reload, refresh, mutate };
}
