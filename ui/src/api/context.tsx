import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { Api } from './api';
import type { EventSourceFactory } from '../live/useLiveStream';

interface ApiContextValue {
  api: Api;
  eventSource: EventSourceFactory;
}

const ApiContext = createContext<ApiContextValue | null>(null);

export function ApiProvider({ api, eventSource, children }: ApiContextValue & { children: ReactNode }) {
  const value = useMemo(() => ({ api, eventSource }), [api, eventSource]);
  return <ApiContext.Provider value={value}>{children}</ApiContext.Provider>;
}

function useApiContext(): ApiContextValue {
  const value = useContext(ApiContext);
  if (value === null) throw new Error('useApi outside ApiProvider');
  return value;
}

export const useApi = () => useApiContext().api;
export const useEventSourceFactory = () => useApiContext().eventSource;

export type QueryState<T> = { status: 'loading' } | { status: 'ready'; data: T } | { status: 'error'; error: Error };

export type Query<T> = QueryState<T> & { reload: () => void };

// Runs one API read when its key changes; reload reads it again, keeping the data on screen until the new answer
// arrives, so a retry, a live event or an action's refresh never flashes the loading state. A null key reads nothing
// and stays loading.
export function useApiQuery<T>(key: string | null, read: (api: Api) => Promise<T>): Query<T> {
  const api = useApi();
  const [version, setVersion] = useState(0);
  const [state, setState] = useState<{ key: string | null; value: QueryState<T> }>({
    key,
    value: { status: 'loading' },
  });
  useEffect(() => {
    if (key === null) return;
    let current = true;
    read(api).then(
      (data) => current && setState({ key, value: { status: 'ready', data } }),
      (error: unknown) =>
        current &&
        setState({ key, value: { status: 'error', error: error instanceof Error ? error : new Error(String(error)) } }),
    );
    return () => {
      current = false;
    };
    // read is a new closure each render; key names what it reads and version asks again.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, key, version]);
  const reload = useCallback(() => setVersion((v) => v + 1), []);
  return { ...(state.key === key ? state.value : { status: 'loading' }), reload };
}
