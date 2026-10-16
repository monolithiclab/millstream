import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
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

// Runs one API read when its key changes; screens of Tasks 4 and 5 may replace it with a caching library.
export function useApiQuery<T>(key: string, read: (api: Api) => Promise<T>): QueryState<T> {
  const api = useApi();
  const [state, setState] = useState<{ key: string; value: QueryState<T> }>({ key, value: { status: 'loading' } });
  useEffect(() => {
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
    // read is a new closure each render; key names what it reads.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, key]);
  return state.key === key ? state.value : { status: 'loading' };
}
