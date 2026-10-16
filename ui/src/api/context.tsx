import { createContext, useContext, useMemo, type ReactNode } from 'react';
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
