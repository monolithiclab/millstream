import { createContext, useContext, useMemo, type ReactNode } from 'react';
import type { Api } from './api';
import { LiveHub } from '../live/hub';
import type { EventSourceFactory } from '../live/useLiveStream';

interface ApiProps {
  api: Api;
  eventSource: EventSourceFactory;
  // The api is ?demo's in-memory one.
  demo?: boolean | undefined;
}

interface ApiContextValue {
  api: Api;
  live: LiveHub;
  demo: boolean;
}

const ApiContext = createContext<ApiContextValue | null>(null);

// The API client and the live connections every screen shares.
export function ApiProvider({ api, eventSource, demo = false, children }: ApiProps & { children: ReactNode }) {
  const live = useMemo(() => new LiveHub(eventSource), [eventSource]);
  const value = useMemo(() => ({ api, live, demo }), [api, live, demo]);
  return <ApiContext.Provider value={value}>{children}</ApiContext.Provider>;
}

function useApiContext(): ApiContextValue {
  const value = useContext(ApiContext);
  if (value === null) throw new Error('useApi outside ApiProvider');
  return value;
}

export const useApi = () => useApiContext().api;
export const useLiveHub = () => useApiContext().live;
export const useDemo = () => useApiContext().demo;
