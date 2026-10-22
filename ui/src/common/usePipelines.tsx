import { createContext, useContext, useEffect, type ReactNode } from 'react';
import type { Pipeline } from '../api/schema';
import { useResource, type Resource } from './useResource';

const PipelinesContext = createContext<Resource<Pipeline[]> | null>(null);

// The one pipeline list of the app (the switcher, the palette, every pipeline filter): read again in the background
// when the window gets focus, so a pipeline applied meanwhile shows up everywhere at once.
export function PipelinesProvider({ children }: { children: ReactNode }) {
  const pipelines = useResource('pipelines', (api) => api.listPipelines());
  const { refresh } = pipelines;
  useEffect(() => {
    window.addEventListener('focus', refresh);
    return () => window.removeEventListener('focus', refresh);
  }, [refresh]);
  return <PipelinesContext.Provider value={pipelines}>{children}</PipelinesContext.Provider>;
}

export function usePipelines(): Resource<Pipeline[]> {
  const value = useContext(PipelinesContext);
  if (value === null) throw new Error('usePipelines outside PipelinesProvider');
  return value;
}
