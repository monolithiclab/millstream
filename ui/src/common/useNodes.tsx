import { createContext, useContext, type ReactNode } from 'react';
import type { Nodes } from '../api/schema';
import { useResource, type Resource } from './useResource';

const NodesContext = createContext<Resource<Nodes> | null>(null);

// The one nodes read of the app: the shell's navigation shows Nodes once a node is listed, and the Nodes screen
// reads it again as it polls, so the entry follows what the screen shows.
export function NodesProvider({ children }: { children: ReactNode }) {
  const nodes = useResource('nodes', (api) => api.listNodes());
  return <NodesContext.Provider value={nodes}>{children}</NodesContext.Provider>;
}

export function useNodes(): Resource<Nodes> {
  const value = useContext(NodesContext);
  if (value === null) throw new Error('useNodes outside NodesProvider');
  return value;
}
