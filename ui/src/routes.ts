// Every screen of the UI, with its URL and its g-shortcut (canvas interaction notes). Workspace screens stand alone;
// pipeline screens live under /p/:pipeline/.
export type ScreenId =
  | 'pipelines'
  | 'inbox'
  | 'nodes'
  | 'metrics'
  | 'logs'
  | 'graph'
  | 'runs'
  | 'items'
  | 'deadLetters'
  | 'insights'
  | 'improve'
  | 'lessons';

// Each label is written out in screens below: the i18n test finds used keys as quoted literals.
export type NavKey = `nav.${ScreenId}`;

export interface Screen {
  id: ScreenId;
  scope: 'workspace' | 'pipeline';
  segment: string;
  label: NavKey;
  // The key pressed after g.
  goKey?: string;
}

export const screens: Screen[] = [
  { id: 'pipelines', scope: 'workspace', segment: 'pipelines', label: 'nav.pipelines', goKey: 'p' },
  { id: 'inbox', scope: 'workspace', segment: 'inbox', label: 'nav.inbox', goKey: 'i' },
  { id: 'nodes', scope: 'workspace', segment: 'nodes', label: 'nav.nodes', goKey: 'n' },
  { id: 'metrics', scope: 'workspace', segment: 'metrics', label: 'nav.metrics', goKey: 'm' },
  { id: 'logs', scope: 'workspace', segment: 'logs', label: 'nav.logs', goKey: 'l' },
  { id: 'graph', scope: 'pipeline', segment: 'graph', label: 'nav.graph', goKey: 'g' },
  { id: 'runs', scope: 'pipeline', segment: 'runs', label: 'nav.runs', goKey: 'r' },
  { id: 'items', scope: 'pipeline', segment: 'items', label: 'nav.items' },
  { id: 'deadLetters', scope: 'pipeline', segment: 'dead-letters', label: 'nav.deadLetters', goKey: 'd' },
  { id: 'insights', scope: 'pipeline', segment: 'insights', label: 'nav.insights' },
  { id: 'improve', scope: 'pipeline', segment: 'improve', label: 'nav.improve' },
  { id: 'lessons', scope: 'pipeline', segment: 'lessons', label: 'nav.lessons' },
];

// The router's path pattern for a screen.
export function routePattern(screen: Screen): string {
  return screen.scope === 'workspace' ? screen.segment : `p/:pipeline/${screen.segment}`;
}

// The URL of a pipeline's screen, its graph by default.
export function pipelinePath(pipeline: string, segment = 'graph'): string {
  return `/p/${encodeURIComponent(pipeline)}/${segment}`;
}

// The URLs of one item and one run of a pipeline.
export const itemPath = (pipeline: string, item: string) => pipelinePath(pipeline, `items/${encodeURIComponent(item)}`);
export const runPath = (pipeline: string, run: string) => pipelinePath(pipeline, `runs/${encodeURIComponent(run)}`);
// A proposal of the pipeline's (spec 11.3), in the Improve view.
export const proposalPath = (pipeline: string, proposal: string) =>
  pipelinePath(pipeline, `improve/${encodeURIComponent(proposal)}`);

// The URL of a screen; a pipeline screen without a pipeline falls back to the pipelines list.
export function screenPath(screen: Screen, pipeline: string | undefined): string {
  if (screen.scope === 'workspace') return `/${screen.segment}`;
  return pipeline === undefined ? '/pipelines' : pipelinePath(pipeline, screen.segment);
}
