import { lazy, Suspense, type ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import type { Api } from './api/api';
import { ApiProvider } from './api/context';
import type { Language } from './i18n';
import type { EventSourceFactory } from './live/useLiveStream';
import { PreferencesProvider, type Theme } from './preferences';
import { InboxScreen } from './screens/inbox/InboxScreen';
import { InsightsScreen } from './screens/insights/InsightsScreen';
import { LessonsScreen } from './screens/lessons/LessonsScreen';
import { LogsScreen } from './screens/logs/LogsScreen';
import { NodesScreen } from './screens/nodes/NodesScreen';
import { NotFound } from './screens/NotFound';
import { DeadLettersScreen } from './screens/deadLetters/DeadLettersScreen';
import { GraphScreen } from './screens/graph/GraphScreen';
import { ItemScreen } from './screens/item/ItemScreen';
import { ItemsScreen } from './screens/items/ItemsScreen';
import { PipelinesScreen } from './screens/pipelines/PipelinesScreen';
import { RunScreen } from './screens/runs/RunScreen';
import { RunsScreen } from './screens/runs/RunsScreen';
import { Loading } from './common/States';
import { routePattern, screens, type Screen, type ScreenId } from './routes';
import { AppShell } from './shell/AppShell';

// uPlot and its stylesheet load with the metrics screen, not with every screen.
const MetricsScreen = lazy(() => import('./screens/metrics/MetricsScreen').then((m) => ({ default: m.MetricsScreen })));

// Every screen's element.
const built: Record<ScreenId, () => ReactNode> = {
  pipelines: () => <PipelinesScreen />,
  graph: () => <GraphScreen />,
  deadLetters: () => <DeadLettersScreen />,
  inbox: () => <InboxScreen />,
  runs: () => <RunsScreen />,
  items: () => <ItemsScreen />,
  insights: () => <InsightsScreen />,
  lessons: () => <LessonsScreen />,
  nodes: () => <NodesScreen />,
  logs: () => <LogsScreen />,
  metrics: () => (
    <Suspense fallback={<Loading />}>
      <MetricsScreen />
    </Suspense>
  ),
};

const elementFor = (screen: Screen) => built[screen.id]();

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Navigate to="/pipelines" replace />} />
        {screens.map((screen) => (
          <Route key={screen.id} path={routePattern(screen)} element={elementFor(screen)} />
        ))}
        {/* Review notifications link to /inbox/<inbox>/<deliveryId> (spec section 8). */}
        <Route path="inbox/:inbox/:delivery?" element={<InboxScreen />} />
        <Route path="p/:pipeline/runs/:run" element={<RunScreen />} />
        <Route path="p/:pipeline/items/:item" element={<ItemScreen />} />
        <Route path="p/:pipeline" element={<Navigate to="graph" replace />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}

// Everything but the router, so main.tsx uses the browser's and tests a MemoryRouter.
export function App({
  api,
  eventSource,
  language,
  theme,
  children = <AppRoutes />,
}: {
  api: Api;
  eventSource: EventSourceFactory;
  language?: Language | undefined;
  theme?: Theme | undefined;
  children?: ReactNode;
}) {
  return (
    <PreferencesProvider language={language} theme={theme}>
      <ApiProvider api={api} eventSource={eventSource}>
        {children}
      </ApiProvider>
    </PreferencesProvider>
  );
}
