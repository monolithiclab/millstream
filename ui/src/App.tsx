import type { ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import type { Api } from './api/api';
import { ApiProvider } from './api/context';
import type { Language } from './i18n';
import type { EventSourceFactory } from './live/useLiveStream';
import { PreferencesProvider, type Theme } from './preferences';
import { InboxScreen } from './screens/inbox/InboxScreen';
import { NotFound } from './screens/NotFound';
import { Placeholder } from './screens/Placeholder';
import { RunScreen } from './screens/runs/RunScreen';
import { RunsScreen } from './screens/runs/RunsScreen';
import { routePattern, screens, type Screen, type ScreenId } from './routes';
import { AppShell } from './shell/AppShell';

// The built screens; the others are placeholders until their task lands.
const built: Partial<Record<ScreenId, () => ReactNode>> = {
  inbox: () => <InboxScreen />,
  runs: () => <RunsScreen />,
};

const elementFor = (screen: Screen) => built[screen.id]?.() ?? <Placeholder screen={screen} />;

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
