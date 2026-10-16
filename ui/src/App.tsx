import type { ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import type { Api } from './api/api';
import { ApiProvider } from './api/context';
import type { Language } from './i18n';
import type { EventSourceFactory } from './live/useLiveStream';
import { PreferencesProvider, type Theme } from './preferences';
import { NotFound } from './screens/NotFound';
import { Placeholder } from './screens/Placeholder';
import { ItemScreen } from './screens/item/ItemScreen';
import { PipelinesScreen } from './screens/pipelines/PipelinesScreen';
import { routePattern, screens, type ScreenId } from './routes';
import { AppShell } from './shell/AppShell';

// The screens built so far; the others show the placeholder.
const built: Partial<Record<ScreenId, ReactNode>> = {
  pipelines: <PipelinesScreen />,
};

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Navigate to="/pipelines" replace />} />
        {screens.map((screen) => (
          <Route
            key={screen.id}
            path={routePattern(screen)}
            element={built[screen.id] ?? <Placeholder screen={screen} />}
          />
        ))}
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
