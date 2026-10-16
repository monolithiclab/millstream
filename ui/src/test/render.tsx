import { render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useEffect } from 'react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { App, AppRoutes } from '../App';
import type { Api } from '../api/api';
import { FakeApi } from '../api/fake';
import type { Language } from '../i18n';
import { fakeEventSource } from '../live/fakeEventSource';

let location = '';

function LocationProbe() {
  const current = useLocation();
  useEffect(() => {
    location = current.pathname + current.search;
  });
  return null;
}

// The whole app on a FakeApi and FakeEventSource, at a route.
export function renderApp({
  route = '/pipelines',
  api = new FakeApi(),
  language = 'en',
}: { route?: string; api?: Api; language?: Language } = {}) {
  const user = userEvent.setup();
  const result = render(
    <MemoryRouter initialEntries={[route]}>
      <App api={api} eventSource={fakeEventSource} language={language}>
        <AppRoutes />
        <LocationProbe />
      </App>
    </MemoryRouter>,
  );
  return { ...result, user, api, location: () => location };
}
