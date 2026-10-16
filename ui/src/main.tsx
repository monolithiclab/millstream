import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './App';
import type { Api } from './api/api';
import { HttpApi } from './api/http';
import { browserEventSource, type EventSourceFactory } from './live/useLiveStream';
import { applyTheme, initialTheme } from './preferences';
import { DEMO_KEY, readStored, writeStored } from './storage';
import './styles/index.css';

// ?demo runs the UI on the in-memory FakeApi, for the end-to-end smoke test and for looking around without a
// server; it lasts for the browser tab, marked by the shell's "Demo data" badge, until its "Exit demo".
const query = new URLSearchParams(window.location.search).get('demo');
const demoMode = query ?? readStored(DEMO_KEY, 'sessionStorage');
if (demoMode !== null) writeStored(DEMO_KEY, demoMode, 'sessionStorage');

// Before the first paint, so the stored theme does not flash.
applyTheme(initialTheme());

const root = document.getElementById('root');
if (root === null) throw new Error('#root missing from index.html');

function render(api: Api, eventSource: EventSourceFactory) {
  createRoot(root as HTMLElement).render(
    <StrictMode>
      <BrowserRouter>
        <App api={api} eventSource={eventSource} demo={demoMode !== null} />
      </BrowserRouter>
    </StrictMode>,
  );
}

if (demoMode === null) {
  render(new HttpApi(), browserEventSource);
} else {
  void import('./demo').then(({ demoBackend }) => {
    const { api, eventSource } = demoBackend(demoMode);
    render(api, eventSource);
  });
}
