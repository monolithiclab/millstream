import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './App';
import { DEMO_NOW, demoFixtures, shiftTimes } from './api/demo';
import { FakeApi } from './api/fake';
import { HttpApi } from './api/http';
import { demoEventSource } from './live/fakeEventSource';
import { browserEventSource } from './live/useLiveStream';
import { applyTheme, initialTheme } from './preferences';
import { readStored, writeStored } from './storage';
import './styles/index.css';

// ?demo runs the UI on the in-memory FakeApi, for the end-to-end smoke test and for looking around without a
// server; it lasts for the browser tab. ?demo=slow holds every read for three seconds and ?demo=error fails them, to
// show the loading and error states.
const query = new URLSearchParams(window.location.search).get('demo');
const demoMode = query ?? readStored('millstream.demo', 'sessionStorage');
const demo = demoMode !== null;
if (demo) writeStored('millstream.demo', demoMode, 'sessionStorage');

function demoApi(): FakeApi {
  const fixtures = shiftTimes(demoFixtures, Date.now() - Date.parse(DEMO_NOW));
  return new FakeApi(fixtures, demoMode === 'slow' ? { delayMs: 3000 } : { failReads: demoMode === 'error' });
}

// Before the first paint, so the stored theme does not flash.
applyTheme(initialTheme());

const root = document.getElementById('root');
if (root === null) throw new Error('#root missing from index.html');
createRoot(root).render(
  <StrictMode>
    <BrowserRouter>
      <App api={demo ? demoApi() : new HttpApi()} eventSource={demo ? demoEventSource : browserEventSource} />
    </BrowserRouter>
  </StrictMode>,
);
