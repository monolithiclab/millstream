import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './App';
import { FakeApi } from './api/fake';
import { HttpApi } from './api/http';
import { demoEventSource } from './live/fakeEventSource';
import { browserEventSource } from './live/useLiveStream';
import { applyTheme, initialTheme } from './preferences';
import { readStored, writeStored } from './storage';
import './styles/index.css';

// ?demo runs the UI on the in-memory FakeApi, for the end-to-end smoke test and for looking around without a
// server; it lasts for the browser tab.
const demo =
  new URLSearchParams(window.location.search).has('demo') || readStored('millstream.demo', 'sessionStorage') === '1';
if (demo) writeStored('millstream.demo', '1', 'sessionStorage');

// Before the first paint, so the stored theme does not flash.
applyTheme(initialTheme());

const root = document.getElementById('root');
if (root === null) throw new Error('#root missing from index.html');
createRoot(root).render(
  <StrictMode>
    <BrowserRouter>
      <App api={demo ? new FakeApi() : new HttpApi()} eventSource={demo ? demoEventSource : browserEventSource} />
    </BrowserRouter>
  </StrictMode>,
);
