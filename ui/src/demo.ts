import type { Api } from './api/api';
import { DEMO_NOW, demoFixtures, shiftTimes } from './api/demo';
import { FakeApi } from './api/fake';
import { demoEventSource } from './live/demoEventSource';
import type { EventSourceFactory } from './live/useLiveStream';

// The in-memory backend of ?demo, loaded by main.tsx with a dynamic import only when demo mode is on, so neither the
// demo dataset nor FakeApi weigh on the bundle a server sends. ?demo=slow holds every read for three seconds and
// ?demo=error fails them, to show the loading and error states.
export function demoBackend(mode: string): { api: Api; eventSource: EventSourceFactory } {
  const fixtures = shiftTimes(demoFixtures, Date.now() - Date.parse(DEMO_NOW));
  const api = new FakeApi(fixtures, mode === 'slow' ? { delayMs: 3000 } : { failReads: mode === 'error' });
  return { api, eventSource: demoEventSource };
}
