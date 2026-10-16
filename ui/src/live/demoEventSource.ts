import { liveUrls } from '../api/api';
import { demoFixtures, demoLogEvents } from '../api/demo';
import { FakeEventSource } from './fakeEventSource';
import type { EventSourceFactory } from './useLiveStream';

// What ?demo streams once open: a pipeline's snapshot, or the log lines of the stream's scope (?item=, ?step=, ...)
// with the lost connection among them.
function demoEvents(url: string): [type: string, data: object][] {
  const snapshot = demoFixtures.snapshots.find((s) => liveUrls.pipeline(s.pipeline) === url);
  if (snapshot !== undefined) return [['snapshot', snapshot]];
  const [path, query] = url.split('?');
  if (path !== liveUrls.logs()) return [];
  const scope = new URLSearchParams(query);
  const fields = { pipeline: 'pipeline', run: 'runId', step: 'step', item: 'itemId' } as const;
  return demoLogEvents.filter(
    ([type, data]) =>
      type === 'gap' ||
      Object.entries(fields).every(([param, field]) => [null, data[field]].includes(scope.get(param))),
  );
}

export const demoEventSource: EventSourceFactory = (url) => {
  const source = new FakeEventSource(url);
  setTimeout(() => {
    source.open();
    demoEvents(url).forEach(([type, data], i) => source.emit(type, JSON.stringify(data), String(i + 1)));
  }, 0);
  return source;
};
