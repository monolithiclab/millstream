import { liveUrls } from '../api/api';
import { demoFixtures, demoLogEvents } from '../api/demo';
import type { EventSourceFactory, EventSourceLike } from './useLiveStream';

// A scriptable EventSource: tests open, feed and fail it; ?demo opens it on the next tick.
export class FakeEventSource implements EventSourceLike {
  static instances: FakeEventSource[] = [];

  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  closed = false;
  private readonly listeners = new Map<string, ((event: MessageEvent<string>) => void)[]>();

  constructor(readonly url: string) {
    FakeEventSource.instances.push(this);
  }

  static reset(): void {
    FakeEventSource.instances = [];
  }

  static latest(): FakeEventSource {
    const last = FakeEventSource.instances.at(-1);
    if (last === undefined) throw new Error('no FakeEventSource created');
    return last;
  }

  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  close(): void {
    this.closed = true;
  }

  open(): void {
    this.onopen?.(new Event('open'));
  }

  emit(type: string, data: string, id = ''): void {
    const event = new MessageEvent<string>(type, { data, lastEventId: id });
    this.listeners.get(type)?.forEach((listener) => listener(event));
  }

  fail(): void {
    this.onerror?.(new Event('error'));
  }
}

export const fakeEventSource: EventSourceFactory = (url) => new FakeEventSource(url);

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
