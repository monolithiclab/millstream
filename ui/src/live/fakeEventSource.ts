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

// ?demo's log stream: a few lines of the canvas's 06:21 run, a lost connection, and the lines after it.
const demoLog = (ts: string, level: string, message: string, step = 'score') =>
  JSON.stringify({
    ts,
    level,
    message,
    pipeline: 'listings-daily',
    runId: 'run_0600',
    step,
    itemId: '',
    deliveryId: '',
    attempt: 1,
  });
export const demoLogLines = [
  demoLog('2026-10-15T06:21:01.204Z', 'info', 'it_2b90d7e4 leased by mill-01 slot 4'),
  demoLog('2026-10-15T06:21:02.881Z', 'info', 'tool get_comparables zip=75018 → 11 results (0.7 s)'),
  demoLog('2026-10-15T06:21:03.950Z', 'warn', 'it_9d02e6b3 output.schema failed, retry 2/2'),
  null,
  demoLog('2026-10-15T06:21:19.402Z', 'info', 'it_5e7f0a12 done in 2.1 s, $0.009'),
  demoLog('2026-10-15T06:21:20.017Z', 'error', 'it_9d02e6b3 dead: output.schema "score" missing'),
  demoLog('2026-10-15T06:21:20.533Z', 'info', 'it_88a1c3f0 leased by mill-01 slot 1', 'enrich'),
];

export const demoEventSource: EventSourceFactory = (url) => {
  const source = new FakeEventSource(url);
  setTimeout(() => {
    source.open();
    if (url.startsWith('/api/v1/live/logs')) {
      demoLogLines.forEach((line, i) =>
        line === null ? source.emit('gap', '{"lastEventId":"3"}') : source.emit('log', line, String(i + 1)),
      );
    }
  }, 0);
  return source;
};
