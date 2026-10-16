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

export const demoEventSource: EventSourceFactory = (url) => {
  const source = new FakeEventSource(url);
  setTimeout(() => source.open(), 0);
  return source;
};
