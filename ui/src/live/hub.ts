import type { EventSourceFactory, EventSourceLike, LiveMessage, LiveState } from './useLiveStream';

// Interaction notes: the Live mark changes after 2 s down, figures dim after 5 s.
export const GRACE_MS = 2000;
export const STALE_MS = 5000;
export const BACKOFF_BASE_MS = 1000;
export const BACKOFF_MAX_MS = 30_000;

export const connecting: LiveState = { status: 'connecting', retryAt: null, stale: false };

export function backoffDelay(attempt: number): number {
  return Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** attempt);
}

// The browser resends Last-Event-ID only on its own reconnection; ours opens a new EventSource, so the id travels
// as the lastEventId query parameter, which the server reads as it reads the header.
export function resumeUrl(url: string, lastEventId: string | null): string {
  if (lastEventId === null) return url;
  return `${url}${url.includes('?') ? '&' : '?'}lastEventId=${encodeURIComponent(lastEventId)}`;
}

// One subscriber of a stream: the named events it wants besides "message", and its callbacks.
export interface Subscriber {
  events: readonly string[];
  onMessage: (message: LiveMessage) => void;
  onReconnect: () => void;
  onSnapshotRequired: () => void;
  onState: (state: LiveState) => void;
}

// One connection to a stream, shared by every subscriber of its url: exponential backoff on error, resumption from
// the last event id, and the stream's state for the live marks.
class Channel {
  private readonly subscribers = new Set<Subscriber>();
  // Every named event a subscriber asked for, listened to on each source.
  private readonly types = new Set<string>(['message']);
  private source: EventSourceLike | null = null;
  private attempt = 0;
  private lastEventId: string | null = null;
  private down = false;
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  state: LiveState = connecting;

  constructor(
    private readonly url: string,
    private readonly factory: EventSourceFactory,
  ) {}

  get size() {
    return this.subscribers.size;
  }

  add(subscriber: Subscriber) {
    this.subscribers.add(subscriber);
    const fresh = subscriber.events.filter((type) => !this.types.has(type));
    fresh.forEach((type) => this.types.add(type));
    if (this.source === null && this.timers.size === 0) this.connect();
    else if (this.source !== null) fresh.forEach((type) => this.listen(this.source as EventSourceLike, type));
  }

  remove(subscriber: Subscriber) {
    this.subscribers.delete(subscriber);
  }

  close() {
    this.clearTimers();
    this.source?.close();
    this.source = null;
  }

  private setState(update: (state: LiveState) => LiveState) {
    this.state = update(this.state);
    this.subscribers.forEach((s) => s.onState(this.state));
  }

  private later(fn: () => void, ms: number) {
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      fn();
    }, ms);
    this.timers.add(timer);
  }

  private clearTimers() {
    this.timers.forEach(clearTimeout);
    this.timers.clear();
  }

  private listen(es: EventSourceLike, type: string) {
    es.addEventListener(type, (event) => {
      if (event.lastEventId) this.lastEventId = event.lastEventId;
      const message = { type, id: event.lastEventId || null, data: event.data };
      this.subscribers.forEach((s) => {
        if (type === 'message' || s.events.includes(type)) s.onMessage(message);
      });
    });
  }

  private connect() {
    const es = this.factory(resumeUrl(this.url, this.lastEventId));
    this.source = es;
    es.onopen = () => {
      const reconnected = this.down;
      this.attempt = 0;
      this.down = false;
      this.clearTimers();
      this.setState(() => ({ status: 'live', retryAt: null, stale: false }));
      if (reconnected) this.subscribers.forEach((s) => s.onReconnect());
    };
    es.onerror = () => {
      if (this.source !== es) return; // a late error from a source already replaced
      es.close();
      this.source = null;
      this.retry();
    };
    this.types.forEach((type) => this.listen(es, type));
    es.addEventListener('snapshotRequired', () => {
      this.lastEventId = null;
      this.subscribers.forEach((s) => s.onSnapshotRequired());
    });
    // A heartbeat sent as an event moves the cursor, so a reconnection after a quiet spell resumes from the head.
    es.addEventListener('heartbeat', (event) => {
      if (event.lastEventId) this.lastEventId = event.lastEventId;
    });
  }

  private retry() {
    const delay = backoffDelay(this.attempt);
    this.attempt += 1;
    const retryAt = Date.now() + delay;
    if (!this.down) {
      this.down = true;
      this.later(() => this.setState((s) => ({ ...s, status: 'reconnecting' })), GRACE_MS);
      this.later(() => this.setState((s) => ({ ...s, stale: true })), STALE_MS);
    }
    this.setState((s) => ({ ...s, retryAt }));
    this.later(() => this.connect(), delay);
  }
}

// The live connections of the app, one per url, reference-counted: the shell's live mark, the graph and a screen's
// streams share one EventSource per url, so tabs stay under the browser's six connections per host over HTTP/1.1.
// The last subscriber leaving closes the connection.
export class LiveHub {
  private readonly channels = new Map<string, Channel>();

  constructor(private readonly factory: EventSourceFactory) {}

  subscribe(url: string, subscriber: Subscriber): () => void {
    let channel = this.channels.get(url);
    if (channel === undefined) {
      channel = new Channel(url, this.factory);
      this.channels.set(url, channel);
    }
    const joined = channel;
    joined.add(subscriber);
    subscriber.onState(joined.state);
    return () => {
      joined.remove(subscriber);
      if (joined.size > 0) return;
      joined.close();
      if (this.channels.get(url) === joined) this.channels.delete(url);
    };
  }
}
