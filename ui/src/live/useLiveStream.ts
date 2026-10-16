import { useEffect, useState } from 'react';
import { useEventSourceFactory } from '../api/context';
import { useLatest } from '../useLatest';

// The part of EventSource the hook uses, so tests and ?demo can supply their own.
export interface EventSourceLike {
  onopen: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

export const browserEventSource: EventSourceFactory = (url) => new EventSource(url) as EventSourceLike;

export interface LiveMessage {
  type: string;
  id: string | null;
  data: string;
}

export type LiveStatus = 'connecting' | 'live' | 'reconnecting';

export interface LiveState {
  status: LiveStatus;
  // When the next attempt starts (epoch ms), while reconnecting.
  retryAt: number | null;
  // True once the stream has been down for STALE_MS: live figures dim.
  stale: boolean;
}

export interface LiveOptions {
  // Named events to listen to besides "message".
  events?: string[];
  onMessage?: (message: LiveMessage) => void;
  // After a reconnection: reload a snapshot through the REST API, the stream resumes from the last event id.
  onReconnect?: () => void;
  // The server sent "snapshotRequired" (the cursor is older than its retention): reload state, resume from now.
  onSnapshotRequired?: () => void;
}

// Interaction notes: the Live mark changes after 2 s down, figures dim after 5 s.
export const GRACE_MS = 2000;
export const STALE_MS = 5000;
export const BACKOFF_BASE_MS = 1000;
export const BACKOFF_MAX_MS = 30_000;

const connecting: LiveState = { status: 'connecting', retryAt: null, stale: false };

export function backoffDelay(attempt: number): number {
  return Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** attempt);
}

// The browser resends Last-Event-ID only on its own reconnection; ours opens a new EventSource, so the id travels
// as the lastEventId query parameter, which the server reads as it reads the header.
export function resumeUrl(url: string, lastEventId: string | null): string {
  if (lastEventId === null) return url;
  return `${url}${url.includes('?') ? '&' : '?'}lastEventId=${encodeURIComponent(lastEventId)}`;
}

// Subscribes to a server-sent event stream with exponential backoff and Last-Event-ID resumption. A null url
// subscribes to nothing.
export function useLiveStream(url: string | null, options: LiveOptions = {}): LiveState {
  const factory = useEventSourceFactory();
  // Keyed by url, so a new url reads as connecting without a reset inside the effect.
  const [keyed, setKeyed] = useState<{ url: string | null; state: LiveState }>({ url, state: connecting });
  const optionsRef = useLatest(options);
  const events = (options.events ?? []).join(',');

  useEffect(() => {
    if (url === null) return;
    const setState = (update: (state: LiveState) => LiveState) =>
      setKeyed((current) => ({ url, state: update(current.url === url ? current.state : connecting) }));
    let source: EventSourceLike | null = null;
    let attempt = 0;
    let lastEventId: string | null = null;
    let down = false;
    const timers = new Set<ReturnType<typeof setTimeout>>();
    const later = (fn: () => void, ms: number) => {
      const timer = setTimeout(() => {
        timers.delete(timer);
        fn();
      }, ms);
      timers.add(timer);
    };
    const clearTimers = () => {
      timers.forEach(clearTimeout);
      timers.clear();
    };

    const connect = () => {
      const es = factory(resumeUrl(url, lastEventId));
      source = es;
      es.onopen = () => {
        const reconnected = down;
        attempt = 0;
        down = false;
        clearTimers();
        setState(() => ({ status: 'live', retryAt: null, stale: false }));
        if (reconnected) optionsRef.current.onReconnect?.();
      };
      es.onerror = () => {
        if (source !== es) return; // a late error from a source already replaced
        es.close();
        source = null;
        retry();
      };
      for (const type of ['message', ...(events === '' ? [] : events.split(','))]) {
        es.addEventListener(type, (event) => {
          if (event.lastEventId) lastEventId = event.lastEventId;
          optionsRef.current.onMessage?.({ type, id: event.lastEventId || null, data: event.data });
        });
      }
      es.addEventListener('snapshotRequired', () => {
        lastEventId = null;
        optionsRef.current.onSnapshotRequired?.();
      });
    };

    const retry = () => {
      const delay = backoffDelay(attempt);
      attempt += 1;
      const retryAt = Date.now() + delay;
      if (!down) {
        down = true;
        later(() => setState((s) => ({ ...s, status: 'reconnecting' })), GRACE_MS);
        later(() => setState((s) => ({ ...s, stale: true })), STALE_MS);
      }
      setState((s) => ({ ...s, retryAt }));
      later(connect, delay);
    };

    connect();
    return () => {
      clearTimers();
      source?.close();
    };
  }, [factory, url, events, optionsRef]);

  return keyed.url === url ? keyed.state : connecting;
}
