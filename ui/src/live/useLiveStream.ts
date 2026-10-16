import { useEffect, useState } from 'react';
import { useLiveHub } from '../api/context';
import { connecting } from './hub';
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

// A message's JSON data, or null when it is not JSON: a malformed event changes nothing.
export function parseData<T>(message: LiveMessage): T | null {
  try {
    return JSON.parse(message.data) as T;
  } catch {
    return null;
  }
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

export { BACKOFF_BASE_MS, BACKOFF_MAX_MS, backoffDelay, GRACE_MS, resumeUrl, STALE_MS } from './hub';

// Subscribes to a server-sent event stream through the app's LiveHub: one connection per url, shared, with
// exponential backoff and Last-Event-ID resumption. A null url subscribes to nothing.
export function useLiveStream(url: string | null, options: LiveOptions = {}): LiveState {
  const hub = useLiveHub();
  // Keyed by url, so a new url reads as connecting without a reset inside the effect.
  const [keyed, setKeyed] = useState<{ url: string | null; state: LiveState }>({ url, state: connecting });
  const optionsRef = useLatest(options);
  const events = (options.events ?? []).join(',');

  useEffect(() => {
    if (url === null) return;
    return hub.subscribe(url, {
      events: events === '' ? [] : events.split(','),
      onMessage: (message) => optionsRef.current.onMessage?.(message),
      onReconnect: () => optionsRef.current.onReconnect?.(),
      onSnapshotRequired: () => optionsRef.current.onSnapshotRequired?.(),
      onState: (state) => setKeyed({ url, state }),
    });
  }, [hub, url, events, optionsRef]);

  return keyed.url === url ? keyed.state : connecting;
}
