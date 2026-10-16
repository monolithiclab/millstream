import { act, renderHook } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiProvider } from '../api/context';
import { FakeApi } from '../api/fake';
import { demoEventSource, fakeEventSource, FakeEventSource } from './fakeEventSource';
import {
  BACKOFF_MAX_MS,
  backoffDelay,
  browserEventSource,
  GRACE_MS,
  resumeUrl,
  STALE_MS,
  useLiveStream,
  type LiveOptions,
} from './useLiveStream';

function wrapper({ children }: { children: ReactNode }) {
  return (
    <ApiProvider api={new FakeApi()} eventSource={fakeEventSource}>
      {children}
    </ApiProvider>
  );
}

function subscribe(url: string | null, options: LiveOptions = {}) {
  return renderHook(({ u, o }) => useLiveStream(u, o), { wrapper, initialProps: { u: url, o: options } });
}

describe('useLiveStream', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it('connects, goes live and delivers messages with their ids', () => {
    const onMessage = vi.fn();
    const { result } = subscribe('/api/v1/live/runs/r', { onMessage, events: ['state'] });
    expect(result.current.status).toBe('connecting');
    const es = FakeEventSource.latest();
    expect(es.url).toBe('/api/v1/live/runs/r');
    act(() => es.open());
    expect(result.current).toEqual({ status: 'live', retryAt: null, stale: false });
    act(() => {
      es.emit('state', '{"n":1}', '41');
      es.emit('message', 'hello');
      es.emit('ignored', 'x', '99');
    });
    expect(onMessage.mock.calls.map(([m]) => m)).toEqual([
      { type: 'state', id: '41', data: '{"n":1}' },
      { type: 'message', id: null, data: 'hello' },
    ]);
  });

  it('reconnects with exponential backoff and resumes from the last event id', () => {
    const onReconnect = vi.fn();
    const { result } = subscribe('/api/v1/live/items/i', { onReconnect, events: ['state'] });
    let es = FakeEventSource.latest();
    act(() => {
      es.open();
      es.emit('state', 'a', '7');
      es.fail();
    });
    expect(es.closed).toBe(true);
    // Within the grace period the mark stays live.
    expect(result.current.status).toBe('live');
    expect(result.current.retryAt).toBe(Date.now() + 1000);

    act(() => vi.advanceTimersByTime(1000));
    es = FakeEventSource.latest();
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(es.url).toBe('/api/v1/live/items/i?lastEventId=7');

    // The second failure waits 2 s, the third 4 s.
    act(() => es.fail());
    act(() => vi.advanceTimersByTime(1999));
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(result.current.status).toBe('reconnecting');
    act(() => vi.advanceTimersByTime(1));
    expect(FakeEventSource.instances).toHaveLength(3);
    act(() => FakeEventSource.latest().fail());
    act(() => vi.advanceTimersByTime(4000));
    expect(FakeEventSource.instances).toHaveLength(4);

    act(() => FakeEventSource.latest().open());
    expect(result.current).toEqual({ status: 'live', retryAt: null, stale: false });
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // Back to the base delay after a successful open.
    act(() => FakeEventSource.latest().fail());
    expect(result.current.retryAt).toBe(Date.now() + 1000);
  });

  it('reports reconnecting after the grace period and stale after five seconds', () => {
    const { result } = subscribe('/s');
    act(() => {
      FakeEventSource.latest().open();
      FakeEventSource.latest().fail();
    });
    act(() => vi.advanceTimersByTime(GRACE_MS - 1));
    expect(result.current.status).toBe('live');
    act(() => vi.advanceTimersByTime(1));
    expect(result.current.status).toBe('reconnecting');
    expect(result.current.stale).toBe(false);
    act(() => vi.advanceTimersByTime(STALE_MS - GRACE_MS));
    expect(result.current.stale).toBe(true);
  });

  it('forgets the cursor when the server asks for a snapshot', () => {
    const onSnapshotRequired = vi.fn();
    subscribe('/s?scope=x', { onSnapshotRequired });
    const es = FakeEventSource.latest();
    act(() => {
      es.open();
      es.emit('message', 'a', '12');
      es.emit('snapshotRequired', '');
      es.fail();
    });
    expect(onSnapshotRequired).toHaveBeenCalledTimes(1);
    act(() => vi.advanceTimersByTime(1000));
    expect(FakeEventSource.latest().url).toBe('/s?scope=x');
  });

  it('ignores a late error from a replaced source', () => {
    subscribe('/a');
    const first = FakeEventSource.latest();
    act(() => first.fail());
    act(() => vi.advanceTimersByTime(1000));
    act(() => first.fail());
    act(() => vi.advanceTimersByTime(BACKOFF_MAX_MS));
    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('closes on unmount and on url change, and subscribes to nothing for null', () => {
    const { rerender, unmount } = subscribe('/a');
    const first = FakeEventSource.latest();
    rerender({ u: '/b', o: {} });
    expect(first.closed).toBe(true);
    const second = FakeEventSource.latest();
    expect(second.url).toBe('/b');
    rerender({ u: null, o: {} });
    expect(second.closed).toBe(true);
    expect(FakeEventSource.instances).toHaveLength(2);
    unmount();
  });

  it('cancels a pending retry on unmount', () => {
    const { unmount } = subscribe('/a');
    act(() => FakeEventSource.latest().fail());
    unmount();
    act(() => vi.advanceTimersByTime(BACKOFF_MAX_MS));
    expect(FakeEventSource.instances).toHaveLength(1);
  });
});

describe('helpers', () => {
  it('caps the backoff', () => {
    expect([0, 1, 2, 3, 4, 5, 6].map(backoffDelay)).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000]);
  });

  it('adds the cursor to the url', () => {
    expect(resumeUrl('/a', null)).toBe('/a');
    expect(resumeUrl('/a', '5')).toBe('/a?lastEventId=5');
    expect(resumeUrl('/a?b=1', 'x y')).toBe('/a?b=1&lastEventId=x%20y');
  });

  it('builds browser and demo sources', () => {
    vi.useFakeTimers();
    const Source = vi.fn(function (this: { url: string }, url: string) {
      this.url = url;
    });
    vi.stubGlobal('EventSource', Source);
    expect(browserEventSource('/x')).toMatchObject({ url: '/x' });
    vi.unstubAllGlobals();
    const demo = demoEventSource('/d') as FakeEventSource;
    const opened = vi.fn();
    demo.onopen = opened;
    vi.runAllTimers();
    expect(opened).toHaveBeenCalled();
    const logs = demoEventSource('/api/v1/live/logs') as FakeEventSource;
    const seen: string[] = [];
    logs.addEventListener('log', () => seen.push('log'));
    logs.addEventListener('gap', () => seen.push('gap'));
    vi.runAllTimers();
    expect(seen).toEqual(['log', 'log', 'log', 'gap', 'log', 'log', 'log']);
    expect(() => {
      FakeEventSource.reset();
      FakeEventSource.latest();
    }).toThrow();
  });
});
