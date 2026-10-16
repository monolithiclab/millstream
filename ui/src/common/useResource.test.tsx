import { act, renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { Api } from '../api/api';
import { ApiProvider } from '../api/context';
import { FakeApi } from '../api/fake';
import { fakeEventSource } from '../live/fakeEventSource';
import { useResource } from './useResource';

const api = new FakeApi();

function wrapper({ children }: { children: ReactNode }) {
  return (
    <ApiProvider api={api} eventSource={fakeEventSource}>
      {children}
    </ApiProvider>
  );
}

function mount<T>(key: string | null, read: (api: Api) => Promise<T>) {
  return renderHook(({ k, r }) => useResource(k, r), { wrapper, initialProps: { k: key, r: read } });
}

// A promise the test settles.
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useResource', () => {
  it('stays loading and reads nothing for a null key', () => {
    const read = vi.fn(() => Promise.resolve(1));
    const { result } = mount(null, read);
    expect(result.current.status).toBe('loading');
    expect(read).not.toHaveBeenCalled();
  });

  it('turns a rejection that is not an Error into one', async () => {
    const { result } = mount('k', () => Promise.reject('offline'));
    await waitFor(() => expect(result.current.status).toBe('error'));
    expect(result.current).toMatchObject({ status: 'error', error: new Error('offline') });
  });

  it('keeps the data on a failed refresh, and shows the error of a failed reload', async () => {
    let fail = false;
    const { result } = mount('k', () => (fail ? Promise.reject(new Error('down')) : Promise.resolve('data')));
    await waitFor(() => expect(result.current.status).toBe('ready'));
    fail = true;
    act(() => result.current.refresh());
    // The background read fails: the data stays on screen.
    await act(() => Promise.resolve());
    expect(result.current).toMatchObject({ status: 'ready', data: 'data' });
    act(() => result.current.reload());
    expect(result.current.status).toBe('loading');
    await waitFor(() => expect(result.current).toMatchObject({ status: 'error', error: new Error('down') }));
  });

  it('shows the error of a failed refresh when there was no data to keep', async () => {
    const { result } = mount('k', () => Promise.reject(new Error('down')));
    await waitFor(() => expect(result.current.status).toBe('error'));
    act(() => result.current.refresh());
    await act(() => Promise.resolve());
    expect(result.current).toMatchObject({ status: 'error', error: new Error('down') });
  });

  it('drops the answer of a key it has left, success or failure', async () => {
    const reads: Record<string, ReturnType<typeof deferred<string>>> = { a: deferred(), b: deferred(), c: deferred() };
    const { result, rerender } = mount<string>('a', () => reads.a!.promise);
    rerender({ k: 'b', r: () => reads.b!.promise });
    rerender({ k: 'c', r: () => reads.c!.promise });
    await act(async () => {
      reads.a!.resolve('stale');
      reads.b!.reject(new Error('stale'));
      await Promise.resolve();
    });
    expect(result.current.status).toBe('loading');
    await act(async () => {
      reads.c!.resolve('fresh');
      await reads.c!.promise;
    });
    expect(result.current).toMatchObject({ status: 'ready', data: 'fresh' });
  });

  it('changes the data in place only when there is data', async () => {
    const read = deferred<number>();
    const { result } = mount('k', () => read.promise);
    act(() => result.current.mutate((n) => n + 1));
    expect(result.current.status).toBe('loading');
    await act(async () => {
      read.resolve(1);
      await read.promise;
    });
    act(() => result.current.mutate((n) => n + 1));
    expect(result.current).toMatchObject({ status: 'ready', data: 2 });
  });
});
