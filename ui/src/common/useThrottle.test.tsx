import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useThrottle } from './useThrottle';

describe('useThrottle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it('trailing: runs once as the window the first call opened closes', () => {
    const run = vi.fn();
    const { result } = renderHook(() => useThrottle(run, 100));
    act(() => {
      result.current();
      result.current();
    });
    expect(run).not.toHaveBeenCalled();
    act(() => vi.advanceTimersByTime(100));
    expect(run).toHaveBeenCalledTimes(1);
    // Quiet: nothing more; the next call opens a new window.
    act(() => vi.advanceTimersByTime(500));
    expect(run).toHaveBeenCalledTimes(1);
    act(() => result.current());
    act(() => vi.advanceTimersByTime(100));
    expect(run).toHaveBeenCalledTimes(2);
  });

  it('leading: runs at once, then once more per window while calls keep coming', () => {
    const run = vi.fn();
    const { result } = renderHook(() => useThrottle(run, 100, { leading: true }));
    act(() => result.current());
    expect(run).toHaveBeenCalledTimes(1);
    act(() => {
      result.current();
      result.current();
    });
    act(() => vi.advanceTimersByTime(100));
    expect(run).toHaveBeenCalledTimes(2);
    act(() => vi.advanceTimersByTime(100));
    expect(run).toHaveBeenCalledTimes(2);
    // The last window closed quiet: the next call runs at once again.
    act(() => vi.advanceTimersByTime(100));
    act(() => result.current());
    expect(run).toHaveBeenCalledTimes(3);
  });

  it('calls the latest function and cancels on unmount', () => {
    const first = vi.fn();
    const second = vi.fn();
    const { result, rerender, unmount } = renderHook(({ run }) => useThrottle(run, 100), {
      initialProps: { run: first },
    });
    act(() => result.current());
    rerender({ run: second });
    act(() => vi.advanceTimersByTime(100));
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);
    act(() => result.current());
    unmount();
    act(() => vi.advanceTimersByTime(100));
    expect(second).toHaveBeenCalledTimes(1);
  });
});
