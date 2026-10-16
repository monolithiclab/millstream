import { useCallback, useEffect, useRef } from 'react';
import { useLatest } from '../useLatest';

// Runs `run` at most once per `ms` however often the returned function is called. Trailing (the default): the first
// call opens a window and `run` fires as it closes. Leading: the first call fires at once, and calls made during the
// window fire once more as it closes, which opens the next window. Unmounting cancels what is pending.
export function useThrottle(run: () => void, ms: number, { leading = false } = {}): () => void {
  const runRef = useLatest(run);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pending = useRef(false);

  useEffect(
    () => () => {
      if (timer.current !== null) clearTimeout(timer.current);
      timer.current = null;
      pending.current = false;
    },
    [],
  );

  return useCallback(() => {
    pending.current = true;
    if (timer.current !== null) return;
    const fire = () => {
      pending.current = false;
      runRef.current();
    };
    const close = () => {
      timer.current = null;
      if (!pending.current) return;
      fire();
      if (leading) timer.current = setTimeout(close, ms);
    };
    if (leading) fire();
    timer.current = setTimeout(close, ms);
  }, [ms, leading, runRef]);
}
