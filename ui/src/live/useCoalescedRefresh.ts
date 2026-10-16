import { useCallback, useEffect, useRef } from 'react';

// Live updates arrive about four times a second and repaint every 250 ms (interaction notes); a refetch through the
// REST API is heavier, so once a second.
export const LIVE_REFRESH_MS = 1000;

// Coalesces live events into one refresh per LIVE_REFRESH_MS: the returned schedule starts a window unless one is
// open, and the refresh runs as it closes.
export function useCoalescedRefresh(refresh: () => void): () => void {
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const schedule = useCallback(() => {
    if (timer.current !== null) return;
    timer.current = setTimeout(() => {
      timer.current = null;
      refresh();
    }, LIVE_REFRESH_MS);
  }, [refresh]);
  useEffect(
    () => () => {
      if (timer.current !== null) clearTimeout(timer.current);
    },
    [],
  );
  return schedule;
}
