import { useThrottle } from '../common/useThrottle';

// Live updates arrive about four times a second and repaint every 250 ms (interaction notes); a refetch through the
// REST API is heavier, so once a second.
export const LIVE_REFRESH_MS = 1000;

// Coalesces live events into one refresh per LIVE_REFRESH_MS: the returned schedule starts a window unless one is
// open, and the refresh runs as it closes.
export const useCoalescedRefresh = (refresh: () => void): (() => void) => useThrottle(refresh, LIVE_REFRESH_MS);
