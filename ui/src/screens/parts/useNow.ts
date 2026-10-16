import { useEffect, useState } from 'react';

// The current time, refreshed every `everyMs` while `ticking`, for ages and relative times rendered without reading
// the clock during render.
export function useNow(everyMs = 30_000, ticking = true): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!ticking) return;
    const timer = setInterval(() => setNow(Date.now()), everyMs);
    return () => clearInterval(timer);
  }, [everyMs, ticking]);
  return now;
}
