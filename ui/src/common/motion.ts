import { useCallback, useSyncExternalStore } from 'react';

// prefers-reduced-motion, live: pebbles freeze and nothing animates while it holds (Review focus 5).
export function useReducedMotion(): boolean {
  return useMediaQuery('(prefers-reduced-motion: reduce)', false);
}

// The md breakpoint of the Tailwind config: below it, phone layouts.
export const WIDE = '(min-width: 768px)';

function subscribeVisibility(onChange: () => void): () => void {
  document.addEventListener('visibilitychange', onChange);
  return () => document.removeEventListener('visibilitychange', onChange);
}

// Animation runs only while the tab is visible (interaction notes).
export function useDocumentVisible(): boolean {
  return useSyncExternalStore(subscribeVisibility, () => document.visibilityState !== 'hidden');
}

// A media query, live; fallback answers where there is no matchMedia (jsdom): by default the wide desktop layout.
export function useMediaQuery(media: string, fallback = true): boolean {
  const subscribe = useCallback(
    (onChange: () => void) => {
      if (typeof window.matchMedia !== 'function') return () => undefined;
      const list = window.matchMedia(media);
      list.addEventListener('change', onChange);
      return () => list.removeEventListener('change', onChange);
    },
    [media],
  );
  return useSyncExternalStore(subscribe, () =>
    typeof window.matchMedia === 'function' ? window.matchMedia(media).matches : fallback,
  );
}
