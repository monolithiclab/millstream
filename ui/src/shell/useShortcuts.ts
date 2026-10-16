import { useEffect } from 'react';
import { useLatest } from '../useLatest';
import { overlayOpen } from './useDismiss';

export interface ShortcutHandlers {
  openPalette: () => void;
  openHelp: () => void;
  // The key typed after g.
  go: (key: string) => void;
}

// How long g waits for its second key.
export const SEQUENCE_MS = 1500;

function isEditable(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName);
}

// Global shortcuts: ⌘K or Ctrl+K always; ? and g-sequences only when single keys are on, no overlay is open, and
// the focus is not in a field (single keys never fire inside inputs).
export function useShortcuts(handlers: ShortcutHandlers, singleKeys: boolean) {
  const handlersRef = useLatest(handlers);

  useEffect(() => {
    let pendingG: ReturnType<typeof setTimeout> | null = null;
    const clearPending = () => {
      if (pendingG !== null) clearTimeout(pendingG);
      pendingG = null;
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        clearPending();
        handlersRef.current.openPalette();
        return;
      }
      if (!singleKeys || overlayOpen() || event.metaKey || event.ctrlKey || event.altKey || isEditable(event.target))
        return;
      if (pendingG !== null) {
        clearPending();
        event.preventDefault();
        handlersRef.current.go(event.key);
        return;
      }
      if (event.key === '?') {
        event.preventDefault();
        handlersRef.current.openHelp();
      } else if (event.key === 'g') {
        pendingG = setTimeout(clearPending, SEQUENCE_MS);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      clearPending();
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [singleKeys, handlersRef]);
}
