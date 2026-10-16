import { useEffect, type RefObject } from 'react';
import { useLatest } from '../useLatest';

// Open overlays, innermost last: only the top one takes Esc, and global shortcuts pause while any is open.
const layers: symbol[] = [];

export function overlayOpen(): boolean {
  return layers.length > 0;
}

// Closes an open overlay (dialog, drawer, popover) on Esc and, when `inside` is given, on a press outside those
// elements; Esc then returns focus to `returnFocus`.
export function useDismiss(
  open: boolean,
  onClose: () => void,
  {
    inside,
    returnFocus,
  }: { inside?: RefObject<HTMLElement | null>[]; returnFocus?: RefObject<HTMLElement | null> } = {},
) {
  const latest = useLatest({ onClose, inside, returnFocus });
  useEffect(() => {
    if (!open) return;
    const layer = Symbol('overlay');
    layers.push(layer);
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== 'Escape' || layers.at(-1) !== layer) return;
      latest.current.onClose();
      latest.current.returnFocus?.current?.focus();
    };
    const onPointer = (event: MouseEvent) => {
      const refs = latest.current.inside;
      if (refs && !refs.some((ref) => ref.current?.contains(event.target as Node))) latest.current.onClose();
    };
    document.addEventListener('keydown', onKey);
    document.addEventListener('mousedown', onPointer);
    return () => {
      layers.splice(layers.indexOf(layer), 1);
      document.removeEventListener('keydown', onKey);
      document.removeEventListener('mousedown', onPointer);
    };
  }, [open, latest]);
}
