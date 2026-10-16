import { useEffect, useRef, type ReactNode } from 'react';
import { useDismiss } from './useDismiss';

const focusable =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

// A modal dialog per the interaction notes: focus moves in (to the first control, else the dialog), Tab stays
// inside, Esc or the backdrop closes, and focus returns to where it came from.
export function Dialog({
  labelledBy,
  onClose,
  children,
}: {
  labelledBy: string;
  onClose: () => void;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const root = ref.current;
    (root?.querySelector<HTMLElement>(focusable) ?? root)?.focus();
    return () => opener?.focus();
  }, []);

  useDismiss(true, onClose);

  useEffect(() => {
    const root = ref.current;
    if (root === null) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Tab') return;
      const items = [...root.querySelectorAll<HTMLElement>(focusable)];
      const first = items[0];
      const last = items.at(-1);
      if (first === undefined || last === undefined) return;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    root.addEventListener('keydown', onKeyDown);
    return () => root.removeEventListener('keydown', onKeyDown);
  }, []);

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center p-4 pt-[12vh]">
      <div className="absolute inset-0 bg-ink/30" aria-hidden="true" data-testid="backdrop" onMouseDown={onClose} />
      <div
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelledBy}
        tabIndex={-1}
        className="relative w-full max-w-xl overflow-hidden rounded-3 border border-control-line bg-surface shadow-2"
      >
        {children}
      </div>
    </div>
  );
}
