import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { useDismiss } from './useDismiss';

const focusable =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

// A modal dialog per the interaction notes: focus moves in (to the control marked data-autofocus, else the first
// control, else the dialog), Tab stays inside, Esc or the backdrop closes, and focus returns to where it came from.
// A sheet rises from the bottom of a narrow screen (the 375 px inbox) and is an ordinary dialog from 640 px. It
// renders at the end of <body> and makes everything else inert while open, so neither the pointer, the keyboard nor
// assistive technology reaches the page behind it.
export function Dialog({
  labelledBy,
  onClose,
  sheet = false,
  children,
}: {
  labelledBy: string;
  onClose: () => void;
  sheet?: boolean;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [host] = useState(() => document.createElement('div'));

  useLayoutEffect(() => {
    // The opener is read before the page goes inert, which would move focus to <body>.
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    document.body.appendChild(host);
    const behind = [...document.body.children].filter((el) => el !== host && !el.hasAttribute('inert'));
    behind.forEach((el) => el.setAttribute('inert', ''));
    const root = ref.current;
    (
      root?.querySelector<HTMLElement>('[data-autofocus]') ??
      root?.querySelector<HTMLElement>(focusable) ??
      root
    )?.focus();
    return () => {
      behind.forEach((el) => el.removeAttribute('inert'));
      host.remove();
      opener?.focus();
    };
  }, [host]);

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

  return createPortal(
    <div
      className={`fixed inset-0 z-50 flex justify-center ${sheet ? 'items-end sm:items-start sm:p-4 sm:pt-[12vh]' : 'items-start p-4 pt-[12vh]'}`}
    >
      <div className="absolute inset-0 bg-ink/30" aria-hidden="true" data-testid="backdrop" onMouseDown={onClose} />
      <div
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelledBy}
        tabIndex={-1}
        className={`relative w-full max-w-xl overflow-hidden border border-control-line bg-surface shadow-2 ${sheet ? 'rounded-t-[24px] sm:rounded-3' : 'rounded-3'}`}
      >
        {children}
      </div>
    </div>,
    host,
  );
}
