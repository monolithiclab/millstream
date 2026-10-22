import { useId, useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useDismiss } from '../shell/useDismiss';
import { card } from './styles';

// The 16 px page gutter a popover keeps from the viewport's edges.
const gutter = 16;

// A `(?)` disclosure holding a screen's (or a field's) explanation: a button, not a hover tooltip, so touch and
// keyboard reach it. Escape, a click outside or a second click closes it.
export function HelpTip({ label, children }: { label?: string; children: ReactNode }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [shift, setShift] = useState(0);
  const button = useRef<HTMLButtonElement>(null);
  const popover = useRef<HTMLSpanElement>(null);
  const popoverId = useId();

  useDismiss(open, () => setOpen(false), { inside: [popover, button], returnFocus: button });

  // Pulled left when it would cross the viewport's right edge (a help toggle late in a long title).
  useLayoutEffect(() => {
    if (!open || !popover.current) return;
    const overflow = popover.current.getBoundingClientRect().right - (window.innerWidth - gutter);
    if (overflow > 0) setShift(-overflow);
    return () => setShift(0);
  }, [open]);

  return (
    <span className="relative inline-flex">
      <button
        ref={button}
        type="button"
        aria-expanded={open}
        aria-controls={popoverId}
        aria-label={label ?? t('help.toggle')}
        onClick={() => setOpen(!open)}
        className="inline-flex size-5 items-center justify-center rounded-pill border border-control-line bg-surface text-tiny font-bold text-ink-2 hover:text-ink aria-expanded:border-ink aria-expanded:text-ink"
      >
        <span aria-hidden="true">?</span>
      </button>
      {open && (
        <span
          ref={popover}
          id={popoverId}
          style={shift ? { left: shift } : undefined}
          className={`${card} absolute block top-full left-0 z-40 mt-1.5 w-max max-w-[min(320px,calc(100vw-32px))] p-3 text-small font-normal text-ink`}
        >
          {children}
        </span>
      )}
    </span>
  );
}
