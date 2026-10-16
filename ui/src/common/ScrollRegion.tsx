import { forwardRef, type ReactNode } from 'react';

// A named region that scrolls (a wide table at 375 px, a log tail): focusable, so a keyboard scrolls it too (axe
// scrollable-region-focusable). role overrides region, for a log.
export const ScrollRegion = forwardRef<
  HTMLDivElement,
  { label: string; className: string; role?: 'region' | 'log'; live?: 'off'; children: ReactNode }
>(function ScrollRegion({ label, className, role = 'region', live, children }, ref) {
  return (
    <div
      ref={ref}
      role={role}
      aria-label={label}
      aria-live={live}
      // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
      tabIndex={0}
      className={className}
    >
      {children}
    </div>
  );
});
