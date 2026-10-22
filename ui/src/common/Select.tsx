import type { SelectHTMLAttributes } from 'react';
import { field } from './styles';

// Every dropdown: a native select without the platform arrow (Chrome on macOS draws it against the border), the
// sidebar switcher's chevron inside the right padding instead. `className` sizes the wrapper.
export function Select({ className = '', ...props }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <span className={`relative inline-flex ${className}`}>
      <select {...props} className={`${field} min-h-[34px] w-full min-w-0 appearance-none pr-8`} />
      <svg
        width="12"
        height="12"
        viewBox="0 0 12 12"
        aria-hidden="true"
        className="pointer-events-none absolute top-1/2 right-2.5 -translate-y-1/2 text-ink-2"
      >
        <path d="M3 4.5L6 7.5L9 4.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </span>
  );
}
