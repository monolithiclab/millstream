import type { ReactNode } from 'react';
import { arrowKeys, optionAttrs } from './arrowKeys';

// A segmented toggle (canvas overlays, group by, range, state pickers): pressed buttons in one outlined group,
// which wraps on a narrow screen. The arrow keys move the choice, as in a tab list; Tab reaches the chosen one.
export function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: ReactNode }[];
  value: T;
  onChange: (value: T) => void;
}) {
  const values = options.map((o) => o.value);
  // A value outside the options leaves the first one in the Tab order.
  const current = values.includes(value) ? value : (values[0] ?? value);
  const onKeyDown = arrowKeys(values, current, onChange);
  return (
    // The group handles the arrow keys of its buttons.
    // eslint-disable-next-line jsx-a11y/no-noninteractive-element-interactions
    <div
      role="group"
      aria-label={label}
      onKeyDown={onKeyDown}
      className="inline-flex max-w-full flex-wrap overflow-hidden rounded-1 border border-control-line text-small"
    >
      {options.map((o, i) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          {...optionAttrs(i, current === o.value)}
          onClick={() => onChange(o.value)}
          className={`min-h-[34px] border-r border-line px-3 last:border-r-0 ${value === o.value ? 'bg-ink font-bold text-bg' : 'font-medium text-ink-2'}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
