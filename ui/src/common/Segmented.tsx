import type { ReactNode } from 'react';

// A segmented toggle (canvas overlays, group by, range, state pickers): pressed buttons in one outlined group,
// which wraps on a narrow screen.
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
  return (
    <div
      role="group"
      aria-label={label}
      className="inline-flex max-w-full flex-wrap overflow-hidden rounded-1 border border-control-line text-small"
    >
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          onClick={() => onChange(o.value)}
          className={`min-h-[34px] border-r border-line px-3 last:border-r-0 ${value === o.value ? 'bg-ink font-bold text-bg' : 'font-medium text-ink-2'}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
