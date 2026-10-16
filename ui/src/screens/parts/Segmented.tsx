import type { ReactNode } from 'react';

export interface Segment<T extends string> {
  value: T;
  label: ReactNode;
}

// A row of toggle buttons, one pressed (canvas: group by, range, insight state).
export function Segmented<T extends string>({
  label,
  segments,
  value,
  onChange,
}: {
  label: string;
  segments: Segment<T>[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="inline-flex max-w-full flex-wrap overflow-hidden rounded-1 border border-control-line text-small"
    >
      {segments.map((segment) => {
        const pressed = segment.value === value;
        return (
          <button
            key={segment.value}
            type="button"
            aria-pressed={pressed}
            onClick={() => onChange(segment.value)}
            className={`min-h-[34px] border-r border-line px-3 last:border-r-0 ${pressed ? 'bg-ink font-bold text-bg' : 'bg-transparent font-medium text-ink-2'}`}
          >
            {segment.label}
          </button>
        );
      })}
    </div>
  );
}
