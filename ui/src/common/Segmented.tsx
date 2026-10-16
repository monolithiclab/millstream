// A segmented toggle (canvas overlay and state pickers): pressed buttons in one outlined group, which wrap.
export function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="flex flex-wrap overflow-hidden rounded-1 border border-control-line text-small"
    >
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          onClick={() => onChange(o.value)}
          className={`border-r border-line px-3 py-[5px] last:border-r-0 ${value === o.value ? 'bg-ink font-bold text-bg' : 'text-ink-2'}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
