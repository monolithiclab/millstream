// Ids truncate in the middle (it_7f3a…9c21), with the full value on hover, for copy and for screen readers.
export function middle(id: string, head = 7, tail = 4): string {
  return id.length <= head + tail + 1 ? id : `${id.slice(0, head)}…${id.slice(-tail)}`;
}

export function Id({ value, className = '' }: { value: string; className?: string }) {
  const short = middle(value);
  if (short === value) return <span className={`font-mono ${className}`}>{value}</span>;
  return (
    <span title={value} className={`font-mono ${className}`}>
      <span aria-hidden="true">{short}</span>
      <span className="sr-only">{value}</span>
    </span>
  );
}
