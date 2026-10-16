// Ids truncate in the middle (interaction notes): it_7f3a…9c21, the full value on hover, for copy and for screen
// readers.
export function shortId(id: string): string {
  return id.length <= 14 ? id : `${id.slice(0, 7)}…${id.slice(-4)}`;
}

export function Id({ value, className = '' }: { value: string; className?: string }) {
  const short = shortId(value);
  if (short === value) return <span className={`font-mono ${className}`}>{value}</span>;
  return (
    <span title={value} className={`font-mono ${className}`}>
      <span aria-hidden="true">{short}</span>
      <span className="sr-only">{value}</span>
    </span>
  );
}
