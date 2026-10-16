// The wordmark's mark: two streams and the millstones between them.
export function Logo() {
  return (
    <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
      <path
        d="M1 6C6 3.5 11 8.5 21 5.5M1 16.5C6 14 11 19 21 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <circle cx="7" cy="11.2" r="1.9" className="fill-accent" opacity="0.45" />
      <circle cx="14" cy="11" r="2.3" className="fill-accent" />
    </svg>
  );
}
