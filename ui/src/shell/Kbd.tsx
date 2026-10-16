import type { ReactNode } from 'react';

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="rounded-[6px] border border-b-2 border-control-line bg-bg px-1.5 font-mono text-tiny whitespace-nowrap text-ink-2">
      {children}
    </kbd>
  );
}
