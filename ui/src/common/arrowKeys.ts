import type { KeyboardEvent } from 'react';

// The keyboard of a tab list or a segmented toggle (WAI-ARIA tabs), set on the element around the options: Left and
// Right move to the previous and next option, wrapping, Home and End to the first and last. The option is chosen
// and focused (each option carries optionAttrs); only the chosen one is in the Tab order, so Tab leaves the group.
export function arrowKeys<T>(
  options: readonly T[],
  current: T,
  choose: (option: T) => void,
): (event: KeyboardEvent<HTMLElement>) => void {
  return (event) => {
    const n = options.length;
    const index = options.indexOf(current);
    const to: Record<string, number> = { ArrowRight: index + 1, ArrowLeft: index - 1 + n, Home: 0, End: n - 1 };
    const target = to[event.key];
    if (target === undefined || n === 0) return;
    const next = target % n;
    event.preventDefault();
    choose(options[next] as T);
    event.currentTarget.querySelector<HTMLElement>(`[data-option="${next}"]`)?.focus();
  };
}

// The attributes of the option at index, chosen or not.
export const optionAttrs = (index: number, chosen: boolean) => ({ 'data-option': index, tabIndex: chosen ? 0 : -1 });
