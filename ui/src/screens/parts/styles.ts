// Class lists the screens share, from the canvas components board. Buttons hug their label (French runs ~30 %
// longer) and wrap rather than clip.
const base =
  'inline-flex items-center justify-center gap-2 rounded-1 font-bold whitespace-nowrap disabled:cursor-not-allowed disabled:opacity-60';

export const button = {
  primary: `${base} bg-accent px-3 py-1.5 text-small text-accent-ink`,
  secondary: `${base} border border-control-line bg-transparent px-3 py-1.5 text-small text-ink`,
  danger: `${base} border border-bad bg-transparent px-3 py-1.5 text-small text-bad`,
  small: `${base} border border-control-line bg-transparent px-2.5 py-1 text-tiny text-ink`,
};

export const card = 'rounded-3 border border-line bg-surface shadow-1';

// Card headings: Instrument Serif at 24 px.
export const cardTitle = 'm-0 font-display text-[24px] leading-7 font-normal';

export const field = 'rounded-1 border border-control-line bg-surface px-2.5 py-1.5 text-small text-ink';
