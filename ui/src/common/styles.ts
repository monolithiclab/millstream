// Class lists the screens share, from the canvas components board. Buttons: primary (accent), secondary (outlined),
// danger (outlined in the bad tone, for discard), link (a text button), small variants for dense rows; they hug
// their label and wrap with it (French runs ~30 % longer).
const base =
  'inline-flex items-center justify-center gap-1.5 rounded-1 font-bold text-small disabled:cursor-not-allowed disabled:opacity-60';

export const buttons = {
  primary: `${base} bg-accent px-3.5 py-[7px] text-accent-ink`,
  secondary: `${base} border border-control-line bg-surface px-3.5 py-[6px] text-ink`,
  danger: `${base} border border-bad bg-surface px-3.5 py-[6px] text-bad`,
  smallPrimary: `${base} bg-accent px-2.5 py-1 text-tiny text-accent-ink`,
  smallSecondary: `${base} border border-control-line bg-surface px-2.5 py-[3px] text-tiny text-ink`,
  link: 'inline-flex items-center gap-1 p-0 font-bold text-tiny text-accent underline-offset-2 hover:underline',
  linkMuted: 'inline-flex items-center gap-1 p-0 font-bold text-tiny text-ink-2 underline-offset-2 hover:underline',
};

// A surface card: the canvas's 18 px radius, seam and first shadow.
export const card = 'rounded-[18px] border border-line bg-surface shadow-1';

// Card headings: Instrument Serif at 24 px.
export const cardTitle = 'm-0 font-display text-[24px] leading-7 font-normal';

// A text field or select.
export const field = 'rounded-1 border border-control-line bg-surface px-2.5 py-1.5 text-small text-ink';

// A filter field's caption, over its control in the toolbar; `filterLabel` wraps a control with it.
export const filterCaption = 'text-tiny font-bold text-ink-2';
export const filterLabel = `flex flex-col gap-1 ${filterCaption}`;
