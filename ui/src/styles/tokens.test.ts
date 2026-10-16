import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const css = readFileSync('src/styles/tokens.css', 'utf8'); // Vitest runs from ui/;

// WCAG 2.2 contrast of every text pair (4.5:1) and UI pair (3:1) the canvas uses, computed from tokens.css itself.
const tokens = new Map<string, [string, string]>(
  [...css.matchAll(/--ms-([\w-]+): light-dark\((#[0-9a-f]{6}), (#[0-9a-f]{6})\)/g)].map((m) => [m[1]!, [m[2]!, m[3]!]]),
);

function luminance(hex: string): number {
  const [r, g, b] = [1, 3, 5].map((i) => {
    const c = parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  }) as [number, number, number];
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x) as [number, number];
  return (hi + 0.05) / (lo + 0.05);
}

const grounds = ['bg', 'surface', 'surface-2', 'sunken'];
const statuses = ['ok', 'warn', 'bad', 'human', 'mute'];
const text: [string, string][] = [
  ...['ink', 'ink-2', 'ink-3'].flatMap((fg) => grounds.map((bg): [string, string] => [fg, bg])),
  ...[...grounds, 'accent-soft'].map((bg): [string, string] => ['accent', bg]),
  ['accent-ink', 'accent'],
  ...statuses.flatMap((s) => [`${s}-soft`, 'surface', 'surface-2', 'bg'].map((bg): [string, string] => [s, bg])),
  ...[...statuses, 'accent'].map((s): [string, string] => ['ink', `${s}-soft`]),
  ...['log-ink', 'log-dim', 'log-warn', 'log-bad'].map((fg): [string, string] => [fg, 'log-bg']),
];
// The focus ring on every ground, the reconnecting outline, the switcher's ink border.
const ui: [string, string][] = [
  ...grounds.map((bg): [string, string] => ['accent', bg]),
  ['warn', 'surface'],
  ['ink', 'surface-2'],
];

describe('design tokens', () => {
  it('parses every colour token for both themes', () => {
    expect(tokens.size).toBeGreaterThanOrEqual(28);
    for (const name of ['bg', 'surface', 'ink', 'accent', 'ok', 'warn', 'bad', 'human', 'mute', 'channel']) {
      expect(tokens.has(name), name).toBe(true);
    }
  });

  it('computes WCAG contrast', () => {
    expect(contrast('#000000', '#ffffff')).toBeCloseTo(21, 5);
    expect(contrast('#777777', '#ffffff')).toBeCloseTo(4.48, 2);
  });

  for (const [index, theme] of ['light', 'dark'].entries()) {
    it.each(text)(`${theme}: text %s on %s meets 4.5:1`, (fg, bg) => {
      expect(contrast(tokens.get(fg)![index]!, tokens.get(bg)![index]!)).toBeGreaterThanOrEqual(4.5);
    });
    it.each(ui)(`${theme}: UI %s on %s meets 3:1`, (fg, bg) => {
      expect(contrast(tokens.get(fg)![index]!, tokens.get(bg)![index]!)).toBeGreaterThanOrEqual(3);
    });
  }
});
