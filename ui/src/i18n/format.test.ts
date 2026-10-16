import { describe, expect, it } from 'vitest';
import { formatDateTime, formatDuration, formatMoney, formatNumber, formatRelativeTime } from './format';

const nb = ' ';
const nnb = ' ';

describe('formatNumber', () => {
  it('groups thousands per locale', () => {
    expect(formatNumber(1240, 'en')).toBe('1,240');
    expect(formatNumber(1240, 'fr')).toBe(`1${nnb}240`);
    expect(formatNumber(50214, 'en')).toBe('50,214');
    expect(formatNumber(2.5, 'fr')).toBe('2,5');
  });
});

describe('formatMoney', () => {
  it.each([
    [0.02, '$0.02', `0,02${nb}$`],
    [9.84, '$9.84', `9,84${nb}$`],
    [13.861, '$13.86', `13,86${nb}$`],
    [0.0042, '$0.0042', `0,0042${nb}$`],
    [0, '$0.00', `0,00${nb}$`],
    [1240.5, '$1,240.50', `1${nnb}240,50${nb}$`],
  ])('%s USD', (usd, en, fr) => {
    expect(formatMoney(usd, 'en')).toBe(en);
    expect(formatMoney(usd, 'fr')).toBe(fr);
  });
});

describe('formatDuration', () => {
  it.each([
    [8, `8${nb}ms`, `8${nb}ms`],
    [999.6, `1,000${nb}ms`, `1${nnb}000${nb}ms`],
    [2400, `2.4${nb}s`, `2,4${nb}s`],
    [3000, `3${nb}s`, `3${nb}s`],
    [42_000, `42${nb}s`, `42${nb}s`],
    [192_000, `3${nb}min${nb}12${nb}s`, `3${nb}min${nb}12${nb}s`],
    [180_000, `3${nb}min`, `3${nb}min`],
    [5 * 3600_000 + 4 * 60_000, `5${nb}h${nb}4${nb}min`, `5${nb}h${nb}4${nb}min`],
    [(2 * 24 + 4) * 3600_000, `2${nb}d${nb}4${nb}h`, `2${nb}j${nb}4${nb}h`],
    [-5, `0${nb}ms`, `0${nb}ms`],
  ])('%s ms', (ms, en, fr) => {
    expect(formatDuration(ms, 'en')).toBe(en);
    expect(formatDuration(ms, 'fr')).toBe(fr);
  });
});

describe('formatRelativeTime', () => {
  const now = new Date('2026-10-15T06:30:00Z');
  const ago = (seconds: number) => new Date(now.getTime() - seconds * 1000);
  it.each([
    [0, 'now', 'maintenant'],
    [30, '30 seconds ago', `il y a 30 secondes`],
    [4 * 60, '4 minutes ago', `il y a 4 minutes`],
    [3 * 3600, '3 hours ago', `il y a 3 heures`],
    [2 * 86_400, '2 days ago', 'avant-hier'],
    [-60, 'in 1 minute', `dans 1 minute`],
  ])('%s s ago', (seconds, en, fr) => {
    expect(formatRelativeTime(ago(seconds), now, 'en')).toBe(en);
    expect(formatRelativeTime(ago(seconds), now, 'fr')).toBe(fr);
  });
});

describe('formatDateTime', () => {
  it('gives the absolute time for hover', () => {
    const date = new Date('2026-10-15T06:12:00Z');
    expect(formatDateTime(date, 'en', 'UTC')).toBe('Oct 15, 2026, 6:12:00 AM');
    expect(formatDateTime(date, 'fr', 'UTC')).toBe('15 oct. 2026, 06:12:00');
    expect(formatDateTime(date, 'en')).toContain('2026');
  });
});
