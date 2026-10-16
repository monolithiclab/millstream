import { useMemo } from 'react';
import {
  formatClock,
  formatDateTime,
  formatDuration,
  formatMoney,
  formatNumber,
  formatPercent,
  formatRelativeTime,
  pluralCategory,
} from '../i18n/format';
import { usePreferences } from '../preferences';

// The formatting of src/i18n/format.ts bound to the current language, plus the plural category, so a screen picks
// between two literal keys (the i18n test finds used keys as quoted literals, so plural suffixes are not used).
export function useFormat() {
  const { language } = usePreferences();
  return useMemo(
    () => ({
      language,
      num: (value: number, options?: Intl.NumberFormatOptions) => formatNumber(value, language, options),
      percent: (ratio: number) => formatPercent(ratio, language, ratio < 0.1 && ratio > 0 ? 1 : 0),
      money: (usd: number) => formatMoney(usd, language),
      duration: (ms: number) => formatDuration(ms, language),
      relative: (iso: string, now = new Date()) => formatRelativeTime(new Date(iso), now, language),
      dateTime: (iso: string) => formatDateTime(new Date(iso), language),
      clock: (iso: string) => formatClock(new Date(iso), language),
      time: (iso: string) => formatClock(new Date(iso), language, 'second'),
      plural: <K extends string>(count: number, one: K, other: K): K =>
        pluralCategory(count, language) === 'one' ? one : other,
    }),
    [language],
  );
}

export type Format = ReturnType<typeof useFormat>;

// The UTF-8 size of a value's JSON, for "1.8 KB" labels.
export function byteSize(text: string): number {
  return new TextEncoder().encode(text).length;
}

const byteUnits = { en: ['B', 'KB', 'MB'], fr: ['o', 'Ko', 'Mo'] } as const;

// 812 B, 1.8 KB, 3.6 MB; French 1,8 Ko.
export function formatBytes(bytes: number, f: Format): string {
  const [b, kb, mb] = byteUnits[f.language];
  if (bytes < 1024) return `${f.num(bytes)}\u00a0${b}`;
  if (bytes < 1024 * 1024) return `${f.num(bytes / 1024, { maximumFractionDigits: 1 })}\u00a0${kb}`;
  return `${f.num(bytes / (1024 * 1024), { maximumFractionDigits: 1 })}\u00a0${mb}`;
}
