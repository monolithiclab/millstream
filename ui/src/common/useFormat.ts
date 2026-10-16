import { useMemo } from 'react';
import { formatDuration, formatMoney, formatRelativeTime, intlLocale } from '../i18n/format';
import { usePreferences } from '../preferences';

// The formatting of src/i18n/format.ts bound to the current language, with its Intl formatters built once per
// language and options (the graph formats hundreds of figures per paint), plus the plural category, so a screen
// picks between two literal keys (the i18n test finds used keys as quoted literals, so plural suffixes are not used).
export function useFormat() {
  const { language } = usePreferences();
  return useMemo(() => {
    const locale = intlLocale[language];
    const numbers = new Map<string, Intl.NumberFormat>();
    const number = (options: Intl.NumberFormatOptions = {}) => {
      const key = JSON.stringify(options);
      let format = numbers.get(key);
      if (format === undefined) numbers.set(key, (format = new Intl.NumberFormat(locale, options)));
      return format;
    };
    const clock = new Intl.DateTimeFormat(locale, { hour: '2-digit', minute: '2-digit' });
    const time = new Intl.DateTimeFormat(locale, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const dateTime = new Intl.DateTimeFormat(locale, { dateStyle: 'medium', timeStyle: 'medium' });
    const plurals = new Intl.PluralRules(locale);
    return {
      language,
      num: (value: number, options?: Intl.NumberFormatOptions) => number(options).format(value),
      percent: (ratio: number) =>
        number({ style: 'percent', maximumFractionDigits: ratio < 0.1 && ratio > 0 ? 1 : 0 }).format(ratio),
      money: (usd: number) => formatMoney(usd, language),
      duration: (ms: number) => formatDuration(ms, language),
      relative: (iso: string, now = new Date()) => formatRelativeTime(new Date(iso), now, language),
      dateTime: (iso: string) => dateTime.format(new Date(iso)),
      clock: (iso: string) => clock.format(new Date(iso)),
      time: (iso: string) => time.format(new Date(iso)),
      // one or other, as French counts 0 and 1 as singular and English only 1.
      plural: <K extends string>(count: number, one: K, other: K): K => (plurals.select(count) === 'one' ? one : other),
    };
  }, [language]);
}

export type Format = ReturnType<typeof useFormat>;

// Ids truncate in the middle (interaction notes): it_7f3a…9c21; the full value goes on hover and copy.
export function shortId(id: string): string {
  return id.length <= 14 ? id : `${id.slice(0, 7)}…${id.slice(-4)}`;
}

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
