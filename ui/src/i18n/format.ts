import type { Language } from './index';

// Number, money, duration and time formatting per the canvas interaction notes: Intl everywhere, so French gets
// its narrow no-break group separator (1 240) and decimal comma, and English 1,240.

const intlLocale: Record<Language, string> = { en: 'en-US', fr: 'fr-FR' };
const NBSP = ' ';

// Building an Intl.NumberFormat is the costly part: lists format hundreds of figures per render.
const numberFormats = new Map<string, Intl.NumberFormat>();

function numberFormat(language: Language, options: Intl.NumberFormatOptions = {}): Intl.NumberFormat {
  const key = `${language}|${JSON.stringify(options)}`;
  let format = numberFormats.get(key);
  if (format === undefined) {
    format = new Intl.NumberFormat(intlLocale[language], options);
    numberFormats.set(key, format);
  }
  return format;
}

const dateFormats = new Map<string, Intl.DateTimeFormat>();

function dateFormat(language: Language, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `${language}|${JSON.stringify(options)}`;
  let format = dateFormats.get(key);
  if (format === undefined) {
    format = new Intl.DateTimeFormat(intlLocale[language], options);
    dateFormats.set(key, format);
  }
  return format;
}

export function formatNumber(value: number, language: Language, options?: Intl.NumberFormatOptions): string {
  return numberFormat(language, options).format(value);
}

// USD, with four decimals under one cent (an agent call costs fractions of a cent), otherwise cents: $0.02, 0,02 $.
export function formatMoney(usd: number, language: Language): string {
  const digits = usd !== 0 && Math.abs(usd) < 0.01 ? 4 : 2;
  return numberFormat(language, {
    style: 'currency',
    currency: 'USD',
    currencyDisplay: 'narrowSymbol',
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(usd);
}

const durationUnits: Record<Language, { ms: string; s: string; min: string; h: string; d: string }> = {
  en: { ms: 'ms', s: 's', min: 'min', h: 'h', d: 'd' },
  fr: { ms: 'ms', s: 's', min: 'min', h: 'h', d: 'j' },
};

// One or two units: 8 ms, 2.4 s, 42 s, 3 min 12 s, 5 h 4 min, 2 d 4 h (FR: 2 j 4 h). A zero second unit is dropped.
export function formatDuration(ms: number, language: Language): string {
  const u = durationUnits[language];
  const part = (value: number, unit: string) => `${formatNumber(value, language)}${NBSP}${unit}`;
  const pair = (major: number, majorUnit: string, minor: number, minorUnit: string) =>
    minor === 0 ? part(major, majorUnit) : `${part(major, majorUnit)}${NBSP}${part(minor, minorUnit)}`;
  const value = Math.max(0, ms);
  if (value < 1000) return part(Math.round(value), u.ms);
  if (value < 10_000) {
    return `${formatNumber(Math.floor(value / 100) / 10, language, { maximumFractionDigits: 1 })}${NBSP}${u.s}`;
  }
  const total = Math.round(value / 1000);
  if (total < 60) return part(total, u.s);
  if (total < 3600) return pair(Math.floor(total / 60), u.min, total % 60, u.s);
  if (total < 86_400) return pair(Math.floor(total / 3600), u.h, Math.floor((total % 3600) / 60), u.min);
  return pair(Math.floor(total / 86_400), u.d, Math.floor((total % 86_400) / 3600), u.h);
}

// Relative time for lists ("4 minutes ago", "il y a 4 minutes"); formatDateTime gives the absolute value for hover.
export function formatRelativeTime(date: Date, now: Date, language: Language): string {
  const format = new Intl.RelativeTimeFormat(intlLocale[language], { numeric: 'auto' });
  const seconds = (date.getTime() - now.getTime()) / 1000;
  const abs = Math.abs(seconds);
  if (abs < 60) return format.format(Math.round(seconds), 'second');
  if (abs < 3600) return format.format(Math.round(seconds / 60), 'minute');
  if (abs < 86_400) return format.format(Math.round(seconds / 3600), 'hour');
  return format.format(Math.round(seconds / 86_400), 'day');
}

export function formatDateTime(date: Date, language: Language, timeZone?: string): string {
  return dateFormat(language, {
    dateStyle: 'medium',
    timeStyle: 'medium',
    timeZone,
  }).format(date);
}

// A time of day, 06:31 in both languages (24-hour clock), to the second or the millisecond for log lines.
export function formatClock(
  date: Date,
  language: Language,
  precision: 'minute' | 'second' | 'millisecond' = 'minute',
  timeZone?: string,
): string {
  return dateFormat(language, {
    hour: '2-digit',
    minute: '2-digit',
    second: precision === 'minute' ? undefined : '2-digit',
    fractionalSecondDigits: precision === 'millisecond' ? 3 : undefined,
    hourCycle: 'h23',
    timeZone,
  }).format(date);
}

// A short day: Thu 15 Oct, jeu. 15 oct.
export function formatDay(date: Date, language: Language, timeZone?: string): string {
  return dateFormat(language, {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    timeZone,
  }).format(date);
}

// A share as a percentage: 34 %, 0.5 → 50 %.
export function formatPercent(share: number, language: Language, digits = 0): string {
  return formatNumber(share, language, { style: 'percent', maximumFractionDigits: digits });
}
