import { describe, expect, it } from 'vitest';
import { catalogues, createI18n, isLanguage } from './index';

interface Tree {
  [key: string]: string | Tree;
}

function flatten(tree: Tree, prefix = ''): Map<string, string> {
  const out = new Map<string, string>();
  for (const [key, value] of Object.entries(tree)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof value === 'string') out.set(path, value);
    else for (const [k, v] of flatten(value, path)) out.set(k, v);
  }
  return out;
}

const en = flatten(catalogues.en);
const fr = flatten(catalogues.fr);

// Every source file but tests, as text: a key is used when it appears as a quoted literal.
const sources = Object.entries(
  import.meta.glob<string>(['../**/*.{ts,tsx}', '!../**/*.test.{ts,tsx}', '!../test/**'], {
    query: '?raw',
    import: 'default',
    eager: true,
  }),
);

const placeholders = (text: string) => [...text.matchAll(/\{\{(\w+)\}\}/g)].map((m) => m[1]).sort();

describe('catalogues', () => {
  it('have the same keys in English and French', () => {
    const missingInFr = [...en.keys()].filter((k) => !fr.has(k));
    const missingInEn = [...fr.keys()].filter((k) => !en.has(k));
    expect({ missingInFr, missingInEn }).toEqual({ missingInFr: [], missingInEn: [] });
  });

  it('have no empty value and the same interpolations in both languages', () => {
    for (const [key, value] of en) {
      expect(value.trim(), key).not.toBe('');
      expect(fr.get(key)?.trim(), key).not.toBe('');
      expect(placeholders(fr.get(key) ?? ''), key).toEqual(placeholders(value));
    }
  });

  it('have no unused key', () => {
    const text = sources.map(([, source]) => source).join('\n');
    const unused = [...en.keys()].filter((key) => !text.includes(`'${key}'`));
    expect(unused).toEqual([]);
  });

  it('define every key the code passes to t()', () => {
    const used = sources.flatMap(([file, source]) =>
      [...source.matchAll(/\bt\(\s*'([\w.]+)'/g)].map((m) => [file, m[1]] as const),
    );
    expect(used.length).toBeGreaterThan(10);
    expect(used.filter(([, key]) => !en.has(key!))).toEqual([]);
  });

  // Espace insécable (U+00A0) or fine (U+202F) before : ; ! ? » and after «, never a plain space or none.
  it('use French typography', () => {
    const faults: string[] = [];
    for (const [key, raw] of fr) {
      const value = raw.replace(/\{\{\w+\}\}/g, 'x');
      for (const match of value.matchAll(/[:;!?»]/g)) {
        const before = value[match.index - 1];
        const time = match[0] === ':' && /\d/.test(before ?? '') && /\d/.test(value[match.index + 1] ?? '');
        if (!time && before !== ' ' && before !== ' ') faults.push(`${key}: “${raw}”`);
      }
      for (const match of value.matchAll(/«/g)) {
        const after = value[match.index + 1];
        if (after !== ' ' && after !== ' ') faults.push(`${key}: “${raw}”`);
      }
    }
    expect(faults).toEqual([]);
  });
});

describe('createI18n', () => {
  it('translates and interpolates in both languages', () => {
    expect(createI18n('en').t('switcher.label', { name: 'listings-daily' })).toBe(
      'Switch pipeline, showing listings-daily',
    );
    expect(createI18n('fr').t('switcher.label', { name: 'listings-daily' })).toBe(
      'Changer de pipeline, affiché : listings-daily',
    );
  });

  it('recognises languages', () => {
    expect(isLanguage('fr')).toBe(true);
    expect(isLanguage('de')).toBe(false);
    expect(isLanguage(null)).toBe(false);
  });
});
