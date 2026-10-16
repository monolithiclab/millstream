import { describe, expect, it } from 'vitest';
import { changes, fieldKind, initialText, lookup, parseField, segments, toPatch } from './patch';

const payload = { marginPct: 17.8, notes: null, tags: ['a'], flags: { lift: false }, 'a/b': { '~x': 1 } };

describe('pointers', () => {
  it('splits and unescapes', () => {
    expect(segments('')).toEqual([]);
    expect(segments('/a~1b/~0x')).toEqual(['a/b', '~x']);
  });

  it('looks values up', () => {
    expect(lookup(payload, '/marginPct')).toEqual({ found: true, value: 17.8 });
    expect(lookup(payload, '/a~1b/~0x')).toEqual({ found: true, value: 1 });
    expect(lookup(payload, '/notes')).toEqual({ found: true, value: null });
    expect(lookup(payload, '/missing/deeper')).toEqual({ found: false, value: undefined });
    expect(lookup(payload, '/notes/x')).toEqual({ found: false, value: undefined });
    expect(lookup(payload, '')).toEqual({ found: true, value: payload });
  });
});

describe('fields', () => {
  it('picks an editor per value', () => {
    expect([17.8, true, ['a'], { x: 1 }, null, 'text', undefined].map(fieldKind)).toEqual([
      'number',
      'boolean',
      'json',
      'json',
      'text',
      'text',
      'text',
    ]);
  });

  it('starts from the value, French numbers with a decimal comma', () => {
    expect(initialText(17.8, true)).toBe('17,8');
    expect(initialText(17.8, false)).toBe('17.8');
    expect(initialText({ x: 1 }, false)).toBe('{\n  "x": 1\n}');
    expect(initialText(false, false)).toBe('false');
    expect(initialText(null, false)).toBe('');
    expect(initialText('hi', false)).toBe('hi');
  });

  it('parses what was typed', () => {
    expect(parseField('number', '16,5')).toEqual({ ok: true, value: 16.5 });
    expect(parseField('number', ' 16.5 ')).toEqual({ ok: true, value: 16.5 });
    expect(parseField('number', '')).toEqual({ ok: false, error: 'number' });
    expect(parseField('number', 'abc')).toEqual({ ok: false, error: 'number' });
    expect(parseField('boolean', 'true')).toEqual({ ok: true, value: true });
    expect(parseField('json', '[1]')).toEqual({ ok: true, value: [1] });
    expect(parseField('json', '[1')).toEqual({ ok: false, error: 'json' });
    expect(parseField('text', 'x')).toEqual({ ok: true, value: 'x' });
  });
});

describe('changes and patch', () => {
  it('keeps only real changes and builds replace or add operations', () => {
    const list = changes(payload, {
      '/marginPct': { ok: true, value: 16.5 },
      '/notes': { ok: true, value: 'No lift' },
      '/tags': { ok: true, value: ['a'] },
      '/flags/lift': { ok: false, error: 'json' },
      '/new': { ok: true, value: 'x' },
      '/empty': { ok: true, value: '' },
    });
    expect(list.map((c) => c.pointer)).toEqual(['/marginPct', '/notes', '/new']);
    expect(toPatch(list)).toEqual([
      { op: 'replace', path: '/marginPct', value: 16.5 },
      { op: 'replace', path: '/notes', value: 'No lift' },
      { op: 'add', path: '/new', value: 'x' },
    ]);
  });
});
