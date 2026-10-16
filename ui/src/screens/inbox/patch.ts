// Editing a review's payload within its editable JSON pointers (RFC 6901), and the JSON Patch (RFC 6902) an
// approval sends.

export function segments(pointer: string): string[] {
  if (pointer === '') return [];
  return pointer
    .slice(1)
    .split('/')
    .map((s) => s.replaceAll('~1', '/').replaceAll('~0', '~'));
}

// The value at a pointer, or found: false when the document has none there.
export function lookup(doc: unknown, pointer: string): { found: boolean; value: unknown } {
  let current: unknown = doc;
  for (const segment of segments(pointer)) {
    if (current === null || typeof current !== 'object' || !Object.hasOwn(current, segment)) {
      return { found: false, value: undefined };
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return { found: true, value: current };
}

// A payload's members, when it is a JSON object; none otherwise.
export function payloadEntries(item: { payload?: unknown }): [string, unknown][] {
  const p = item.payload;
  return p !== null && typeof p === 'object' && !Array.isArray(p) ? Object.entries(p as Record<string, unknown>) : [];
}

// How a field is edited, from the value it holds: numbers and text in an input, booleans as a checkbox, objects and
// arrays as JSON. A missing or null value is edited as text.
export type FieldKind = 'number' | 'text' | 'boolean' | 'json';

export function fieldKind(value: unknown): FieldKind {
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (value !== null && typeof value === 'object') return 'json';
  return 'text';
}

// The text a field starts with: French numbers use a decimal comma.
export function initialText(value: unknown, decimalComma: boolean): string {
  switch (fieldKind(value)) {
    case 'number':
      return decimalComma ? String(value).replace('.', ',') : String(value);
    case 'json':
      return JSON.stringify(value, null, 2);
    case 'boolean':
      return String(value);
    default:
      return typeof value === 'string' ? value : '';
  }
}

export type Parsed = { ok: true; value: unknown } | { ok: false; error: 'number' | 'json' };

export function parseField(kind: FieldKind, text: string): Parsed {
  switch (kind) {
    case 'number': {
      const n = Number(text.trim().replace(',', '.'));
      return text.trim() === '' || !Number.isFinite(n) ? { ok: false, error: 'number' } : { ok: true, value: n };
    }
    case 'boolean':
      return { ok: true, value: text === 'true' };
    case 'json':
      try {
        return { ok: true, value: JSON.parse(text) as unknown };
      } catch {
        return { ok: false, error: 'json' };
      }
    default:
      return { ok: true, value: text };
  }
}

export interface Change {
  pointer: string;
  before: unknown;
  after: unknown;
  found: boolean;
}

// The fields whose value differs from the payload's. A null or missing value left empty is unchanged.
export function changes(payload: unknown, edits: Record<string, Parsed>): Change[] {
  const out: Change[] = [];
  for (const [pointer, parsed] of Object.entries(edits)) {
    if (!parsed.ok) continue;
    const { found, value } = lookup(payload, pointer);
    if ((value === null || value === undefined) && parsed.value === '') continue;
    if (JSON.stringify(value) === JSON.stringify(parsed.value)) continue;
    out.push({ pointer, before: value, after: parsed.value, found });
  }
  return out;
}

// A type, not an interface, so it fits the API's patch items ({[key: string]: unknown}).
// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export type PatchOp = {
  op: 'replace' | 'add';
  path: string;
  value: unknown;
};

export function toPatch(list: Change[]): PatchOp[] {
  return list.map((c) => ({ op: c.found ? 'replace' : 'add', path: c.pointer, value: c.after }));
}
