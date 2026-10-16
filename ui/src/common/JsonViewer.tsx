import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { buttons, card } from './styles';
import { CopyButton } from './Copy';
import { useFormat } from './useFormat';

interface Row {
  depth: number;
  // "key": or nothing (array elements, the root, closing brackets).
  label: string;
  text: string;
  kind: 'open' | 'collapsed' | 'close' | 'string' | 'number' | 'literal';
  path: string;
  comma: boolean;
  // Object or array rows: their child count, to label a collapsed one.
  size?: number;
  array?: boolean;
}

function isContainer(value: unknown): value is Record<string, unknown> | unknown[] {
  return typeof value === 'object' && value !== null;
}

// The rows of a value, nested containers folded when their path is in collapsed.
export function flatten(value: unknown, collapsed: ReadonlySet<string>): Row[] {
  const rows: Row[] = [];
  const walk = (v: unknown, depth: number, label: string, path: string, comma: boolean) => {
    if (!isContainer(v)) {
      const kind = typeof v === 'string' ? 'string' : typeof v === 'number' ? 'number' : 'literal';
      rows.push({ depth, label, text: JSON.stringify(v) ?? 'null', kind, path, comma });
      return;
    }
    const array = Array.isArray(v);
    const entries: [string, unknown][] = array ? v.map((x, i) => [String(i), x]) : Object.entries(v);
    if (collapsed.has(path) || entries.length === 0) {
      rows.push({ depth, label, text: '', kind: 'collapsed', path, comma, size: entries.length, array });
      return;
    }
    rows.push({ depth, label, text: array ? '[' : '{', kind: 'open', path, comma: false, size: entries.length, array });
    entries.forEach(([key, child], i) =>
      walk(child, depth + 1, array ? '' : `${JSON.stringify(key)}: `, `${path}/${key}`, i < entries.length - 1),
    );
    rows.push({ depth, label: '', text: array ? ']' : '}', kind: 'close', path, comma });
  };
  walk(value, 0, '', '', false);
  return rows;
}

// Every container path but the root: "Collapse all" folds them.
function containerPaths(value: unknown, path = '', out: string[] = []): string[] {
  if (!isContainer(value)) return out;
  if (path !== '') out.push(path);
  for (const [key, child] of Array.isArray(value) ? value.map((x, i) => [i, x] as const) : Object.entries(value))
    containerPaths(child, `${path}/${key}`, out);
  return out;
}

const valueClass: Record<Row['kind'], string> = {
  open: 'text-ink-3',
  close: 'text-ink-3',
  collapsed: 'text-ink-3',
  string: 'text-ink',
  number: 'text-accent',
  literal: 'text-accent',
};

// A collapsible JSON viewer (Components board): line numbers, carets that fold objects and arrays, Collapse all,
// Copy JSON. Long strings wrap rather than clip.
export function JsonViewer({ value, title, meta }: { value: unknown; title: string; meta?: string }) {
  const { t } = useTranslation();
  const f = useFormat();
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const rows = useMemo(() => flatten(value, collapsed), [value, collapsed]);
  const all = useMemo(() => containerPaths(value), [value]);
  const toggle = (path: string) => {
    const next = new Set(collapsed);
    if (!next.delete(path)) next.add(path);
    setCollapsed(next);
  };
  const anyCollapsed = collapsed.size > 0;
  const collapsedText = ({ size = 0, array }: Row) => {
    if (size === 0) return array ? '[]' : '{}';
    const n = f.num(size);
    return array
      ? `[ ${t(f.plural(size, 'common.jsonItemsOne', 'common.jsonItemsOther'), { n })} ]`
      : `{ ${t(f.plural(size, 'common.jsonKeysOne', 'common.jsonKeysOther'), { n })} }`;
  };

  return (
    <section aria-label={title} className={card}>
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-line px-3.5 py-2.5">
        <h2 className="m-0 text-ui font-bold">{title}</h2>
        {meta !== undefined && <span className="text-tiny text-ink-3">{meta}</span>}
        <div className="flex-1" />
        {all.length > 0 && (
          <button
            type="button"
            className={buttons.smallSecondary}
            onClick={() => setCollapsed(anyCollapsed ? new Set() : new Set(all))}
          >
            {anyCollapsed ? t('common.expandAll') : t('common.collapseAll')}
          </button>
        )}
        <CopyButton text={JSON.stringify(value, null, 2)} label={t('common.copyJson')} />
      </div>
      <div className="py-2.5 font-mono text-tiny leading-5" data-testid="json">
        {rows.map((row, i) => {
          const toggleable = row.kind === 'open' || (row.kind === 'collapsed' && (row.size ?? 0) > 0);
          const name = row.label.replace(/: $/, '') || row.path || '/';
          return (
            <div key={`${row.path}:${row.kind}`} className="flex px-3.5">
              <span className="w-[26px] flex-none pr-2.5 text-right text-ink-3 select-none" aria-hidden="true">
                {i + 1}
              </span>
              <span className="w-3.5 flex-none">
                {toggleable && row.path !== '' && (
                  <button
                    type="button"
                    aria-expanded={row.kind === 'open'}
                    aria-label={row.kind === 'open' ? t('common.collapse', { name }) : t('common.expand', { name })}
                    onClick={() => toggle(row.path)}
                    className="text-ink-3"
                  >
                    {row.kind === 'open' ? '▾' : '▸'}
                  </button>
                )}
              </span>
              <span
                className="min-w-0 break-words whitespace-pre-wrap [overflow-wrap:anywhere]"
                style={{ paddingLeft: row.depth * 14 }}
              >
                {row.label !== '' && <span className="text-ink-2">{row.label}</span>}
                <span className={valueClass[row.kind]}>{row.kind === 'collapsed' ? collapsedText(row) : row.text}</span>
                {row.comma && <span className="text-ink-3">,</span>}
              </span>
            </div>
          );
        })}
      </div>
    </section>
  );
}
