import { memo, useEffect, useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router-dom';
import { liveUrls } from '../../api/api';
import type { LogLine } from '../../api/schema';
import { formatClock } from '../../i18n/format';
import { useLiveStream, type LiveMessage } from '../../live/useLiveStream';
import { usePreferences } from '../../preferences';
import { LiveIndicator } from '../../shell/LiveIndicator';
import { ScreenPage } from '../ScreenPage';
import { PipelineSelect } from '../parts/PipelineSelect';
import { button, field } from '../parts/styles';

// The tail keeps this many entries; history stays in the host's log tools (spec 10.3).
export const MAX_LINES = 1000;
// Interaction notes: paint at most every 250 ms.
export const PAINT_MS = 250;

type Entry = { id: number; kind: 'line'; line: LogLine } | { id: number; kind: 'gap'; from?: string; to?: string };

const scopes = ['run', 'step', 'item'] as const;
type Scope = (typeof scopes)[number];

const levelTone: Record<string, string> = { WARN: 'text-log-warn', ERROR: 'text-log-bad', FATAL: 'text-log-bad' };

// The live log tail (/live/logs), filtered by pipeline, run, step and item in the URL. Best effort: after a
// reconnection the server marks lost lines with a gap, shown as a dashed rule with the missing window. Scrolling
// follows new lines unless paused; a paused tail counts what arrived.
export function LogsScreen() {
  const { t } = useTranslation();
  const [search, setSearch] = useSearchParams();
  const ids = useId();
  const filter = {
    pipeline: search.get('pipeline') ?? '',
    run: search.get('run') ?? '',
    step: search.get('step') ?? '',
    item: search.get('item') ?? '',
  };
  const url = liveUrls.logs({
    ...(filter.pipeline && { pipeline: filter.pipeline }),
    ...(filter.run && { run: filter.run }),
    ...(filter.step && { step: filter.step }),
    ...(filter.item && { item: filter.item }),
  });

  const setFilter = (name: keyof typeof filter, value: string) => {
    const next = new URLSearchParams();
    for (const [key, v] of Object.entries({ ...filter, [name]: value.trim() })) if (v) next.set(key, v);
    setSearch(next);
  };
  const scopeLabels: Record<Scope, string> = { run: t('logs.run'), step: t('logs.step'), item: t('logs.item') };
  return (
    <ScreenPage title={t('nav.logs')}>
      <div className="flex flex-wrap items-end gap-x-4 gap-y-3">
        <label className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
          {t('logs.pipeline')}
          <PipelineSelect
            value={filter.pipeline}
            onChange={(p) => setFilter('pipeline', p)}
            className="min-h-[34px] font-normal"
          />
        </label>
        {scopes.map((scope) => (
          <label key={scope} htmlFor={`${ids}-${scope}`} className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
            {scopeLabels[scope]}
            <input
              id={`${ids}-${scope}`}
              key={`${scope}-${filter[scope]}`}
              defaultValue={filter[scope]}
              placeholder={t('logs.any')}
              onBlur={(e) => e.currentTarget.value.trim() !== filter[scope] && setFilter(scope, e.currentTarget.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') setFilter(scope, e.currentTarget.value);
              }}
              className={`${field} min-h-[34px] w-40 font-mono font-normal`}
            />
          </label>
        ))}
      </div>

      {/* A new filter is a new tail. */}
      <Tail key={url} url={url} />
    </ScreenPage>
  );
}

// One stream's tail: its lines and gaps, painted in batches, paused or following.
function Tail({ url }: { url: string }) {
  const { t } = useTranslation();
  const [entries, setEntries] = useState<Entry[]>([]);
  const [frozenAt, setFrozenAt] = useState<number | null>(null);
  const pending = useRef<Entry[]>([]);
  const nextId = useRef(1);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const tail = useRef<HTMLDivElement>(null);
  // The last line's time, where the next gap's window starts.
  const lastTs = useRef<string | undefined>(undefined);

  useEffect(
    () => () => {
      if (timer.current !== null) clearTimeout(timer.current);
    },
    [],
  );

  const paint = () => {
    timer.current = null;
    const batch = pending.current;
    pending.current = [];
    setEntries((current) => {
      const merged = [...current];
      for (const entry of batch) {
        const last = merged.at(-1);
        // The first line after a gap closes its window.
        if (entry.kind === 'line' && last?.kind === 'gap' && last.to === undefined) {
          merged[merged.length - 1] = { ...last, to: entry.line.ts };
        }
        merged.push(entry);
      }
      return merged.slice(-MAX_LINES);
    });
  };

  const onMessage = (message: LiveMessage) => {
    if (message.type === 'log') {
      try {
        const line = JSON.parse(message.data) as LogLine;
        pending.current.push({ id: nextId.current++, kind: 'line', line });
        lastTs.current = line.ts;
      } catch {
        return;
      }
    } else if (message.type === 'gap') {
      pending.current.push({ id: nextId.current++, kind: 'gap', ...(lastTs.current && { from: lastTs.current }) });
    } else {
      return;
    }
    if (timer.current === null) timer.current = setTimeout(paint, PAINT_MS);
  };

  const live = useLiveStream(url, { events: ['log', 'gap'], onMessage });

  const shown = frozenAt === null ? entries : entries.filter((e) => e.id <= frozenAt);
  const waiting = entries.length - shown.length;

  useEffect(() => {
    if (frozenAt === null && tail.current) tail.current.scrollTop = tail.current.scrollHeight;
  }, [entries, frozenAt]);

  const clear = () => {
    pending.current = [];
    lastTs.current = undefined;
    setEntries([]);
    setFrozenAt(null);
  };

  return (
    <>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <LiveIndicator state={live} />
        <button
          type="button"
          aria-pressed={frozenAt !== null}
          onClick={() => setFrozenAt(frozenAt === null ? (entries.at(-1)?.id ?? 0) : null)}
          className={button.secondary}
        >
          {frozenAt === null ? t('logs.pause') : t('logs.resume')}
        </button>
        <button type="button" onClick={clear} className={button.secondary}>
          {t('logs.clear')}
        </button>
      </div>
      <div className="relative">
        <div
          ref={tail}
          role="log"
          aria-live="off"
          aria-label={t('logs.tail')}
          // Scrollable, so it takes focus for keyboard scrolling.
          // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
          tabIndex={0}
          className="h-[min(60vh,560px)] overflow-y-auto rounded-1 bg-log-bg px-3 py-2.5 font-mono text-tiny leading-[19px] text-log-ink"
        >
          {shown.length === 0 && <p className="m-0 text-log-dim">{t('logs.empty')}</p>}
          {shown.map((entry) => (
            <LogRow key={entry.id} entry={entry} />
          ))}
        </div>
        {waiting > 0 && (
          <button
            type="button"
            onClick={() => setFrozenAt(null)}
            className="absolute bottom-3 left-1/2 -translate-x-1/2 rounded-pill bg-accent px-3 py-1 text-tiny font-bold text-accent-ink shadow-2"
          >
            {t('logs.newLines', { count: waiting })}
          </button>
        )}
      </div>
    </>
  );
}

// One line or gap of the tail. Entries never change once painted, so a row renders once.
const LogRow = memo(function LogRow({ entry }: { entry: Entry }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const clock = (ts: string) => formatClock(new Date(ts), language, 'millisecond');
  if (entry.kind === 'gap') {
    return (
      <p className="my-1 border-y border-dashed border-log-rule py-1 text-center text-log-warn">
        {entry.from && entry.to ? t('logs.gapWindow', { from: clock(entry.from), to: clock(entry.to) }) : t('logs.gap')}
      </p>
    );
  }
  const level = entry.line.level.toUpperCase();
  return (
    <p className={`m-0 flex gap-2.5 ${levelTone[level] ?? ''}`}>
      <span className="flex-none text-log-dim">{clock(entry.line.ts)}</span>
      <span className="w-11 flex-none font-bold">{level}</span>
      <span className="min-w-0 break-words">
        {entry.line.step && <span className="text-log-dim">{entry.line.step} · </span>}
        {entry.line.message}
      </span>
    </p>
  );
});
