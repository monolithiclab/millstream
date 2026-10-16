import { memo, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { LogLine } from '../api/schema';
import { formatClock } from '../i18n/format';
import { parseData, useLiveStream, type LiveMessage } from '../live/useLiveStream';
import { usePreferences } from '../preferences';
import { LiveIndicator } from '../shell/LiveIndicator';
import { ScrollRegion } from './ScrollRegion';
import { useThrottle } from './useThrottle';
import { buttons } from './styles';

// The tail keeps this many entries; history stays in the host's log tools (spec 10.3).
export const MAX_LINES = 1000;
// A paused tail keeps what it shows and buffers what arrives, up to this many entries in all; past it, the oldest
// lines that arrived during the pause are dropped, and counted.
export const MAX_PAUSED_LINES = 10_000;
// Interaction notes: paint at most every 250 ms.
export const PAINT_MS = 250;

type Entry = { id: number; kind: 'line'; line: LogLine } | { id: number; kind: 'gap'; from?: string; to?: string };

const levelTone: Record<string, string> = { WARN: 'text-log-warn', ERROR: 'text-log-bad', FATAL: 'text-log-bad' };

// A live log tail over /live/logs (Components board): dark, monospace, painted in batches. Best effort: after a
// reconnection the server marks lost lines with a gap, shown as a dashed rule with the missing window. Scrolling
// follows new lines unless paused; a paused tail keeps its lines, buffers and counts what arrives (a bounded number,
// counting what it had to drop), and shows it on resume. A new url is a new tail.
export function LogTail(props: { url: string; title: string; className?: string }) {
  return <Tail key={props.url} {...props} />;
}

function Tail({ url, title, className = 'max-h-80' }: { url: string; title: string; className?: string }) {
  const { t } = useTranslation();
  const [{ entries, dropped }, setTail] = useState<{ entries: Entry[]; dropped: number }>({ entries: [], dropped: 0 });
  const [frozenAt, setFrozenAt] = useState<number | null>(null);
  const pending = useRef<Entry[]>([]);
  const nextId = useRef(1);
  const tail = useRef<HTMLDivElement>(null);
  // The last line's time, where the next gap's window starts.
  const lastTs = useRef<string | undefined>(undefined);

  const paint = useThrottle(() => {
    const batch = pending.current;
    pending.current = [];
    setTail(({ entries: current, dropped: lost }) => {
      const merged = [...current];
      for (const entry of batch) {
        const last = merged.at(-1);
        // The first line after a gap closes its window.
        if (entry.kind === 'line' && last?.kind === 'gap' && last.to === undefined) {
          merged[merged.length - 1] = { ...last, to: entry.line.ts };
        }
        merged.push(entry);
      }
      if (frozenAt === null) return { entries: merged.slice(-MAX_LINES), dropped: 0 };
      const excess = merged.length - MAX_PAUSED_LINES;
      if (excess <= 0) return { entries: merged, dropped: lost };
      // Paused: what is shown stays; the oldest of what arrived since makes room.
      const since = merged.findIndex((e) => e.id > frozenAt);
      return { entries: [...merged.slice(0, since), ...merged.slice(since + excess)], dropped: lost + excess };
    });
  }, PAINT_MS);

  const onMessage = (message: LiveMessage) => {
    if (message.type === 'log') {
      const line = parseData<LogLine>(message);
      if (line === null) return;
      pending.current.push({ id: nextId.current++, kind: 'line', line });
      lastTs.current = line.ts;
    } else if (message.type === 'gap') {
      pending.current.push({ id: nextId.current++, kind: 'gap', ...(lastTs.current && { from: lastTs.current }) });
    } else {
      return;
    }
    paint();
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
    setTail({ entries: [], dropped: 0 });
    setFrozenAt(null);
  };
  // Resuming shows the newest MAX_LINES, as a tail that never paused.
  const resume = () => {
    setTail((tail) => ({ entries: tail.entries.slice(-MAX_LINES), dropped: 0 }));
    setFrozenAt(null);
  };

  return (
    <div className="flex flex-col gap-2.5">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <LiveIndicator state={live} />
        <button
          type="button"
          aria-pressed={frozenAt !== null}
          onClick={() => (frozenAt === null ? setFrozenAt(entries.at(-1)?.id ?? 0) : resume())}
          className={buttons.secondary}
        >
          {frozenAt === null ? t('logs.pause') : t('logs.resume')}
        </button>
        <button type="button" onClick={clear} className={buttons.secondary}>
          {t('logs.clear')}
        </button>
      </div>
      <div className="relative">
        <ScrollRegion
          ref={tail}
          role="log"
          live="off"
          label={title}
          className={`${className} overflow-y-auto rounded-1 bg-log-bg px-3 py-2.5 font-mono text-tiny leading-[19px] text-log-ink`}
        >
          {shown.length === 0 && <p className="m-0 text-log-dim">{t('logs.empty')}</p>}
          {shown.map((entry) => (
            <LogRow key={entry.id} entry={entry} />
          ))}
        </ScrollRegion>
        {waiting > 0 && (
          <button
            type="button"
            onClick={resume}
            className="absolute bottom-3 left-1/2 -translate-x-1/2 rounded-pill bg-accent px-3 py-1 text-tiny font-bold text-accent-ink shadow-2"
          >
            {dropped > 0
              ? t('logs.newLinesDropped', { count: waiting, dropped })
              : t('logs.newLines', { count: waiting })}
          </button>
        )}
      </div>
    </div>
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
      <span className="min-w-0 break-words [overflow-wrap:anywhere]">
        {entry.line.step && <span className="text-log-dim">{entry.line.step} · </span>}
        {entry.line.message}
      </span>
    </p>
  );
});
