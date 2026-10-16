import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { LogLine } from '../api/schema';
import { parseData, useLiveStream, type LiveMessage } from '../live/useLiveStream';
import { ScrollRegion } from './ScrollRegion';
import { buttons } from './buttons';
import { useFormat } from './useFormat';

type Entry = { kind: 'line'; id: string; line: LogLine } | { kind: 'gap'; id: string };

// Lines kept in memory: a tail, not a history (the host's log tools keep that).
const MAX_LINES = 500;

const levelClass: Record<string, string> = {
  WARN: 'text-[#e8ae45]',
  ERROR: 'text-[#f2806f]',
};

// A live log tail over /live/logs (Components board): dark, monospace, a dashed gap marker where the stream dropped
// lines, and Pause scroll. Best effort: the server keeps a ring, so a long disconnection loses lines.
export function LogTail({ url, title }: { url: string | null; title: string }) {
  const { t } = useTranslation();
  const f = useFormat();
  const [entries, setEntries] = useState<Entry[]>([]);
  const [paused, setPaused] = useState(false);
  const box = useRef<HTMLDivElement>(null);
  const seq = useRef(0);

  useLiveStream(url, {
    events: ['log', 'gap'],
    onMessage: (message: LiveMessage) => {
      seq.current += 1;
      const id = message.id ?? `local-${seq.current}`;
      const line = message.type === 'log' ? parseData<LogLine>(message) : null;
      const entry: Entry | null =
        message.type === 'gap' ? { kind: 'gap', id } : line === null ? null : { kind: 'line', id, line };
      if (entry !== null) setEntries((current) => [...current, entry].slice(-MAX_LINES));
    },
  });

  useEffect(() => {
    const el = box.current;
    if (!paused && el) el.scrollTop = el.scrollHeight;
  }, [entries, paused]);

  return (
    <section aria-label={title} className="flex flex-col gap-2">
      <div className="flex items-center gap-2.5">
        <h3 className="m-0 text-ui font-bold">{title}</h3>
        <div className="flex-1" />
        <button
          type="button"
          className={buttons.smallSecondary}
          aria-pressed={paused}
          onClick={() => setPaused(!paused)}
        >
          {t('kit.pauseScroll')}
        </button>
      </div>
      <ScrollRegion
        ref={box}
        role="log"
        live="off"
        label={title}
        className="max-h-80 overflow-auto rounded-1 bg-log-bg px-3 py-2.5 font-mono text-tiny leading-[19px] text-log-ink"
      >
        {entries.length === 0 && <p className="m-0">{t('kit.noLogs')}</p>}
        {entries.map((entry) =>
          entry.kind === 'gap' ? (
            <div key={entry.id} className="my-1 border-y border-dashed border-log-ink/50 py-0.5 text-center">
              {t('kit.logGap')}
            </div>
          ) : (
            <div key={entry.id} className={`flex gap-2.5 ${levelClass[entry.line.level.toUpperCase()] ?? ''}`}>
              <span className="flex-none opacity-80">{f.time(entry.line.ts)}</span>
              <span className="flex-none font-bold">{entry.line.level.toUpperCase()}</span>
              <span className="min-w-0 break-words [overflow-wrap:anywhere]">
                {entry.line.step !== '' && <span className="opacity-80">{entry.line.step} · </span>}
                {entry.line.message}
              </span>
            </div>
          ),
        )}
      </ScrollRegion>
    </section>
  );
}
