import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Attempt, Delivery } from '../../api/schema';
import { ErrorText } from '../../common/Copy';
import { QueryView } from '../../common/States';
import { useResource } from '../../common/useResource';
import { StatusGlyph, useStatusLabel, type StatusKey } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import { AttemptTranscript } from './Transcript';

const outcomeStatus: Record<Attempt['outcome'], StatusKey> = {
  running: 'running',
  done: 'done',
  retry: 'failed',
  dead: 'dead',
  filtered: 'filtered',
  cancelled: 'cancelled',
  leaseExpired: 'failed',
  awaitingReview: 'awaitingReview',
};

const toneText: Record<string, string> = {
  running: 'text-accent',
  leased: 'text-accent',
  pending: 'text-mute',
  awaitingReview: 'text-human',
  done: 'text-ok',
  filtered: 'text-mute',
  failed: 'text-warn',
  dead: 'text-bad',
  cancelled: 'text-mute',
};

// One line of what a delivery did, after its state word.
function useNote() {
  const { t } = useTranslation();
  const f = useFormat();
  return (d: Delivery): string => {
    switch (d.state) {
      case 'done':
        // Counted from two attempts on, so always plural.
        return d.attempt > 1 ? t('item.journey.attempts', { n: f.num(d.attempt) }) : t('item.journey.doneFirst');
      case 'filtered':
        return d.filterReason ? t('item.journey.filteredBy', { reason: d.filterReason }) : t('item.journey.filtered');
      case 'dead':
        return [d.errorClass, d.errorMessage?.split('\n')[0]].filter(Boolean).join(': ');
      case 'awaitingReview':
        return t('item.journey.inInbox');
      case 'leased':
        return d.leaseOwner ? t('item.journey.leasedBy', { owner: d.leaseOwner }) : t('item.journey.leased');
      case 'pending':
        return d.nextAttemptAt
          ? t('item.journey.retryAt', { time: f.time(d.nextAttemptAt) })
          : t('item.journey.pending');
      default:
        return t('item.journey.cancelled');
    }
  };
}

function AttemptRow({ pipeline, delivery, attempt }: { pipeline: string; delivery: string; attempt: Attempt }) {
  const { t } = useTranslation();
  const f = useFormat();
  const statusLabel = useStatusLabel();
  const status = outcomeStatus[attempt.outcome];
  const transcript = attempt.artifacts.some((a) => a.name === 'transcript.json');
  return (
    <li className="flex flex-col gap-2.5">
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-tiny text-ink-2">
        <span className="inline-flex items-center gap-1.5 rounded-1 border border-line px-2 py-px">
          <StatusGlyph status={status} size={11} />
          <span className={`font-bold ${toneText[status] ?? ''}`}>
            {t('item.journey.attempt', { n: f.num(attempt.attempt) })} ·{' '}
            {attempt.outcome === 'leaseExpired' ? t('item.journey.leaseExpired') : statusLabel(status)}
          </span>
        </span>
        {attempt.errorClass !== undefined && <span className="font-mono">{attempt.errorClass}</span>}
        <span className="ml-auto font-mono">
          {f.duration(attempt.workMs)}
          {attempt.costUsd > 0 && ` · ${f.money(attempt.costUsd)}`}
        </span>
      </div>
      {attempt.error !== undefined && attempt.error !== '' && <ErrorText text={attempt.error} />}
      {transcript && <AttemptTranscript pipeline={pipeline} delivery={delivery} attempt={attempt.attempt} />}
    </li>
  );
}

// A delivery's attempts, read when its row opens (the list reads deliveries without them).
function Attempts({ pipeline, delivery }: { pipeline: string; delivery: string }) {
  const { t } = useTranslation();
  const read = useResource(`delivery:${pipeline}:${delivery}`, (api) => api.getDelivery(pipeline, delivery));
  return (
    <QueryView query={read} title={t('item.journey.attemptsError')} rows={1} className="h-10">
      {({ attempts = [] }) =>
        attempts.length === 0 ? (
          <p className="m-0 text-small text-ink-2">{t('item.journey.noAttempts')}</p>
        ) : (
          <ol className="m-0 flex list-none flex-col gap-4 p-0">
            {attempts.map((a) => (
              <AttemptRow key={a.attempt} pipeline={pipeline} delivery={delivery} attempt={a} />
            ))}
          </ol>
        )
      }
    </QueryView>
  );
}

// The item's journey (canvas): each step that produced it or read it, oldest first, with its state, what it did,
// how long, what it cost and when; a row opens on its attempts, their errors and the agent's transcript.
export function Journey({ pipeline, deliveries, now }: { pipeline: string; deliveries: Delivery[]; now: number }) {
  const { t } = useTranslation();
  const f = useFormat();
  const statusLabel = useStatusLabel();
  const note = useNote();
  // A dead delivery opens by itself, as that is usually why someone is here; a press flips a row from its default.
  const [toggled, setToggled] = useState<ReadonlySet<string>>(new Set());
  if (deliveries.length === 0) return <p className="m-0 px-4 py-6 text-small text-ink-2">{t('item.journey.empty')}</p>;
  const toggle = (id: string) => {
    const next = new Set(toggled);
    if (!next.delete(id)) next.add(id);
    setToggled(next);
  };
  return (
    <ol className="m-0 list-none p-0 py-2">
      {deliveries.map((d) => {
        const expanded = (d.state === 'dead') !== toggled.has(d.id);
        const status: StatusKey = d.state;
        // A delivery in review shows how long it has waited, as of the last read.
        const ms = d.state === 'awaitingReview' && d.leasedAt ? now - Date.parse(d.leasedAt) : d.workMs;
        return (
          <li key={d.id} className={`border-b border-line last:border-b-0 ${expanded ? 'bg-surface-2' : ''}`}>
            <button
              type="button"
              aria-expanded={expanded}
              aria-controls={`attempts-${d.id}`}
              onClick={() => toggle(d.id)}
              className="grid w-full grid-cols-[18px_minmax(0,1fr)_auto] items-center gap-x-2.5 gap-y-1 px-4 py-3 text-left md:grid-cols-[22px_minmax(0,150px)_minmax(0,1fr)_72px_64px_72px] md:gap-x-3 md:py-[9px]"
            >
              <StatusGlyph status={status} size={14} />
              <span className="min-w-0 font-mono text-small font-bold break-words">{d.step}</span>
              <span className="col-start-2 col-end-4 row-start-2 min-w-0 text-small break-words text-ink-2 md:col-auto md:row-auto">
                <strong className={toneText[status] ?? ''}>{statusLabel(status)}</strong>
                {note(d) !== '' && ` · ${note(d)}`}
              </span>
              <span className="col-start-3 row-start-1 text-right font-mono text-tiny md:col-auto md:row-auto">
                {f.duration(ms)}
              </span>
              <span className="hidden text-right font-mono text-tiny text-ink-2 md:block">
                {d.costUsd > 0 ? f.money(d.costUsd) : '—'}
              </span>
              <time
                dateTime={d.createdAt}
                title={f.dateTime(d.createdAt)}
                className="hidden text-right font-mono text-tiny text-ink-3 md:block"
              >
                {f.time(d.createdAt)}
              </time>
            </button>
            {expanded && (
              <div id={`attempts-${d.id}`} className="px-4 pt-1 pb-4 md:pl-[50px]">
                <Attempts pipeline={pipeline} delivery={d.id} />
              </div>
            )}
          </li>
        );
      })}
    </ol>
  );
}
