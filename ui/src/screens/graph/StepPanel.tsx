import { useEffect, useRef, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import type { DeadLetterGroup } from '../../api/schema';
import { itemPath, pipelinePath } from '../../routes';
import { buttons } from '../../common/styles';
import { SH, StatusBadge, StatusText } from '../../common/Status';
import { shortId } from '../../common/Id';
import { useInsightKindLabel } from '../../common/insightKind';
import { useFormat } from '../../common/useFormat';
import { overlayOpen } from '../../shell/useDismiss';
import { targetOf, type DeadLetterAction, type DeadLetterTarget } from '../deadLetters/useDeadLetterActions';
import type { StepModel } from './model';
import { KindIcon, useKindLabel } from './nodes';

// Recent failures shown in the panel; the rest are one link away.
const RECENT = 3;

function Stat({ label, value, sub, tone = 'text-ink' }: { label: string; value: string; sub?: string; tone?: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-[12px] bg-surface-2 px-3.5 py-3">
      <dt className="text-tiny font-medium text-ink-3">{label}</dt>
      <dd className={`m-0 font-display text-[30px] leading-[34px] ${tone}`} data-live>
        {value}
      </dd>
      {sub !== undefined && <dd className="m-0 text-tiny text-ink-2">{sub}</dd>}
    </div>
  );
}

// The side panel of a step (canvas): state, live figures, Pause or Resume, its open insight, its recent dead letters
// with Replay and Discard, and links to its items and dead letters. Opening it moves focus to its heading; Esc
// closes it and returns focus to the step.
export function StepPanel({
  pipeline,
  revision,
  model,
  deadLetters,
  busy,
  onClose,
  onPause,
  onDeadLetters,
}: {
  pipeline: string;
  revision: number;
  model: StepModel;
  deadLetters: DeadLetterGroup[];
  busy: boolean;
  onClose: () => void;
  onPause: (paused: boolean) => void;
  onDeadLetters: (action: DeadLetterAction, target: DeadLetterTarget) => void;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const kindLabel = useKindLabel();
  const insightLabel = useInsightKindLabel();
  const heading = useRef<HTMLHeadingElement>(null);
  const fig = model.figures;
  const paused = model.state === 'paused';

  useEffect(() => heading.current?.focus(), [model.name]);

  const onKeyDown = (event: KeyboardEvent) => {
    // A dialog opened from the panel takes Esc first.
    if (event.key === 'Escape' && !overlayOpen()) {
      event.preventDefault();
      onClose();
    }
  };

  const letters = deadLetters
    .flatMap((g) => g.deadLetters.map((d) => ({ ...d, group: g })))
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));

  return (
    // The panel is a landmark the step's button controls; Esc inside it closes it.
    // eslint-disable-next-line jsx-a11y/no-noninteractive-element-interactions
    <aside
      id="step-panel"
      aria-labelledby="step-panel-title"
      onKeyDown={onKeyDown}
      className="fixed inset-0 z-30 flex flex-col overflow-auto bg-surface md:static md:z-auto md:w-[364px] md:flex-none md:rounded-[18px] md:border md:border-line md:shadow-1"
    >
      <div className="flex flex-col gap-3 border-b border-line px-5 pt-[18px] pb-4">
        <div className="flex flex-wrap items-center gap-2">
          <span className={model.kind === 'review' ? 'text-human' : 'text-accent'}>
            <KindIcon kind={model.kind} size={16} />
          </span>
          <h2
            ref={heading}
            id="step-panel-title"
            tabIndex={-1}
            className="m-0 min-w-0 font-display text-[32px] leading-[34px] font-normal break-words focus-visible:outline-none"
          >
            {model.name}
          </h2>
          <StatusBadge status={model.state} />
          <button
            type="button"
            onClick={onClose}
            aria-label={t('graph.panel.close')}
            className="ml-auto inline-flex size-9 items-center justify-center rounded-1 text-ink-2"
          >
            <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
              <path d="M3 3L11 11M11 3L3 11" stroke="currentColor" strokeWidth="1.6" />
            </svg>
          </button>
        </div>
        <p className="m-0 text-tiny text-ink-2">
          {t('graph.panel.subtitle', {
            kind: kindLabel(model.kind),
            action: model.step.action,
            revision: `r${revision}`,
          })}
        </p>
        {model.step.pauseCause !== undefined && paused && (
          <p className="m-0 text-tiny text-ink-2">{t('graph.panel.pauseCause', { cause: model.step.pauseCause })}</p>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <button type="button" className={buttons.secondary} disabled={busy} onClick={() => onPause(!paused)}>
            <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
              <path
                d={paused ? 'M3.5 2L9.5 6L3.5 10Z' : SH.pause}
                fill={paused ? 'currentColor' : 'none'}
                stroke="currentColor"
                strokeWidth="1.8"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            {paused ? t('graph.panel.resume') : t('graph.panel.pause')}
          </button>
          <Link
            to={`${pipelinePath(pipeline, 'items')}?step=${encodeURIComponent(model.name)}`}
            className="px-3 py-[7px] text-small font-bold no-underline"
          >
            {t('graph.panel.items')}
          </Link>
        </div>
      </div>

      {model.insight !== undefined && (
        <div
          className={`mx-5 mt-4 flex flex-col gap-1.5 rounded-2 border px-3.5 py-3 ${model.insight.severity === 'error' ? 'border-bad bg-bad-soft' : 'border-warn bg-warn-soft'}`}
        >
          <div
            className={`flex items-center gap-1.5 text-small font-bold ${model.insight.severity === 'error' ? 'text-bad' : 'text-warn'}`}
          >
            <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
              <path d={SH.tri} fill="currentColor" />
            </svg>
            {t('graph.panel.insight', { kind: insightLabel(model.insight.kind) })}
          </div>
          <p className="m-0 text-small text-ink">{model.insight.summary}</p>
          <Link to={pipelinePath(pipeline, 'insights')} className="text-small font-bold text-ink">
            {t('graph.panel.seeInsights')}
          </Link>
        </div>
      )}

      <dl className="m-0 grid grid-cols-2 gap-2 px-5 py-4">
        {fig === null ? (
          <p className="col-span-2 m-0 text-small text-ink-2">{t('graph.figure.noSnapshot')}</p>
        ) : (
          <>
            {model.kind === 'review' && (
              <Stat label={t('graph.panel.toReview')} value={f.num(fig.awaitingReview)} tone="text-human" />
            )}
            <Stat
              label={t('graph.panel.inFlight')}
              value={f.num(fig.inFlight)}
              sub={t('graph.panel.slots', { n: f.num(model.step.concurrency) })}
            />
            <Stat
              label={t('graph.panel.waiting')}
              value={f.num(fig.queued)}
              tone={fig.queued > 0 ? 'text-accent' : 'text-ink'}
            />
            <Stat
              label={t('graph.panel.pace')}
              value={t('graph.panel.perMinute', { n: f.num(fig.throughputPerMin, { maximumFractionDigits: 1 }) })}
            />
            <Stat label={t('graph.panel.p95')} value={f.duration(fig.p95LatencyMs)} />
            <Stat
              label={t('graph.panel.errors')}
              value={f.percent(fig.errorRate)}
              tone={fig.errorRate > 0 ? 'text-bad' : 'text-ink'}
              sub={t(f.plural(model.openDeadLetters, 'graph.panel.deadOne', 'graph.panel.deadOther'), {
                n: f.num(model.openDeadLetters),
              })}
            />
          </>
        )}
      </dl>

      <section aria-labelledby="step-panel-failures" className="flex flex-col gap-2.5 px-5 pb-5">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 id="step-panel-failures" className="m-0 text-ui font-bold">
            {t('graph.panel.failures')}
          </h3>
          {letters.length > 0 && (
            <Link to={pipelinePath(pipeline, 'dead-letters')} className="text-tiny font-bold">
              {t('graph.panel.allDeadLetters')}
            </Link>
          )}
        </div>
        {letters.length === 0 && <p className="m-0 text-small text-ink-2">{t('graph.panel.noFailures')}</p>}
        <ul className="m-0 flex list-none flex-col gap-2.5 p-0">
          {letters.slice(0, RECENT).map((letter) => (
            <li key={letter.id} className="flex flex-col gap-1.5 rounded-2 border border-line px-3 py-2.5">
              <div className="flex flex-wrap items-center gap-2 text-tiny">
                <StatusText status={letter.group.kind === 'unrouted' ? 'unrouted' : 'dead'} />
                <Link to={itemPath(pipeline, letter.id)} className="font-mono text-ink" title={letter.id}>
                  {shortId(letter.id)}
                </Link>
                <time dateTime={letter.createdAt} title={f.dateTime(letter.createdAt)} className="ml-auto text-ink-3">
                  {f.relative(letter.createdAt)}
                </time>
              </div>
              <p
                className="m-0 line-clamp-2 font-mono text-[11.5px] leading-4 break-words text-ink-2"
                title={letter.error}
              >
                {letter.error}
              </p>
              <div className="flex gap-2">
                <button
                  type="button"
                  className={buttons.smallPrimary}
                  onClick={() => onDeadLetters('replay', targetOf([{ ...letter, step: model.name }]))}
                >
                  {t('graph.panel.replay')}
                </button>
                <button
                  type="button"
                  className={buttons.smallSecondary}
                  onClick={() => onDeadLetters('discard', targetOf([{ ...letter, step: model.name }]))}
                >
                  {t('graph.panel.discard')}
                </button>
              </div>
            </li>
          ))}
        </ul>
        {letters.length > RECENT && (
          <p className="m-0 text-tiny text-ink-3">
            {t('graph.panel.moreFailures', { n: f.num(letters.length - RECENT) })}
          </p>
        )}
      </section>
    </aside>
  );
}
