import { useId, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams } from 'react-router-dom';
import { liveUrls, withQuery, type Api } from '../../api/api';
import { useApi } from '../../api/context';
import type { Delivery, Pipeline, Run, RunReport } from '../../api/schema';
import {
  formatClock,
  formatDuration,
  formatMoney,
  formatNumber,
  formatPercent,
  pluralCategory,
} from '../../i18n/format';
import { useCoalescedRefresh } from '../../live/useCoalescedRefresh';
import { useLiveStream } from '../../live/useLiveStream';
import { usePreferences } from '../../preferences';
import { itemPath, pipelinePath, runPath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { ConfirmDialog } from '../../common/ConfirmDialog';
import { Id } from '../../common/Id';
import { errorMessage } from '../../common/Notice';
import { StatusBadge, StatusGlyph } from '../../common/Status';
import { QueryView } from '../../common/States';
import { buttons, card, cardTitle } from '../../common/styles';
import { useNow } from '../../common/useNow';
import { useResource, type Query, type Resource } from '../../common/useResource';
import { settledStates, useRunName } from './runParts';
import { buildTimeline, density, position, ticks, type SegmentKind, type StepRow, type Timeline } from './timeline';

// What a live event changes; the pipeline and the neighbouring runs are read once.
interface Live {
  run: Run;
  report: RunReport;
  deliveries: Delivery[];
}

interface Context {
  pipeline: Pipeline;
  neighbours: Run[];
}

type RunData = Live & Context;

async function readLive(api: Api, pipeline: string, id: string): Promise<Live> {
  const [run, report, deliveries] = await Promise.all([
    api.getRun(pipeline, id),
    api.getRunReport(pipeline, id),
    api.listRunDeliveries(pipeline, id),
  ]);
  return { run, report, deliveries };
}

async function readContext(api: Api, pipeline: string): Promise<Context> {
  const [p, page] = await Promise.all([api.getPipeline(pipeline), api.listRuns(pipeline, { limit: 50 })]);
  return { pipeline: p, neighbours: page.items };
}

// Both reads as one: loading until both are in, the first error otherwise; reload reads again what failed.
function combine(live: Resource<Live>, context: Resource<Context>): Query<RunData> {
  const reload = () => {
    if (live.status === 'error') live.reload();
    if (context.status === 'error') context.reload();
  };
  if (live.status === 'error') return { status: 'error', error: live.error, reload };
  if (context.status === 'error') return { status: 'error', error: context.error, reload };
  if (live.status === 'ready' && context.status === 'ready') {
    return { status: 'ready', data: { ...live.data, ...context.data }, reload };
  }
  return { status: 'loading', reload };
}

// One run: its Gantt per step, the density strip of a step's deliveries, and the run report (what the run waits
// on, ETA, critical path, slowest steps, failures by cluster, cost and figures against recent runs). Live while
// the run is open.
export function RunScreen() {
  const { t } = useTranslation();
  const { pipeline = '', run: id = '' } = useParams();
  const live = useResource(`run:${pipeline}:${id}`, (api) => readLive(api, pipeline, id));
  const context = useResource(`run-context:${pipeline}:${id}`, (api) => readContext(api, pipeline));
  const data = combine(live, context);
  const name = useRunName();
  const refresh = live.refresh;
  const settled = data.status === 'ready' && settledStates.has(data.data.run.state);

  const schedule = useCoalescedRefresh(refresh);
  useLiveStream(data.status !== 'ready' || settled ? null : liveUrls.run(pipeline, id), {
    events: ['state'],
    onMessage: schedule,
    onReconnect: refresh,
    onSnapshotRequired: refresh,
  });

  const title = data.status === 'ready' ? name(data.data.run) : t('runs.run');
  return (
    <ScreenPage
      title={title}
      eyebrow={
        <p className="m-0 flex items-center gap-2 text-small">
          <Link to={pipelinePath(pipeline, 'runs')} className="text-ink-2">
            {t('nav.runs')}
          </Link>
          <span aria-hidden="true" className="text-ink-3">
            /
          </span>
          <span className="font-mono text-ink-3">{pipeline}</span>
        </p>
      }
      header={data.status === 'ready' && <RunHeader data={data.data} onChanged={refresh} />}
    >
      <QueryView query={data} rows={4}>
        {(d) => <RunBody data={d} />}
      </QueryView>
    </ScreenPage>
  );
}

function RunHeader({ data, onChanged }: { data: RunData; onChanged: () => void }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const name = useRunName();
  const { run, deliveries, neighbours } = data;
  const items = Object.values(run.items ?? {}).reduce((a, b) => a + b, 0);
  const at = neighbours.findIndex((r) => r.id === run.id);
  const older = at >= 0 ? neighbours[at + 1] : undefined;
  const newer = at > 0 ? neighbours[at - 1] : undefined;
  const [confirm, setConfirm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const api = useApi();
  const cancel = () =>
    api
      .cancelRun(run.pipeline, run.id)
      .then(onChanged, (e: unknown) => setError(errorMessage(e)))
      .finally(() => setConfirm(false));
  return (
    <>
      <StatusBadge status={run.state} />
      <p className="m-0 text-small text-ink-2">
        {run.kind === 'replay' ? t('runs.replayRun') : t('runs.trigger', { trigger: run.trigger ?? run.triggerSource })}
        {' · '}
        {t('runs.revision', { revision: run.revision })}
        {' · '}
        {t('runs.items', { count: formatNumber(items, language) })}
        {' · '}
        {t('runs.deliveries', { count: formatNumber(deliveries.length, language) })}
        {' · '}
        {formatMoney(run.costUsd, language)}
      </p>
      <div className="flex flex-1 flex-wrap justify-end gap-1.5">
        <nav aria-label={t('runs.neighbours')} className="flex flex-wrap gap-1.5">
          {older && (
            <Link to={runPath(run.pipeline, older.id)} className={`${buttons.secondary} no-underline`}>
              ← {name(older)}
            </Link>
          )}
          {newer && (
            <Link to={runPath(run.pipeline, newer.id)} className={`${buttons.secondary} no-underline`}>
              {name(newer)} →
            </Link>
          )}
        </nav>
        {!settledStates.has(run.state) && (
          <button type="button" onClick={() => setConfirm(true)} className={buttons.secondary}>
            {t('runs.cancel')}
          </button>
        )}
      </div>
      {error !== null && (
        <p role="alert" className="m-0 w-full text-small text-bad">
          {t('runs.cancelFailed')} {error}
        </p>
      )}
      {confirm && (
        <ConfirmDialog
          title={t('runs.cancelTitle', { name: name(run) })}
          confirmLabel={t('runs.cancelConfirm')}
          cancelLabel={t('runs.keepRunning')}
          destructive
          onConfirm={cancel}
          onCancel={() => setConfirm(false)}
        >
          <p className="m-0">{t('runs.cancelBody')}</p>
        </ConfirmDialog>
      )}
    </>
  );
}

function RunBody({ data }: { data: RunData }) {
  const { run, report, deliveries, pipeline } = data;
  const settledAt = run.settledAt ? Date.parse(run.settledAt) : null;
  // A settled run's timeline does not move.
  const now = useNow(10_000, settledAt === null);
  const eta = report.eta ? Date.parse(report.eta) : null;
  const reviewSteps = useMemo(
    () => new Set(pipeline.steps.filter((s) => s.builtin && s.action === 'review').map((s) => s.name)),
    [pipeline],
  );
  const timeline = useMemo(
    () =>
      buildTimeline(
        pipeline.steps.map((s) => s.name),
        reviewSteps,
        deliveries,
        Date.parse(run.startedAt),
        settledAt,
        Math.max(now, Date.parse(run.startedAt)),
        eta,
      ),
    [pipeline, reviewSteps, deliveries, run.startedAt, settledAt, now, eta],
  );
  return (
    <div className="flex flex-wrap items-start gap-6">
      <Gantt timeline={timeline} running={settledAt === null} eta={eta} pipeline={run.pipeline} />
      <Report data={data} timeline={timeline} reviewSteps={reviewSteps} now={now} eta={eta} />
    </div>
  );
}

// The look of each kind of span, shared by the bars and the legend.
const segmentTone: Record<SegmentKind, string> = {
  work: 'bg-accent',
  wait: 'bg-surface-2 bg-[repeating-linear-gradient(135deg,var(--ms-hatch)_0_2px,transparent_2px_5px)]',
  people: 'bg-human-soft bg-[repeating-linear-gradient(135deg,var(--ms-human)_0_1.5px,transparent_1.5px_5px)]',
  projected: 'border border-dashed border-ink-3',
};

function Legend() {
  const { t } = useTranslation();
  const items: [SegmentKind, string][] = [
    ['work', t('runs.legend.work')],
    ['wait', t('runs.legend.wait')],
    ['people', t('runs.legend.people')],
    ['projected', t('runs.legend.projected')],
  ];
  return (
    <ul className="m-0 flex list-none flex-wrap items-center gap-x-[18px] gap-y-2 p-0 text-tiny text-ink-2">
      {items.map(([kind, label]) => (
        <li key={kind} className="inline-flex items-center gap-1.5">
          <span aria-hidden="true" className={`inline-block h-2.5 w-[18px] ${segmentTone[kind]}`} />
          {label}
        </li>
      ))}
      <li className="inline-flex items-center gap-1.5">
        <StatusGlyph status="open" size={10} />
        {t('runs.legend.dead')}
      </li>
    </ul>
  );
}

function Gantt({
  timeline,
  running,
  eta,
  pipeline,
}: {
  timeline: Timeline;
  running: boolean;
  eta: number | null;
  pipeline: string;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const { from, to, end, rows } = timeline;
  const [open, setOpen] = useState<string | null>(null);
  const pos = (time: number) => position(time, from, to);
  return (
    <section
      aria-label={t('runs.timeline')}
      className={`${card} flex min-w-0 flex-[999_1_640px] flex-col gap-3 px-4 pt-4 pb-5 md:px-5`}
    >
      <div className="flex flex-wrap items-center gap-x-[18px] gap-y-2">
        <h2 className={cardTitle}>{t('runs.timeline')}</h2>
        <Legend />
      </div>
      <div className="grid grid-cols-[minmax(0,110px)_minmax(0,1fr)] gap-x-3.5 md:grid-cols-[150px_minmax(0,1fr)]">
        <span />
        <div aria-hidden="true" className="relative h-[22px] border-b border-line font-mono text-[11px] text-ink-3">
          {ticks(from, to).map((tick) => (
            <span key={tick} className="absolute -translate-x-1/2" style={{ left: `${pos(tick)}%` }}>
              {formatClock(new Date(tick), language)}
            </span>
          ))}
        </div>
        {rows.map((row) => (
          <GanttRow
            key={row.step}
            row={row}
            from={from}
            to={to}
            end={end}
            pipeline={pipeline}
            open={open === row.step}
            onToggle={() => setOpen(open === row.step ? null : row.step)}
          />
        ))}
        <span />
        <div aria-hidden="true" className="relative h-[22px] font-mono text-[11px] font-bold">
          {running && (
            <span
              className="absolute top-0.5 -translate-x-1/2 rounded-1 bg-ink px-1.5 text-bg"
              style={{ left: `${pos(end)}%` }}
            >
              {t('runs.now', { time: formatClock(new Date(end), language) })}
            </span>
          )}
          {running && eta !== null && eta > end && (
            <span className="absolute top-0.5 -translate-x-[90%] text-ink-2" style={{ left: `${pos(eta)}%` }}>
              {t('runs.eta', { time: formatClock(new Date(eta), language) })}
            </span>
          )}
        </div>
      </div>
    </section>
  );
}

function GanttRow({
  row,
  from,
  to,
  end,
  pipeline,
  open,
  onToggle,
}: {
  row: StepRow;
  from: number;
  to: number;
  end: number;
  pipeline: string;
  open: boolean;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const stripId = useId();
  const pos = (time: number) => position(time, from, to);
  const clock = (time: number) => formatClock(new Date(time), language);
  const summary =
    row.start === null
      ? t('runs.row.notStarted', { step: row.step })
      : t('runs.row.summary', {
          step: row.step,
          start: clock(row.start),
          end: row.drained === null ? t('runs.row.open') : t('runs.row.drained', { time: clock(row.drained) }),
          work: formatPercent(row.shares.work, language),
          people: formatPercent(row.shares.people, language),
          wait: formatPercent(row.shares.wait, language),
          dead: row.dead.length,
        });
  return (
    <>
      <div className="flex h-[34px] items-center border-b border-line">
        <button
          type="button"
          aria-expanded={open}
          aria-controls={stripId}
          disabled={row.deliveries.length === 0}
          onClick={onToggle}
          className="min-w-0 truncate text-left font-mono text-tiny font-bold text-ink disabled:font-normal disabled:text-ink-3"
        >
          {row.step}
        </button>
      </div>
      <div role="img" aria-label={summary} className="relative h-[34px] border-b border-line">
        {row.segments.map((s) => (
          <span
            key={`${s.kind}-${s.from}`}
            className={`absolute top-[9px] box-border h-4 rounded-pill ${segmentTone[s.kind]}`}
            style={{ left: `${pos(s.from)}%`, width: `${Math.max(0.4, pos(s.to) - pos(s.from))}%` }}
          />
        ))}
        {row.dead.map((time, i) => (
          <span
            key={`${time}-${i}`}
            className="absolute top-1 -ml-[3px] size-[7px] bg-bad shadow-[0_0_0_1.5px_var(--ms-surface)]"
            style={{ left: `${pos(time)}%` }}
          />
        ))}
        {row.drained !== null && row.start !== null && (
          <span className="absolute top-[9px] right-0 bg-surface pl-1.5 font-mono text-[11px] text-ink-3">
            {formatDuration(row.drained - row.start, language)}
          </span>
        )}
      </div>
      {open && <Density id={stripId} row={row} from={from} end={end} pipeline={pipeline} />}
    </>
  );
}

// The density strip: a step's deliveries binned over the run; a bin lists the items behind it.
function Density({
  id,
  row,
  from,
  end,
  pipeline,
}: {
  id: string;
  row: StepRow;
  from: number;
  end: number;
  pipeline: string;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const { bins, width } = density(row.deliveries, from, end);
  const most = Math.max(1, ...bins.map((b) => b.length));
  const [picked, setPicked] = useState<number | null>(null);
  const clock = (time: number) => formatClock(new Date(time), language);
  return (
    <div id={id} className="col-span-2 flex flex-col gap-2 border-b border-line bg-surface-2 px-2 py-2.5">
      <p className="m-0 text-tiny text-ink-2">{t('runs.density', { step: row.step, count: row.deliveries.length })}</p>
      <div className="flex h-12 items-end gap-px">
        {bins.map((bin, i) => {
          const label = t(pluralCategory(bin.length, language) === 'one' ? 'runs.binOne' : 'runs.binOther', {
            from: clock(from + i * width),
            to: clock(from + (i + 1) * width),
            count: bin.length,
          });
          const bar = (
            <span
              className={`block w-full ${bin.length === 0 ? '' : picked === i ? 'bg-ink' : 'bg-accent'}`}
              style={{ height: `${(bin.length / most) * 100}%` }}
            />
          );
          return bin.length === 0 ? (
            <span key={i} className="flex h-full flex-1 items-end" aria-hidden="true">
              {bar}
            </span>
          ) : (
            <button
              key={i}
              type="button"
              aria-label={label}
              aria-pressed={picked === i}
              onClick={() => setPicked(picked === i ? null : i)}
              className="flex h-full min-w-0 flex-1 items-end"
            >
              {bar}
            </button>
          );
        })}
      </div>
      {picked !== null && (
        <ul className="m-0 flex list-none flex-wrap gap-x-3 gap-y-1 p-0 text-tiny" aria-label={t('runs.binItems')}>
          {(bins[picked] ?? []).map((d) => (
            <li key={d.id}>
              <Link to={itemPath(pipeline, d.itemId)}>
                <Id value={d.itemId} />
              </Link>{' '}
              <span className="text-ink-3">{t(deliveryState[d.state])}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

const deliveryState = {
  pending: 'runs.delivery.pending',
  leased: 'runs.delivery.leased',
  done: 'runs.delivery.done',
  filtered: 'runs.delivery.filtered',
  awaitingReview: 'runs.delivery.awaitingReview',
  dead: 'runs.delivery.dead',
  cancelled: 'runs.delivery.cancelled',
} as const;

function Report({
  data,
  timeline,
  reviewSteps,
  now,
  eta,
}: {
  data: RunData;
  timeline: Timeline;
  reviewSteps: Set<string>;
  now: number;
  eta: number | null;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const { run, report } = data;
  const open = !settledStates.has(run.state);
  const parked = data.deliveries.filter((d) => d.state === 'awaitingReview');
  const oldestParked = Math.min(...parked.map((d) => Date.parse(d.createdAt)));
  const queued = timeline.rows
    .map((r) => ({ step: r.step, count: r.deliveries.filter((d) => d.state === 'pending').length }))
    .filter((q) => q.count > 0);
  const slowest = [...(report.steps ?? [])].sort((a, b) => b.p95Seconds - a.p95Seconds).slice(0, 3);
  const box = `${card} flex flex-col gap-2.5 p-4`;
  return (
    <aside aria-label={t('runs.report')} className="flex min-w-0 flex-[1_1_340px] flex-col gap-4">
      {open && (
        <section className={box}>
          <h2 className={cardTitle}>{t('runs.waitingOn')}</h2>
          {parked.length > 0 && (
            <Link
              to={withQuery('/inbox', { pipeline: run.pipeline })}
              className="flex items-start gap-2.5 rounded-1 bg-human-soft px-3 py-2.5 text-small text-ink no-underline"
            >
              <span>
                <strong>{t('runs.parked', { count: parked.length })}</strong>{' '}
                {t('runs.parkedOldest', { age: formatDuration(now - oldestParked, language) })}
                <br />
                <span className="font-bold text-human">{t('runs.openInbox')}</span>
              </span>
            </Link>
          )}
          {queued.map((q) => (
            <p key={q.step} className="m-0 rounded-1 bg-surface-2 px-3 py-2.5 text-small">
              <strong>{t('runs.queued', { step: q.step, count: q.count })}</strong>
            </p>
          ))}
          {parked.length === 0 && queued.length === 0 && (
            <p className="m-0 text-small text-ink-2">{t('runs.nothingWaits')}</p>
          )}
          <p className="m-0 text-tiny text-ink-3">
            {eta === null ? t('runs.noEta') : t('runs.etaLine', { time: formatClock(new Date(eta), language) })}
          </p>
        </section>
      )}

      <section className={box}>
        <h2 className={cardTitle}>{t('runs.criticalPath')}</h2>
        {report.criticalPath.steps === null || report.criticalPath.steps.length === 0 ? (
          <p className="m-0 text-small text-ink-2">{t('runs.noPath')}</p>
        ) : (
          <ol className="m-0 flex list-none flex-col p-0">
            {report.criticalPath.steps.map((s, i) => (
              <li key={`${s.step}-${i}`} className="flex items-center gap-2.5 py-[5px] text-small">
                <span
                  aria-hidden="true"
                  className={`size-2 flex-none ${reviewSteps.has(s.step) ? 'bg-human' : s.waitSeconds > s.workSeconds ? 'bg-warn' : 'bg-accent'}`}
                />
                <span className="min-w-0 flex-1 truncate font-mono text-tiny font-bold">{s.step}</span>
                <span className="font-mono text-tiny text-ink-2">
                  {i === 0 ? '' : '+ '}
                  {formatDuration(s.latencySeconds * 1000, language)}
                  {reviewSteps.has(s.step) && `, ${t('runs.people')}`}
                </span>
              </li>
            ))}
          </ol>
        )}
      </section>

      {slowest.length > 0 && (
        <section className={box}>
          <h2 className={cardTitle}>{t('runs.slowest')}</h2>
          <ul className="m-0 flex list-none flex-col gap-1.5 p-0 text-small">
            {slowest.map((s) => (
              <li key={s.step} className="flex flex-wrap items-baseline gap-x-2.5">
                <span className="font-mono text-tiny font-bold">{s.step}</span>
                <span className="text-ink-2">
                  {t(pluralCategory(s.deliveries, language) === 'one' ? 'runs.p95One' : 'runs.p95Other', {
                    p95: formatDuration(s.p95Seconds * 1000, language),
                    count: formatNumber(s.deliveries, language),
                    cost: formatMoney(s.costUsd, language),
                  })}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section className={box}>
        <h2 className={cardTitle}>{t('runs.failures')}</h2>
        {report.failures === null || report.failures.length === 0 ? (
          <p className="m-0 text-small text-ink-2">{t('runs.noFailures')}</p>
        ) : (
          report.failures.map((f) => (
            <Link
              key={`${f.step}-${f.class}-${f.message}`}
              to={pipelinePath(run.pipeline, 'dead-letters')}
              className="flex gap-2.5 text-small text-ink no-underline"
            >
              <strong className="min-w-[22px] text-bad">×{f.count}</strong>
              <span className="min-w-0 break-words">
                <span className="font-mono text-tiny">{f.step}</span> · {f.class}, {f.message}
                {f.recovered > 0 && (
                  <span className="text-ink-3"> · {t('runs.recovered', { count: f.recovered })}</span>
                )}
              </span>
            </Link>
          ))
        )}
      </section>

      <Comparison report={report} />
    </aside>
  );
}

function Comparison({ report }: { report: RunReport }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const c = report.comparison;
  if (c === null) {
    return (
      <section className={`${card} flex flex-col gap-2.5 p-4`}>
        <h2 className={cardTitle}>{t('runs.cost')}</h2>
        <p className="m-0 text-small text-ink-2">
          {t('runs.costAlone', { cost: formatMoney(report.costUsd, language) })} {t('runs.noComparison')}
        </p>
      </section>
    );
  }
  const most = Math.max(c.costUsd.value, c.costUsd.median, 0.0001);
  const delta = (value: number, median: number) =>
    median === 0
      ? ''
      : formatNumber((value - median) / median, language, { style: 'percent', signDisplay: 'exceptZero' });
  const bar = (value: number, tone: string) => (
    <span className="relative h-2.5 bg-surface-2" aria-hidden="true">
      <span className={`absolute inset-y-0 left-0 ${tone}`} style={{ width: `${(value / most) * 100}%` }} />
    </span>
  );
  const rows: [string, string, string][] = [
    [
      t('runs.figure.duration'),
      formatDuration(c.durationSeconds.value * 1000, language),
      formatDuration(c.durationSeconds.median * 1000, language),
    ],
    [t('runs.figure.items'), formatNumber(c.items.value, language), formatNumber(c.items.median, language)],
    [
      t('runs.figure.failed'),
      formatNumber(c.failedAttempts.value, language),
      formatNumber(c.failedAttempts.median, language),
    ],
  ];
  return (
    <section className={`${card} flex flex-col gap-3 p-4`}>
      <h2 className={cardTitle}>{t('runs.costVs', { runs: c.runs })}</h2>
      <div className="grid grid-cols-[minmax(0,auto)_minmax(0,1fr)_minmax(0,auto)] items-center gap-x-2.5 gap-y-2 text-tiny">
        <span className="text-ink-2">{t('runs.thisRun')}</span>
        {bar(c.costUsd.value, 'bg-accent')}
        <strong className="text-right font-mono">{formatMoney(c.costUsd.value, language)}</strong>
        <span className="text-ink-2">{t('runs.median', { runs: c.runs })}</span>
        {bar(c.costUsd.median, 'bg-control-line')}
        <strong className="text-right font-mono">{formatMoney(c.costUsd.median, language)}</strong>
      </div>
      <p className="m-0 text-tiny text-ink-3">
        {t('runs.vsMedian', { delta: delta(c.costUsd.value, c.costUsd.median) })}
      </p>
      <table className="w-full border-collapse text-tiny">
        <caption className="sr-only">{t('runs.figures')}</caption>
        <thead>
          <tr className="text-left text-ink-3">
            <th scope="col" className="py-1 font-bold">
              <span className="sr-only">{t('runs.figure.name')}</span>
            </th>
            <th scope="col" className="py-1 text-right font-bold">
              {t('runs.thisRun')}
            </th>
            <th scope="col" className="py-1 text-right font-bold">
              {t('runs.median', { runs: c.runs })}
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([label, value, median]) => (
            <tr key={label} className="border-t border-line">
              <th scope="row" className="py-1 text-left font-normal text-ink-2">
                {label}
              </th>
              <td className="py-1 text-right font-mono">{value}</td>
              <td className="py-1 text-right font-mono text-ink-2">{median}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
