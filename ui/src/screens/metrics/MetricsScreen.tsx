import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router-dom';
import type { MetricSeries } from '../../api/schema';
import { formatDateTime, formatDuration, formatMoney, formatNumber } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { ScreenPage } from '../ScreenPage';
import { CopyButton } from '../../common/Copy';
import { ScrollRegion } from '../../common/ScrollRegion';
import { Segmented } from '../../common/Segmented';
import { EmptyState, QueryView } from '../../common/States';
import { card, cardTitle, field } from '../../common/styles';
import { useNow } from '../../common/useNow';
import { useResource } from '../../common/useResource';
import { PipelineSelect } from '../../common/PipelineSelect';
import { MetricChart, type ChartSeries, type Mark } from './MetricChart';
import {
  bucket,
  defaultStat,
  queryOf,
  ranges,
  readView,
  stats,
  summary,
  windowOf,
  writeView,
  type Range,
  type Stat,
  type View,
} from './view';

// Series styles: a colour token and a dash, so lines differ by more than hue.
const styles: { tone: string; dash: number[]; swatch: string }[] = [
  { tone: '--ms-accent', dash: [], swatch: 'border-accent border-solid' },
  { tone: '--ms-warn', dash: [6, 3], swatch: 'border-warn border-dashed' },
  { tone: '--ms-human', dash: [1.5, 3], swatch: 'border-human border-dotted' },
  { tone: '--ms-ink-3', dash: [], swatch: 'border-ink-3 border-solid' },
  { tone: '--ms-bad', dash: [6, 3], swatch: 'border-bad border-dashed' },
  { tone: '--ms-ok', dash: [1.5, 3], swatch: 'border-ok border-dotted' },
];
const style = (i: number) => styles[i % styles.length] as (typeof styles)[number];

// The labels the engine's metrics carry (internal/metrics), offered when grouping by another label.
const knownLabels = ['stream', 'reason', 'class', 'decision', 'state', 'agreement', 'le'];

type GroupMode = 'none' | 'step' | 'label';

// The metrics explorer: pick a metric, group it, choose a range and a statistic; the whole view lives in the URL so
// the link reopens it. uPlot draws the lines; the series table and the data table say the same in text.
export function MetricsScreen() {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const [search, setSearch] = useSearchParams();
  const asked = readView(search);
  const now = useNow(60_000);
  const names = useResource(`metric-names:${asked.pipeline}`, (api) =>
    api.listMetricNames(asked.pipeline ? { pipeline: asked.pipeline, since: '30d' } : { since: '30d' }),
  );
  const available = names.status === 'ready' ? names.data : [];
  // Without a metric in the URL, the latency of deliveries when there is one, else the first.
  const fallback = available.includes('delivery_latency_seconds') ? 'delivery_latency_seconds' : (available[0] ?? '');
  const view: View = asked.metric ? asked : { ...asked, metric: fallback, stat: defaultStat(fallback) };
  const set = (change: Partial<View>) => setSearch(writeView({ ...view, ...change }));
  // The label to group by is applied when typed out (Enter or leaving the field), not at every keystroke.
  const commitLabel = (text: string) => {
    const label = text.trim();
    if (label !== '' && label !== view.by) set({ by: label, hidden: [] });
  };

  const query = queryOf(view, now);
  // A fixed range asks for "since 24h", so the key does not move with the clock.
  const series = useResource(view.metric ? `metrics:${JSON.stringify(query)}` : null, (api) => api.getMetrics(query));
  const revisions = useResource(view.pipeline && view.marks ? `revisions:${view.pipeline}` : null, (api) =>
    api.listRevisions(view.pipeline),
  );
  const marks = useMemo<Mark[]>(
    () =>
      (revisions.status === 'ready' ? revisions.data : []).map((r) => ({
        t: Date.parse(r.appliedAt),
        label: t('metrics.applied', { revision: r.id }),
      })),
    [revisions, t],
  );
  // A fixed range slides with the clock: read it again each minute, in the background.
  const refresh = series.refresh;
  const lastTick = useRef(now);
  useEffect(() => {
    if (lastTick.current === now) return;
    lastTick.current = now;
    if (view.range !== 'custom') refresh();
  }, [now, view.range, refresh]);
  const labelInput = useId();

  const groupMode: GroupMode = view.by === '' ? 'none' : view.by === 'step' ? 'step' : 'label';
  const rangeLabels: Record<Range, string> = {
    '1h': t('metrics.range.1h'),
    '24h': t('metrics.range.24h'),
    '7d': t('metrics.range.7d'),
    '30d': t('metrics.range.30d'),
    custom: t('metrics.range.custom'),
  };
  const statLabels: Record<Stat, string> = {
    sum: t('metrics.stat.sum'),
    mean: t('metrics.stat.mean'),
    max: t('metrics.stat.max'),
    count: t('metrics.stat.count'),
  };

  // Durations in seconds, costs in dollars, everything else as a number; a count is always a number.
  const format = useCallback(
    (value: number) => {
      if (view.stat !== 'count' && view.metric.endsWith('_seconds')) return formatDuration(value * 1000, language);
      if (view.stat !== 'count' && view.metric.endsWith('_usd')) return formatMoney(value, language);
      return formatNumber(value, language, { maximumFractionDigits: 2 });
    },
    [view.stat, view.metric, language],
  );

  const path = `/metrics${search.toString() ? `?${search.toString()}` : ''}`;

  const header = (
    <div className="flex max-w-full min-w-0 items-center rounded-1 border border-line bg-bg md:ml-auto">
      <span className="min-w-0 truncate px-2.5 py-1 font-mono text-tiny text-ink-2">{path}</span>
      <CopyButton
        text={window.location.origin + path}
        label={t('metrics.copy')}
        className="border-l border-line px-2.5 py-1 text-tiny font-bold whitespace-nowrap text-accent"
      />
    </div>
  );

  return (
    <ScreenPage title={t('nav.metrics')} header={header}>
      <div className="flex flex-wrap items-end gap-x-5 gap-y-3.5">
        <label className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
          {t('metrics.metric')}
          <select
            value={view.metric}
            onChange={(e) => set({ metric: e.target.value, stat: defaultStat(e.target.value), hidden: [] })}
            className={`${field} min-h-[34px] min-w-[240px] font-mono font-normal`}
          >
            {available.length === 0 && <option value="">{t('metrics.noMetric')}</option>}
            {available.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
          {t('metrics.pipeline')}
          <PipelineSelect
            value={view.pipeline}
            onChange={(pipeline) => set({ pipeline })}
            className="min-h-[34px] font-normal"
          />
        </label>
        <div className="flex flex-col gap-1">
          <span className="text-tiny font-bold text-ink-2">{t('metrics.groupBy')}</span>
          <div className="flex flex-wrap items-center gap-2">
            <Segmented<GroupMode>
              label={t('metrics.groupBy')}
              value={groupMode}
              options={[
                { value: 'none', label: t('metrics.group.none') },
                { value: 'step', label: t('metrics.group.step') },
                { value: 'label', label: t('metrics.group.label') },
              ]}
              onChange={(mode) => set({ by: mode === 'none' ? '' : mode === 'step' ? 'step' : 'stream', hidden: [] })}
            />
            {groupMode === 'label' && (
              <>
                <label htmlFor={labelInput} className="sr-only">
                  {t('metrics.labelName')}
                </label>
                <input
                  id={labelInput}
                  list={`${labelInput}-list`}
                  defaultValue={view.by}
                  onBlur={(e) => commitLabel(e.currentTarget.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') commitLabel(e.currentTarget.value);
                  }}
                  className={`${field} min-h-[34px] w-32 font-mono`}
                />
                <datalist id={`${labelInput}-list`}>
                  {knownLabels.map((l) => (
                    <option key={l} value={l} />
                  ))}
                </datalist>
              </>
            )}
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-tiny font-bold text-ink-2">{t('metrics.rangeLabel')}</span>
          <Segmented<Range>
            label={t('metrics.rangeLabel')}
            value={view.range}
            options={ranges.map((value) => ({ value, label: rangeLabels[value] }))}
            onChange={(range) => {
              const w = windowOf(view, now);
              set(
                range === 'custom'
                  ? { range, from: new Date(w.from).toISOString(), to: new Date(w.to).toISOString() }
                  : { range },
              );
            }}
          />
        </div>
        {view.range === 'custom' && (
          <div className="flex flex-wrap gap-2">
            {(['from', 'to'] as const).map((end) => (
              <label key={end} className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
                {end === 'from' ? t('metrics.from') : t('metrics.to')}
                <input
                  type="datetime-local"
                  value={toLocalInput(view[end])}
                  onChange={(e) => {
                    const date = new Date(e.target.value);
                    if (!Number.isNaN(date.getTime())) set({ [end]: date.toISOString() });
                  }}
                  className={`${field} min-h-[34px] font-normal`}
                />
              </label>
            ))}
          </div>
        )}
        <div className="flex flex-col gap-1">
          <span className="text-tiny font-bold text-ink-2">{t('metrics.statLabel')}</span>
          <Segmented<Stat>
            label={t('metrics.statLabel')}
            value={view.stat}
            options={stats.map((value) => ({ value, label: statLabels[value] }))}
            onChange={(stat) => set({ stat })}
          />
        </div>
        <label className="inline-flex min-h-[34px] items-center gap-2 text-small text-ink-2">
          <input
            type="checkbox"
            checked={view.marks}
            disabled={!view.pipeline}
            onChange={(e) => set({ marks: e.target.checked })}
            className="size-[15px] accent-accent"
          />
          {view.pipeline ? t('metrics.marks') : t('metrics.marksNeedPipeline')}
        </label>
      </div>

      <QueryView query={names}>
        {(list) =>
          list.length === 0 ? (
            <EmptyState title={t('metrics.empty')}>{t('metrics.emptyBody')}</EmptyState>
          ) : (
            <QueryView query={series} rows={2}>
              {(data) => (
                <Explorer
                  view={view}
                  now={now}
                  data={data}
                  marks={marks}
                  format={format}
                  statLabel={statLabels[view.stat]}
                  rangeLabel={rangeLabels[view.range]}
                  onToggle={(group) =>
                    set({
                      hidden: view.hidden.includes(group)
                        ? view.hidden.filter((g) => g !== group)
                        : [...view.hidden, group],
                    })
                  }
                />
              )}
            </QueryView>
          )
        }
      </QueryView>
    </ScreenPage>
  );
}

// An ISO time as a datetime-local value in the browser's time zone.
function toLocalInput(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '';
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

// The chart and its hover readout: hovering re-renders only this, not the tables.
function ChartPanel({
  times,
  visible,
  marks,
  format,
  label,
}: {
  times: number[];
  visible: { group: string; values: (number | null)[]; style: (typeof styles)[number] }[];
  marks: Mark[];
  format: (value: number) => string;
  label: string;
}) {
  const { language } = usePreferences();
  const [hover, setHover] = useState<number | null>(null);
  const series: ChartSeries[] = useMemo(
    () => visible.map((s) => ({ group: s.group, tone: s.style.tone, dash: s.style.dash, values: s.values })),
    [visible],
  );
  const at = hover === null ? undefined : times[hover];
  return (
    <>
      <p aria-hidden="true" className="m-0 min-h-5 text-tiny text-ink-2">
        {hover !== null && at !== undefined && (
          <>
            <strong className="font-mono">{formatDateTime(new Date(at), language)}</strong>
            {visible.map((s) => {
              const v = s.values[hover];
              return (
                <span key={s.group}>
                  {' · '}
                  {s.group} <strong className="font-mono">{v === null || v === undefined ? '—' : format(v)}</strong>
                </span>
              );
            })}
          </>
        )}
      </p>
      <MetricChart times={times} series={series} marks={marks} format={format} label={label} onHover={setHover} />
    </>
  );
}

function Explorer({
  view,
  now,
  data,
  marks,
  format,
  statLabel,
  rangeLabel,
  onToggle,
}: {
  view: View;
  now: number;
  data: MetricSeries[];
  marks: Mark[];
  format: (value: number) => string;
  statLabel: string;
  rangeLabel: string;
  onToggle: (group: string) => void;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const { from, to } = windowOf(view, now);
  const rolled = useMemo(() => bucket(data, view.stat, from, to), [data, view.stat, from, to]);
  const all = useMemo(() => rolled.series.map((s, i) => ({ ...s, style: style(i) })), [rolled]);
  const visible = useMemo(() => all.filter((s) => !view.hidden.includes(s.group)), [all, view.hidden]);
  const when = (time: number) => formatDateTime(new Date(time), language);

  if (all.length === 0) {
    return <EmptyState title={t('metrics.noData')}>{t('metrics.noDataBody', { range: rangeLabel })}</EmptyState>;
  }

  const description = t('metrics.chartLabel', {
    metric: view.metric,
    stat: statLabel,
    range: rangeLabel,
    shown: visible.length,
    total: all.length,
  });

  return (
    <>
      <section aria-label={t('metrics.chart')} className={`${card} flex flex-col gap-2 px-[18px] pt-4 pb-3`}>
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <h2 className={`${cardTitle} font-mono text-[20px] break-all`}>{view.metric}</h2>
          <span className="text-tiny text-ink-3">
            {t('metrics.subtitle', { stat: statLabel, range: rangeLabel, shown: visible.length, total: all.length })}
          </span>
        </div>
        <ChartPanel times={rolled.times} visible={visible} marks={marks} format={format} label={description} />
      </section>

      <ScrollRegion label={t('metrics.series')} className={`${card} overflow-x-auto`}>
        <table className="w-full min-w-[560px] border-collapse text-small">
          <thead>
            <tr className="bg-surface-2 text-left text-tiny text-ink-3">
              <th scope="col" className="py-2.5 pr-2 pl-4 font-bold">
                {view.by === '' ? t('metrics.seriesName') : view.by}
              </th>
              <th scope="col" className="px-2 py-2.5 text-right font-bold">
                {t('metrics.last')}
              </th>
              <th scope="col" className="px-2 py-2.5 text-right font-bold">
                {t('metrics.median')}
              </th>
              <th scope="col" className="py-2.5 pr-4 pl-2 text-right font-bold">
                {t('metrics.max')}
              </th>
            </tr>
          </thead>
          <tbody>
            {all.map((s) => {
              const sum = summary(s.values);
              const shown = !view.hidden.includes(s.group);
              const cell = (v: number | null) => (v === null ? '—' : format(v));
              return (
                <tr key={s.group} className={`border-t border-line ${shown ? 'text-ink' : 'text-ink-2'}`}>
                  <td className="py-2.5 pr-2 pl-4">
                    <label className="inline-flex cursor-pointer items-center gap-2.5">
                      <input
                        type="checkbox"
                        checked={shown}
                        onChange={() => onToggle(s.group)}
                        className="size-[15px] accent-accent"
                      />
                      <span
                        aria-hidden="true"
                        className={`w-[18px] border-t-2 ${shown ? s.style.swatch : 'border-control-line'}`}
                      />
                      <span className="font-mono font-bold">{s.group}</span>
                    </label>
                  </td>
                  <td className="px-2 py-2.5 text-right font-mono">{cell(sum.last)}</td>
                  <td className="px-2 py-2.5 text-right font-mono">{cell(sum.median)}</td>
                  <td className="py-2.5 pr-4 pl-2 text-right font-mono">{cell(sum.max)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </ScrollRegion>

      <details className={`${card} px-4 py-3 text-small`}>
        <summary className="cursor-pointer font-bold text-ink-2">{t('metrics.asTable')}</summary>
        <ScrollRegion label={t('metrics.asTable')} className="mt-2 max-h-[420px] overflow-auto">
          <table className="w-full border-collapse text-tiny">
            <caption className="sr-only">{description}</caption>
            <thead>
              <tr className="text-left text-ink-3">
                <th scope="col" className="sticky top-0 bg-surface py-1 pr-3 font-bold">
                  {t('metrics.time')}
                </th>
                {visible.map((s) => (
                  <th
                    key={s.group}
                    scope="col"
                    className="sticky top-0 bg-surface px-2 py-1 text-right font-mono font-bold"
                  >
                    {s.group}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rolled.times.map((time, i) => (
                <tr key={time} className="border-t border-line">
                  <th scope="row" className="py-1 pr-3 text-left font-mono font-normal whitespace-nowrap">
                    {when(time)}
                  </th>
                  {visible.map((s) => {
                    const v = s.values[i];
                    return (
                      <td key={s.group} className="px-2 py-1 text-right font-mono">
                        {v === null || v === undefined ? '—' : format(v)}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </ScrollRegion>
      </details>
    </>
  );
}
