import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { withQuery } from '../../api/api';
import { useApi } from '../../api/context';
import type { Insight, InsightState } from '../../api/schema';
import {
  formatDateTime,
  formatDuration,
  formatMoney,
  formatNumber,
  formatPercent,
  formatRelativeTime,
} from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { itemPath, pipelinePath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { Id } from '../../common/Id';
import { errorMessage } from '../../common/Notice';
import { Segmented } from '../../common/Segmented';
import { EmptyState, ErrorState, QueryView } from '../../common/States';
import { useInsightKindLabel } from '../../common/insightKind';
import { StatusBadge, useStatusLabel, type StatusKey } from '../../common/Status';
import { buttons, card, field } from '../../common/styles';
import { useNow } from '../../common/useNow';
import { useResource } from '../../common/useResource';

// The canvas groups open and acknowledged insights as needing attention.
type Tab = 'attention' | 'resolved' | 'muted';
const tabOf = (state: InsightState): Tab =>
  state === 'resolved' ? 'resolved' : state === 'muted' ? 'muted' : 'attention';

// An open insight wants attention: the warning triangle, not the open dead letter's square.
const stateGlyph = (state: InsightState): StatusKey => (state === 'open' ? 'slow' : state);

type Evidence = Record<string, unknown>;
const num = (ev: Evidence, key: string): number | null => (typeof ev[key] === 'number' ? (ev[key] as number) : null);
const str = (ev: Evidence, key: string): string => (typeof ev[key] === 'string' ? (ev[key] as string) : '');

interface Figure {
  label: string;
  value: string;
  sub?: string;
}

// The insights of a pipeline (spec 11.1), by kind and step, each with its evidence: figures, sample items, a way
// in (metrics, dead letters) and Acknowledge or Mute. Proposals arrive with the Improve view (M5).
export function InsightsScreen() {
  const { t } = useTranslation();
  const { pipeline = '' } = useParams();
  const [search, setSearch] = useSearchParams();
  const tab = (['attention', 'resolved', 'muted'] as const).find((x) => x === search.get('state')) ?? 'attention';
  const insights = useResource(`insights:${pipeline}`, (api) => api.listInsights({ pipeline }));
  const kindLabel = useInsightKindLabel();
  const [hidden, setHidden] = useState<string[]>([]);
  const [step, setStep] = useState('');

  const all = insights.status === 'ready' ? insights.data : [];
  const counts = { attention: 0, resolved: 0, muted: 0 };
  for (const i of all) counts[tabOf(i.state)] += 1;
  const inTab = all.filter((i) => tabOf(i.state) === tab);
  const kinds = [...new Set(inTab.map((i) => i.kind))].sort();
  const steps = [...new Set(inTab.map((i) => i.scope))].sort();
  // A step chosen on another tab does not filter this one.
  const activeStep = steps.includes(step) ? step : '';
  const shown = inTab.filter((i) => !hidden.includes(i.kind) && (activeStep === '' || i.scope === activeStep));
  const now = useNow();

  return (
    <ScreenPage
      title={t('nav.insights')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{pipeline}</p>}
      header={
        <>
          <Segmented<Tab>
            label={t('insights.stateLabel')}
            value={tab}
            options={[
              { value: 'attention', label: t('insights.tab.attention', { count: counts.attention }) },
              { value: 'resolved', label: t('insights.tab.resolved', { count: counts.resolved }) },
              { value: 'muted', label: t('insights.tab.muted', { count: counts.muted }) },
            ]}
            onChange={(value) => setSearch(value === 'attention' ? {} : { state: value })}
          />
          <p className="m-0 text-tiny text-ink-3 md:ml-auto">{t('insights.explain')}</p>
        </>
      }
    >
      <QueryView query={insights}>
        {() => (
          <div className="flex flex-wrap items-start gap-6">
            <aside
              aria-label={t('insights.filters')}
              className="flex max-w-full flex-[1_1_200px] flex-col gap-1 md:max-w-[240px]"
            >
              <h2 className="m-0 px-2 pb-1.5 font-display text-[20px] leading-6 font-normal text-ink-3 italic">
                {t('insights.byKind')}
              </h2>
              {kinds.length === 0 && <p className="m-0 px-2 text-small text-ink-3">{t('insights.noKinds')}</p>}
              {kinds.map((kind) => (
                <label key={kind} className="flex items-center gap-2.5 rounded-1 px-2 py-[7px] text-small">
                  <input
                    type="checkbox"
                    checked={!hidden.includes(kind)}
                    onChange={() =>
                      setHidden(hidden.includes(kind) ? hidden.filter((k) => k !== kind) : [...hidden, kind])
                    }
                    className="size-[15px] accent-accent"
                  />
                  <span className="flex-1">{kindLabel(kind)}</span>
                  <span className="font-mono text-tiny text-ink-3">{inTab.filter((i) => i.kind === kind).length}</span>
                </label>
              ))}
              {steps.length > 1 && (
                <label className="mt-3 flex flex-col gap-1 px-2 text-tiny font-bold text-ink-2">
                  {t('insights.step')}
                  <select
                    value={activeStep}
                    onChange={(e) => setStep(e.target.value)}
                    className={`${field} font-mono font-normal`}
                  >
                    <option value="">{t('insights.allSteps')}</option>
                    {steps.map((s) => (
                      <option key={s} value={s}>
                        {s}
                      </option>
                    ))}
                  </select>
                </label>
              )}
            </aside>
            <div className="flex min-w-0 flex-[999_1_640px] flex-col gap-4">
              {shown.length === 0 ? (
                <EmptyState
                  title={tab === 'attention' ? t('insights.none') : t('insights.noneHere')}
                  done={tab === 'attention'}
                >
                  {tab === 'attention' && t('insights.noneBody')}
                </EmptyState>
              ) : (
                shown.map((insight) => (
                  <InsightCard key={insight.id} insight={insight} now={now} onChanged={insights.refresh} />
                ))
              )}
            </div>
          </div>
        )}
      </QueryView>
    </ScreenPage>
  );
}

// The title, body, figures and way in of an insight, from its kind's evidence (internal/insights).
function useReading(insight: Insight): {
  title: string;
  body: ReactNode;
  figures: Figure[];
  open?: { to: string; label: string };
} {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const ev = insight.evidence as Evidence;
  const step = insight.scope;
  // An evidence figure, or a dash when the engine had too little data for it.
  const fig = (key: string, format: (v: number) => string) => {
    const v = num(ev, key);
    return v === null ? '—' : format(v);
  };
  const pct = (key: string) => fig(key, (v) => formatPercent(v, language));
  const secs = (key: string) => fig(key, (v) => formatDuration(v * 1000, language));
  const money = (key: string) => fig(key, (v) => formatMoney(v, language));
  const count = (key: string) => formatNumber(num(ev, key) ?? 0, language);
  const metrics = (metric: string) => ({
    to: withQuery('/metrics', { m: metric, p: insight.pipeline, range: '7d' }),
    label: t('insights.openMetrics'),
  });
  switch (insight.kind) {
    case 'slowStep':
      return {
        title: t('insights.title.slowStep', { step }),
        body: t('insights.body.slowStep', { share: pct('criticalPathShare'), runs: count('runs') }),
        figures: [
          {
            label: t('insights.figure.p95'),
            value: secs('p95Seconds'),
            sub: t('insights.figure.baseline', { value: secs('baselineP95Seconds') }),
          },
          { label: t('insights.figure.pathShare'), value: pct('criticalPathShare') },
          {
            label: t('insights.figure.queueShare'),
            value: pct('queueWaitShare'),
            sub: t('insights.figure.workShare', { value: pct('workShare') }),
          },
        ],
        open: metrics('delivery_latency_seconds'),
      };
    case 'failureCluster':
      return {
        title: t('insights.title.failureCluster', { count: count('count'), step }),
        body: (
          <>
            <span className="font-mono">{str(ev, 'class')}</span>: {str(ev, 'message')}
          </>
        ),
        figures: [
          {
            label: t('insights.figure.failures'),
            value: count('count'),
            sub: t('insights.figure.dead', { count: count('dead') }),
          },
          { label: t('insights.figure.rate'), value: pct('rate') },
          { label: t('insights.figure.recovered'), value: pct('retrySuccessShare') },
        ],
        open: { to: pipelinePath(insight.pipeline, 'dead-letters'), label: t('insights.openDeadLetters') },
      };
    case 'cost':
      return {
        title: t('insights.title.cost', { step, cost: money('costUsd') }),
        body: t('insights.body.cost', { outputs: count('outputs') }),
        figures: [
          {
            label: t('insights.figure.perOutput'),
            value: money('costPerOutput'),
            sub: t('insights.figure.baseline', { value: money('baselineCostPerOutput') }),
          },
          { label: t('insights.figure.perApproved'), value: money('costPerApprovedItem') },
          { label: t('insights.figure.schemaFailures'), value: count('schemaFailures') },
        ],
        open: metrics('delivery_cost_usd'),
      };
    case 'reviewPattern': {
      const spots = Array.isArray(ev.editHotSpots) ? (ev.editHotSpots as { pointer?: string }[]) : [];
      const pointers = spots.map((s) => s.pointer ?? '').filter((p) => p !== '');
      return {
        title: t('insights.title.reviewPattern', { step }),
        body:
          pointers.length > 0
            ? t('insights.body.edits', { pointers: pointers.join(', ') })
            : t('insights.body.noEdits'),
        figures: [
          { label: t('insights.figure.intervention'), value: pct('interventionRate') },
          {
            label: t('insights.figure.editRate'),
            value: pct('editRate'),
            sub: t('insights.figure.rejected', { value: pct('rejectionRate') }),
          },
          {
            label: t('insights.figure.agreement'),
            value: pct('sampledAgreement'),
            sub: t('insights.figure.decision', { value: secs('timeToDecisionP50Seconds') }),
          },
        ],
        open: metrics('review_decisions'),
      };
    }
    case 'unschedulable':
      return {
        title: t('insights.title.unschedulable', { step }),
        body: (
          <>
            <span className="font-mono">{str(ev, 'class')}</span>: {str(ev, 'example')}
          </>
        ),
        figures: [
          { label: t('insights.figure.attempts'), value: count('count') },
          { label: t('insights.figure.deliveries'), value: count('deliveries') },
        ],
        open: { to: '/nodes', label: t('insights.openNodes') },
      };
    default:
      // A kind this UI does not know yet: the engine's own summary, which is in English.
      return { title: insight.kind, body: <span lang="en">{insight.summary}</span>, figures: [] };
  }
}

function InsightCard({ insight, now, onChanged }: { insight: Insight; now: number; onChanged: () => void }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const api = useApi();
  const { title, body, figures, open } = useReading(insight);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const samples = Array.isArray(insight.evidence.samples) ? (insight.evidence.samples as string[]) : [];
  const statusLabel = useStatusLabel();
  const kindLabel = useInsightKindLabel();
  const act = (action: (p: string, i: string) => Promise<void>) => {
    setBusy(true);
    setError(null);
    action(insight.pipeline, insight.id).then(
      () => {
        setBusy(false);
        onChanged();
      },
      (e: unknown) => {
        setBusy(false);
        setError(errorMessage(e));
      },
    );
  };
  return (
    <article
      id={insight.id}
      aria-labelledby={`${insight.id}-title`}
      className={`${card} flex flex-col gap-3 px-5 py-[18px]`}
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-tiny">
        <StatusBadge status={stateGlyph(insight.state)} label={statusLabel(insight.state)} />
        <span className="font-bold text-ink-2">{kindLabel(insight.kind)}</span>
        <Link
          to={withQuery(pipelinePath(insight.pipeline, 'graph'), { step: insight.scope })}
          className="font-mono font-bold text-ink"
        >
          {insight.scope}
        </Link>
        <span className="text-ink-3" title={formatDateTime(new Date(insight.openedAt), language)}>
          {t('insights.raised', { when: formatRelativeTime(new Date(insight.openedAt), new Date(now), language) })}
        </span>
      </div>
      <h2
        id={`${insight.id}-title`}
        className="m-0 font-display text-[28px] leading-8 font-normal break-words md:text-[32px] md:leading-9"
      >
        {title}
      </h2>
      <p className="m-0 max-w-[760px] text-ui text-ink-2 break-words">{body}</p>
      {figures.length > 0 && (
        <dl className="m-0 grid grid-cols-[repeat(auto-fit,minmax(160px,1fr))] gap-2.5">
          {figures.map((f) => (
            <div key={f.label} className="flex flex-col gap-0.5 rounded-2 bg-surface-2 px-4 py-3.5">
              <dt className="text-tiny text-ink-3">{f.label}</dt>
              <dd className="m-0 font-display text-[32px] leading-9">{f.value}</dd>
              {f.sub !== undefined && <dd className="m-0 text-tiny text-ink-2">{f.sub}</dd>}
            </div>
          ))}
        </dl>
      )}
      {samples.length > 0 && (
        <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1.5 text-tiny">
          <span className="text-ink-3">{t('insights.samples')}</span>
          {samples.map((id) => (
            <Link key={id} to={itemPath(insight.pipeline, id)} className="text-ink">
              <Id value={id} />
            </Link>
          ))}
        </div>
      )}
      {error !== null && (
        <ErrorState title={t('insights.actionFailed')} message={error} onDismiss={() => setError(null)} />
      )}
      <div className="flex flex-wrap gap-2 pt-1">
        {open && (
          <Link to={open.to} className={`${buttons.primary} no-underline`}>
            {open.label}
          </Link>
        )}
        {insight.state === 'open' && (
          <button
            type="button"
            disabled={busy}
            onClick={() => act(api.acknowledgeInsight)}
            className={buttons.secondary}
          >
            {t('insights.acknowledge')}
          </button>
        )}
        {insight.state !== 'muted' && insight.state !== 'resolved' && (
          <button type="button" disabled={busy} onClick={() => act(api.muteInsight)} className={buttons.secondary}>
            {t('insights.mute')}
          </button>
        )}
      </div>
    </article>
  );
}
