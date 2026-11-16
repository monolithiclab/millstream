import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { withQuery } from '../../api/api';
import { useApi } from '../../api/context';
import type { Insight, InsightState, ProposalKind } from '../../api/schema';
import { Id } from '../../common/Id';
import { errorMessage } from '../../common/Notice';
import { ErrorState } from '../../common/States';
import { useInsightKindLabel } from '../../common/insightKind';
import { PROPOSAL_KINDS, useProposalKindLabel } from '../../common/proposal';
import { StatusBadge, Tag, useStatusLabel, type StatusKey } from '../../common/Status';
import { buttons, card } from '../../common/styles';
import { useFormat } from '../../common/useFormat';
import { formatPercent } from '../../i18n/format';
import { itemPath, pipelinePath } from '../../routes';

// An open insight wants attention: the warning triangle, not the open dead letter's square.
const stateGlyph = (state: InsightState): StatusKey => (state === 'open' ? 'slow' : state);

type Evidence = Record<string, unknown>;
const num = (ev: Evidence, key: string): number | null => (typeof ev[key] === 'number' ? (ev[key] as number) : null);
const str = (ev: Evidence, key: string): string => (typeof ev[key] === 'string' ? (ev[key] as string) : '');
const list = <T,>(ev: Evidence, key: string): T[] => (Array.isArray(ev[key]) ? (ev[key] as T[]) : []);

// The sample item ids of an insight's evidence.
export const samplesOf = (insight: Insight): string[] => list<string>(insight.evidence as Evidence, 'samples');
const record = (value: unknown): Evidence =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? (value as Evidence) : {};

interface Figure {
  label: string;
  value: string;
  sub?: string;
}

interface Reading {
  title: string;
  body: ReactNode;
  figures: Figure[];
  // What the kind adds below its figures: reviewPattern's attribution and proposal hints.
  extra?: ReactNode;
  open?: { to: string; label: string };
}

// The title, body, figures and way in of an insight, from its kind's evidence (internal/insights).
function useReading(insight: Insight): Reading {
  const { t } = useTranslation();
  const f = useFormat();
  const ev = insight.evidence as Evidence;
  const step = insight.scope;
  // An evidence figure, or a dash when the engine had too little data for it.
  const fig = (key: string, format: (v: number) => string) => {
    const v = num(ev, key);
    return v === null ? '—' : format(v);
  };
  const pct = (key: string) => fig(key, (v) => formatPercent(v, f.language));
  const secs = (key: string) => fig(key, (v) => f.duration(v * 1000));
  const money = (key: string) => fig(key, f.money);
  const count = (key: string) => f.num(num(ev, key) ?? 0);
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
      const pointers = list<{ pointer?: string }>(ev, 'editHotSpots')
        .map((s) => s.pointer ?? '')
        .filter((p) => p !== '');
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
        extra: <ReviewLearning evidence={ev} />,
        open: metrics('review_decisions'),
      };
    }
    case 'drift':
      return {
        title: t('insights.title.drift', { step }),
        body: t('insights.body.drift', {
          disagreed: count('disagreed'),
          audits: f.num((num(ev, 'agreed') ?? 0) + (num(ev, 'disagreed') ?? 0)),
          threshold: pct('threshold'),
        }),
        figures: [
          { label: t('insights.figure.auditAgreement'), value: pct('agreement') },
          {
            label: t('insights.figure.disagreed'),
            value: count('disagreed'),
            sub: t('insights.figure.auditsFailed', { count: count('failed') }),
          },
          { label: t('insights.figure.audits'), value: count('audits') },
        ],
      };
    case 'waste': {
      const by = list<{ step?: string; kind?: string; items?: number }>(ev, 'byStep');
      const suggestion = record(ev.suggestion);
      return {
        title: t('insights.title.waste', { step, cost: money('wastedCostUsd') }),
        body: (
          <>
            {t('insights.body.waste', {
              steps: by.map((d) => `${d.step ?? ''} (${f.num(d.items ?? 0)})`).join(', '),
            })}
            {typeof suggestion.moveUpstream === 'string' && (
              <> {t('insights.body.wasteMove', { filter: suggestion.moveUpstream, step: str(suggestion, 'before') })}</>
            )}
          </>
        ),
        figures: [
          { label: t('insights.figure.discarded'), value: count('discardedItems') },
          {
            label: t('insights.figure.wasted'),
            value: money('wastedCostUsd'),
            sub: t('insights.figure.ofStepCost', { value: pct('wasteShare') }),
          },
          { label: t('insights.figure.stepCost'), value: money('costUsd') },
        ],
        open: metrics('delivery_cost_usd'),
      };
    }
    case 'distillable': {
      const fields = record(ev.fields);
      const names = (key: string) => {
        const values = list<string>(fields, key);
        return values.length === 0 ? '—' : values.join(', ');
      };
      return {
        title: t('insights.title.distillable', { step }),
        body: t('insights.body.distillable', {
          share: pct('boundedShare'),
          bounded: names('bounded'),
          copied: names('copied'),
          free: names('free'),
        }),
        figures: [
          {
            label: t('insights.figure.saving'),
            value: money('monthlySavingUsd'),
            sub: t('insights.figure.rank', { rank: count('rank') }),
          },
          {
            label: t('insights.figure.turns'),
            value: fig('turnsP95', f.num),
            sub: t('insights.figure.deliveriesCount', { count: count('deliveries') }),
          },
          {
            label: t('insights.figure.reviewerAgreement'),
            value: pct('reviewerAgreement'),
            sub: t('insights.figure.reviewed', { count: count('reviewedDecisions') }),
          },
        ],
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

type Hint = Record<string, unknown>;

// A proposal hint of a reviewPattern (spec 11.6) in words: the pattern, and the proposal it calls for.
function useHintText(): (hint: Hint) => string {
  const { t } = useTranslation();
  const f = useFormat();
  const unattributed = t('insights.hint.unattributed');
  return (hint) => {
    const n = (key: string) => f.num(typeof hint[key] === 'number' ? (hint[key] as number) : 0);
    const s = (key: string) => (typeof hint[key] === 'string' ? (hint[key] as string) : unattributed);
    switch (hint.kind) {
      case 'topology':
        return t('insights.hint.topology', {
          condition: s('condition'),
          after: s('after'),
          before: s('before'),
          rejections: n('rejections'),
          falseDrops: n('falseDrops'),
          approvals: n('approvals'),
        });
      case 'lessons':
        return t('insights.hint.lessons', { step: s('step'), pointer: s('pointer'), edits: n('edits') });
      case 'config':
        return t('insights.hint.config', {
          step: s('step'),
          decisions: n('decisions'),
          rate: formatPercent(typeof hint.approvalRate === 'number' ? hint.approvalRate : 0, f.language),
        });
      case 'reasons':
        return t('insights.hint.reasons', { step: s('step'), rejections: n('rejections') });
      default:
        return String(hint.pattern ?? hint.kind ?? '');
    }
  };
}

// From M5, a reviewPattern says who authored the fields reviewers edit, and which proposals its patterns call for.
function ReviewLearning({ evidence }: { evidence: Evidence }) {
  const { t } = useTranslation();
  const f = useFormat();
  const attribution = list<{ pointer?: string; step?: string | null; edits?: number }>(evidence, 'attribution');
  const hints = list<Hint>(evidence, 'proposals');
  const describe = useHintText();
  const unattributed = num(evidence, 'unattributedEdits') ?? 0;
  const kindLabel = useProposalKindLabel();
  // A hint names a proposal kind, or new reason codes, which no proposal kind covers.
  const hintKind = (kind: unknown) =>
    kind === 'reasons'
      ? t('insights.hint.reasonCodes')
      : PROPOSAL_KINDS.includes(kind as ProposalKind)
        ? kindLabel(kind as ProposalKind)
        : String(kind);
  if (attribution.length === 0 && hints.length === 0) return null;
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {attribution.length > 0 && (
        <div className="flex min-w-0 flex-col gap-1.5">
          <h3 className="m-0 text-tiny font-bold tracking-wide text-ink-3 uppercase">{t('insights.attribution')}</h3>
          <ul className="m-0 flex list-none flex-col gap-1 p-0 text-small">
            {attribution.map((a) => (
              <li key={`${a.pointer ?? ''}:${a.step ?? ''}`} className="break-words">
                {t('insights.attributed', {
                  pointer: a.pointer ?? '',
                  step: a.step ?? t('insights.hint.unattributed'),
                  edits: f.num(a.edits ?? 0),
                })}
              </li>
            ))}
          </ul>
          {unattributed > 0 && (
            <p className="m-0 text-tiny text-ink-2">{t('insights.unattributed', { count: f.num(unattributed) })}</p>
          )}
        </div>
      )}
      {hints.length > 0 && (
        <div className="flex min-w-0 flex-col gap-1.5">
          <h3 className="m-0 text-tiny font-bold tracking-wide text-ink-3 uppercase">{t('insights.hints')}</h3>
          <ul className="m-0 flex list-none flex-col gap-1.5 p-0 text-small">
            {hints.map((hint, i) => (
              <li key={i} className="flex flex-wrap items-baseline gap-x-2 gap-y-1 break-words">
                <Tag tone="human">{hintKind(hint.kind)}</Tag>
                <span className="min-w-0">{describe(hint)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

// An insight with its evidence, Acknowledge or Mute, and an optional action of the screen showing it (the Improve
// view's Propose a fix).
export function InsightCard({
  insight,
  now,
  onChanged,
  action,
}: {
  insight: Insight;
  now: number;
  onChanged: () => void;
  action?: ReactNode;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const { title, body, figures, extra, open } = useReading(insight);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const statusLabel = useStatusLabel();
  const kindLabel = useInsightKindLabel();
  const act = (call: (p: string, i: string) => Promise<void>) => {
    setBusy(true);
    setError(null);
    call(insight.pipeline, insight.id).then(
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
      tabIndex={-1}
      className={`${card} flex scroll-mt-4 flex-col gap-3 px-5 py-[18px]`}
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
        <span className="text-ink-3" title={f.dateTime(insight.openedAt)}>
          {t('insights.raised', { when: f.relative(insight.openedAt, new Date(now)) })}
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
          {figures.map((fig) => (
            <div key={fig.label} className="flex flex-col gap-0.5 rounded-2 bg-surface-2 px-4 py-3.5">
              <dt className="text-tiny text-ink-3">{fig.label}</dt>
              <dd className="m-0 font-display text-[32px] leading-9">{fig.value}</dd>
              {fig.sub !== undefined && <dd className="m-0 text-tiny text-ink-2">{fig.sub}</dd>}
            </div>
          ))}
        </dl>
      )}
      {extra}
      <SampleLinks pipeline={insight.pipeline} ids={samplesOf(insight)} />
      {error !== null && (
        <ErrorState title={t('insights.actionFailed')} message={error} onDismiss={() => setError(null)} />
      )}
      <div className="flex flex-wrap gap-2 pt-1">
        {open && (
          <Link to={open.to} className={`${buttons.primary} no-underline`}>
            {open.label}
          </Link>
        )}
        {action}
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

// An insight's sample items, each a link to the item (the evidence behind it); nothing when it has none.
export function SampleLinks({ pipeline, ids }: { pipeline: string; ids: string[] }) {
  const { t } = useTranslation();
  if (ids.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1.5 text-tiny">
      <span className="text-ink-3">{t('insights.samples')}</span>
      {ids.map((id) => (
        <Link key={id} to={itemPath(pipeline, id)} className="text-ink">
          <Id value={id} />
        </Link>
      ))}
    </div>
  );
}
