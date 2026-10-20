import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import type { Proposal, ProposalKind, ProposalState } from '../../api/schema';
import { NoticeRegion, useNotice } from '../../common/Notice';
import { isOpenProposal, useProposalKindLabel } from '../../common/proposal';
import { Segmented } from '../../common/Segmented';
import { EmptyState, QueryView } from '../../common/States';
import { StatusBadge, StatusText, Tag } from '../../common/Status';
import { buttons, card, field } from '../../common/styles';
import { useFormat } from '../../common/useFormat';
import { useNow } from '../../common/useNow';
import { useResource } from '../../common/useResource';
import { proposalPath } from '../../routes';
import { InsightsBoard } from '../insights/InsightsBoard';
import { ScreenPage } from '../ScreenPage';
import { useProposeFix } from './useProposeFix';

type View = 'proposals' | 'insights';
// Drafting, validating and ready are open: still to decide.
type Tab = 'open' | 'stale' | 'accepted' | 'rejected';
const tabOf = (state: ProposalState): Tab => (isOpenProposal(state) ? 'open' : (state as Exclude<Tab, 'open'>));
const tabs: Tab[] = ['open', 'stale', 'accepted', 'rejected'];

// The Improve view (spec 12.2): the proposals the improve pipeline drafted, by state and kind, and the insights
// needing attention with Propose a fix, which runs the improve pipeline by hand.
export function ImproveScreen() {
  const { t } = useTranslation();
  const { pipeline = '' } = useParams();
  const [search, setSearch] = useSearchParams();
  const proposals = useResource(`proposals:${pipeline}`, (api) => api.listProposals({ pipeline }));
  const insights = useResource(`insights:${pipeline}`, (api) => api.listInsights({ pipeline }));
  const { notice, setNotice, dismiss } = useNotice();
  const fix = useProposeFix(pipeline, setNotice);
  const reasonId = useId();
  const view: View = search.get('view') === 'insights' ? 'insights' : 'proposals';
  const attention =
    insights.status === 'ready' ? insights.data.filter((i) => i.state === 'open' || i.state === 'acknowledged') : [];
  const open = proposals.status === 'ready' ? proposals.data.filter((p) => tabOf(p.state) === 'open').length : 0;
  const proposeButton = (label: string, onClick: () => void, className: string) => (
    <button
      type="button"
      disabled={!fix.available}
      aria-describedby={fix.reason === null ? undefined : reasonId}
      onClick={onClick}
      className={className}
    >
      {label}
    </button>
  );

  return (
    <ScreenPage
      title={t('nav.improve')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{pipeline}</p>}
      header={
        <>
          <Segmented<View>
            label={t('improve.viewLabel')}
            value={view}
            options={[
              { value: 'proposals', label: t('improve.view.proposals', { count: open }) },
              { value: 'insights', label: t('improve.view.insights', { count: attention.length }) },
            ]}
            onChange={(value) => setSearch(value === 'proposals' ? {} : { view: value })}
          />
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 md:ml-auto">
            {fix.reason !== null && (
              <p id={reasonId} className="m-0 max-w-[420px] text-tiny text-ink-2">
                {fix.reason}
              </p>
            )}
            {proposeButton(t('improve.proposeFix'), () => fix.propose(), buttons.primary)}
          </div>
        </>
      }
    >
      <p className="m-0 max-w-[760px] text-small text-ink-2">{t('improve.explain')}</p>
      {view === 'proposals' ? (
        <QueryView query={proposals}>{(all) => <ProposalList pipeline={pipeline} proposals={all} />}</QueryView>
      ) : (
        <QueryView query={insights}>
          {() => (
            <InsightsBoard
              insights={attention}
              onChanged={insights.refresh}
              empty={
                <EmptyState title={t('insights.none')} done>
                  {t('insights.noneBody')}
                </EmptyState>
              }
              action={(insight) =>
                proposeButton(t('improve.proposeFix'), () => fix.propose(insight), buttons.secondary)
              }
            />
          )}
        </QueryView>
      )}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}

function ProposalList({ pipeline, proposals }: { pipeline: string; proposals: Proposal[] }) {
  const { t } = useTranslation();
  const [search, setSearch] = useSearchParams();
  const kindLabel = useProposalKindLabel();
  const now = useNow();
  const summarize = useValidationSummary();
  const set = (key: string, value: string | null) => {
    const next = new URLSearchParams(search);
    if (value === null) next.delete(key);
    else next.set(key, value);
    setSearch(next);
  };
  const tab = tabs.find((x) => x === search.get('state')) ?? 'open';
  const counts: Record<Tab, number> = { open: 0, stale: 0, accepted: 0, rejected: 0 };
  for (const p of proposals) counts[tabOf(p.state)] += 1;
  const inTab = proposals.filter((p) => tabOf(p.state) === tab);
  const kinds = [...new Set(inTab.map((p) => p.kind))].sort();
  const kind = kinds.find((k) => k === search.get('kind'));
  const shown = inTab.filter((p) => kind === undefined || p.kind === kind);
  const tabLabels: Record<Tab, string> = {
    open: t('improve.tab.open', { count: counts.open }),
    stale: t('improve.tab.stale', { count: counts.stale }),
    accepted: t('improve.tab.accepted', { count: counts.accepted }),
    rejected: t('improve.tab.rejected', { count: counts.rejected }),
  };
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-end gap-3">
        <Segmented<Tab>
          label={t('improve.stateLabel')}
          value={tab}
          options={tabs.map((value) => ({ value, label: tabLabels[value] }))}
          onChange={(value) => set('state', value === 'open' ? null : value)}
        />
        {kinds.length > 1 && (
          <label className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
            {t('improve.kindLabel')}
            <select
              value={kind ?? ''}
              onChange={(e) => set('kind', e.target.value === '' ? null : e.target.value)}
              className={`${field} font-normal`}
            >
              <option value="">{t('improve.allKinds')}</option>
              {kinds.map((k: ProposalKind) => (
                <option key={k} value={k}>
                  {kindLabel(k)}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>
      {shown.length === 0 ? (
        <EmptyState title={tab === 'open' ? t('improve.none') : t('improve.noneHere')} done={tab === 'open'}>
          {tab === 'open' && t('improve.noneBody')}
        </EmptyState>
      ) : (
        <ul aria-label={t('improve.listLabel')} className="m-0 flex list-none flex-col gap-3 p-0">
          {shown.map((p) => (
            <ProposalRow
              key={p.id}
              pipeline={pipeline}
              proposal={p}
              now={now}
              kind={kindLabel(p.kind)}
              summary={summarize(p)}
            />
          ))}
        </ul>
      )}
    </div>
  );
}

// The validation in one line: not run yet, passed, or what failed.
function useValidationSummary() {
  const { t } = useTranslation();
  return (p: Proposal): { passed: boolean | null; text: string } => {
    const r = p.report;
    if (r === null) return { passed: null, text: t('improve.summary.notYet') };
    if (r.passed) return { passed: true, text: t('improve.summary.passed') };
    const failed = [
      r.check?.passed === false && t('improve.summary.check'),
      r.evals?.some((e) => !e.passed) === true && t('improve.summary.evals'),
      r.backtest?.passed === false && t('improve.summary.backtest'),
    ].filter((x): x is string => typeof x === 'string');
    return { passed: false, text: t('improve.summary.failed', { what: failed.join(', ') || (r.error ?? '') }) };
  };
}

function ProposalRow({
  pipeline,
  proposal,
  now,
  kind,
  summary,
}: {
  pipeline: string;
  proposal: Proposal;
  now: number;
  kind: string;
  summary: { passed: boolean | null; text: string };
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const titleId = useId();
  return (
    <li aria-labelledby={titleId} className={`${card} flex flex-col gap-2 px-5 py-4`}>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-tiny">
        <StatusBadge status={proposal.state} />
        <Tag tone="human">{kind}</Tag>
        <span className="font-mono text-ink-2">{proposal.steps.join(', ')}</span>
        <span className="text-ink-3" title={f.dateTime(proposal.createdAt)}>
          {t('improve.created', { who: proposal.createdBy, when: f.relative(proposal.createdAt, new Date(now)) })}
        </span>
      </div>
      <Link id={titleId} to={proposalPath(pipeline, proposal.id)} className="text-ui font-bold break-words text-ink">
        <span className="font-mono">{proposal.id}</span> · {proposal.rationale || t('improve.noRationale')}
      </Link>
      <p className="m-0 flex flex-wrap items-center gap-x-3 gap-y-1 text-small text-ink-2">
        {summary.passed === null ? (
          <span>{summary.text}</span>
        ) : (
          <StatusText status={summary.passed ? 'passed' : 'failed'} label={summary.text} />
        )}
        <span className="font-mono text-tiny break-all">{proposal.paths.join(' · ')}</span>
      </p>
      {proposal.state === 'rejected' && proposal.reason !== undefined && (
        <p className="m-0 text-small break-words text-ink-2">
          {t('improve.rejectedBecause', { reason: proposal.reason })}
        </p>
      )}
    </li>
  );
}
