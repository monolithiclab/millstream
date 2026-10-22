import { useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams } from 'react-router-dom';
import { ApiError, enc } from '../../api/api';
import { useApi } from '../../api/context';
import type { Insight, Proposal, Revision } from '../../api/schema';
import { Breadcrumb, pipelineCrumb } from '../../common/Breadcrumb';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { Id } from '../../common/Id';
import { errorMessage, NoticeRegion, useNotice } from '../../common/Notice';
import { ErrorState, QueryView } from '../../common/States';
import { StatusBadge, Tag, useStatusLabel } from '../../common/Status';
import { buttons, card, cardTitle } from '../../common/styles';
import { useFormat } from '../../common/useFormat';
import { isDecided, useProposalKindLabel } from '../../common/proposal';
import { useResource, type Resource } from '../../common/useResource';
import { useInsightKindLabel } from '../../common/insightKind';
import { pipelinePath } from '../../routes';
import { SampleLinks, samplesOf } from '../insights/InsightCard';
import { ScreenPage } from '../ScreenPage';
import { DiffViewer } from './DiffViewer';
import { ValidationReport } from './ValidationReport';

type Dialog = 'accept' | 'reject' | null;

// A card of the page: its heading names the region.
function Card({ title, children }: { title: string; children: ReactNode }) {
  const id = useId();
  return (
    <section aria-labelledby={id} className={`${card} flex flex-col gap-3 px-5 py-[18px]`}>
      <h2 id={id} className={cardTitle}>
        {title}
      </h2>
      {children}
    </section>
  );
}

// One proposal (spec 11.3): what it changes and why, the patch, the validation, the evidence and its history; Accept
// a ready one (naming the patch read), Reject one still open or stale with a reason, Revalidate a stale one.
export function ProposalScreen() {
  const { t } = useTranslation();
  const { pipeline = '', proposal: id = '' } = useParams();
  const proposal = useResource(`proposal:${id}`, (api) => api.getProposal(id));
  // Read with the proposal, not after it: they need only the pipeline.
  const revisions = useResource(`revisions:${pipeline}`, (api) => api.listRevisions(pipeline));
  const insights = useResource(`insights:${pipeline}`, (api) => api.listInsights({ pipeline }));
  const kindLabel = useProposalKindLabel();
  return (
    <ScreenPage
      title={t('improve.title', { id })}
      context={
        <Breadcrumb
          items={[pipelineCrumb(pipeline), { label: t('nav.improve'), to: pipelinePath(pipeline, 'improve') }]}
        />
      }
      status={
        proposal.status === 'ready' ? (
          <>
            <StatusBadge status={proposal.data.state} />
            <Tag tone="human">{kindLabel(proposal.data.kind)}</Tag>
          </>
        ) : undefined
      }
    >
      <QueryView query={proposal} title={t('improve.loadFailed', { id })}>
        {(p) => <ProposalView proposal={p} revisions={revisions} insights={insights} onChanged={proposal.refresh} />}
      </QueryView>
    </ScreenPage>
  );
}

function ProposalView({
  proposal: p,
  revisions,
  insights,
  onChanged,
}: {
  proposal: Proposal;
  revisions: Resource<Revision[]>;
  insights: Resource<Insight[]>;
  onChanged: () => void;
}) {
  const { t } = useTranslation();
  const api = useApi();
  const statusLabel = useStatusLabel();
  const { notice, setNotice, dismiss } = useNotice();
  const [dialog, setDialog] = useState<Dialog>(null);
  const [refusal, setRefusal] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const base = revisions.status === 'ready' ? revisions.data.find((r) => r.packageHash === p.baseRevision) : undefined;

  // The server's refusal stays on the page (a moved base, a changed patch: 409 says why), and the proposal is read
  // again, since a refusal may have made it stale.
  const act = async (call: () => Promise<Proposal>, done: (q: Proposal) => string) => {
    setRefusal(null);
    try {
      const q = await call();
      setNotice({ kind: 'ok', message: done(q) });
    } catch (e) {
      setRefusal(
        e instanceof ApiError && e.status === 409 ? t('improve.conflict', { message: e.message }) : errorMessage(e),
      );
    } finally {
      setDialog(null);
      onChanged();
    }
  };
  const revalidate = () => {
    setBusy(true);
    void act(
      () => api.revalidateProposal(p.id),
      (q) => t('improve.revalidated', { state: statusLabel(q.state) }),
    ).finally(() => setBusy(false));
  };

  return (
    <>
      <div className="flex flex-col gap-4">
        <Card title={t('improve.why')}>
          <p className="m-0 max-w-[760px] text-body break-words">{p.rationale || t('improve.noRationale')}</p>
          <Facts proposal={p} base={base} activeKnown={revisions.status === 'ready'} />
          {refusal !== null && (
            <ErrorState title={t('improve.refused')} message={refusal} onDismiss={() => setRefusal(null)} />
          )}
          <div className="flex flex-wrap gap-2 pt-1">
            {p.state === 'ready' && (
              <button type="button" className={buttons.primary} onClick={() => setDialog('accept')}>
                {t('improve.accept.action')}
              </button>
            )}
            {(p.state === 'stale' || p.state === 'drafting') && (
              <button type="button" className={buttons.secondary} onClick={revalidate} disabled={busy} aria-busy={busy}>
                {t('improve.revalidate')}
              </button>
            )}
            {!isDecided(p.state) && (
              <button type="button" className={buttons.danger} onClick={() => setDialog('reject')}>
                {t('improve.reject.action')}
              </button>
            )}
          </div>
          {p.state === 'validating' && <p className="m-0 text-tiny text-ink-2">{t('improve.validatingNote')}</p>}
          {p.state === 'stale' && <p className="m-0 text-tiny text-ink-2">{t('improve.staleNote')}</p>}
        </Card>

        <Card title={t('improve.validation.title')}>
          <ValidationReport report={p.report} state={p.state} />
        </Card>

        <Card title={t('improve.patch')}>
          <DiffViewer patch={p.patch} />
        </Card>

        <Evidence proposal={p} insights={insights} />
        <History proposal={p} />
      </div>
      {dialog === 'accept' && (
        <ConfirmDialog
          title={t('improve.accept.title', { id: p.id })}
          confirmLabel={t('improve.accept.confirm')}
          onCancel={() => setDialog(null)}
          onConfirm={() =>
            act(
              () => api.acceptProposal(p.id, { patchSha256: p.patchSha256 }),
              () => t('improve.accept.done', { id: p.id }),
            )
          }
        >
          <Consequences>
            <li>{t('improve.accept.publish')}</li>
            <li>{t('improve.accept.base', { revision: base === undefined ? p.baseRevision : `r${base.id}` })}</li>
            <li>{t('improve.accept.others')}</li>
            <li>
              {t('improve.accept.hash')} <Id value={p.patchSha256} />
            </li>
          </Consequences>
        </ConfirmDialog>
      )}
      {dialog === 'reject' && (
        <RejectDialog
          proposal={p}
          onCancel={() => setDialog(null)}
          onReject={(reason) =>
            act(
              () => api.rejectProposal(p.id, { reason }),
              () => t('improve.reject.done', { id: p.id }),
            )
          }
        />
      )}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </>
  );
}

function RejectDialog({
  proposal,
  onCancel,
  onReject,
}: {
  proposal: Proposal;
  onCancel: () => void;
  onReject: (reason: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [reason, setReason] = useState('');
  const fieldId = useId();
  const hintId = useId();
  return (
    <ConfirmDialog
      title={t('improve.reject.title', { id: proposal.id })}
      confirmLabel={t('improve.reject.confirm')}
      destructive
      confirmDisabled={reason.trim() === ''}
      onCancel={onCancel}
      onConfirm={() => onReject(reason.trim())}
    >
      <label htmlFor={fieldId} className="text-small font-bold text-ink">
        {t('improve.reject.reason')}
      </label>
      <textarea
        id={fieldId}
        value={reason}
        required
        aria-describedby={hintId}
        onChange={(e) => setReason(e.target.value)}
        rows={3}
        data-autofocus
        className="rounded-1 border border-control-line bg-bg px-3 py-2 text-[16px] text-ink"
      />
      <p id={hintId} className="m-0 text-small">
        {t('improve.reject.hint')}
      </p>
    </ConfirmDialog>
  );
}

function Facts({
  proposal: p,
  base,
  activeKnown,
}: {
  proposal: Proposal;
  base: Revision | undefined;
  activeKnown: boolean;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const baseText =
    base === undefined
      ? activeKnown
        ? t('improve.facts.baseUnknown')
        : ''
      : base.active
        ? t('improve.facts.baseActive', { id: base.id })
        : t('improve.facts.baseInactive', { id: base.id });
  const facts: [string, ReactNode][] = [
    [
      t('improve.facts.base'),
      <>
        <Id value={p.baseRevision} /> {baseText}
      </>,
    ],
    [t('improve.facts.steps'), <span className="font-mono">{p.steps.join(', ') || '—'}</span>],
    [t('improve.facts.paths'), <span className="font-mono break-all">{p.paths.join(', ') || '—'}</span>],
    [t('improve.facts.patch'), <Id value={p.patchSha256} />],
    [t('improve.facts.created'), t('improve.facts.createdValue', { who: p.createdBy, when: f.dateTime(p.createdAt) })],
  ];
  // Spec 11.3: an accepted proposal goes to improve.publish's action, which says where it put it.
  if (p.state === 'accepted') {
    facts.push([
      t('improve.facts.published'),
      p.published ? (
        <>
          {t('improve.facts.publishedValue', { publisher: p.published.publisher, when: f.dateTime(p.published.at) })}{' '}
          <span className="font-mono break-all">{p.published.location}</span>
        </>
      ) : (
        t('improve.facts.notPublished')
      ),
    ]);
  }
  return (
    <dl className="m-0 grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-4 gap-y-1.5 text-small">
      {facts.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-ink-3">{label}</dt>
          <dd className="m-0 break-words">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

// The insights the proposal answers, each with its sample items: the evidence it was drafted from.
function Evidence({ proposal: p, insights }: { proposal: Proposal; insights: Resource<Insight[]> }) {
  const { t } = useTranslation();
  const kindLabel = useInsightKindLabel();
  return (
    <Card title={t('improve.evidence')}>
      {p.insights.length === 0 ? (
        <p className="m-0 text-small text-ink-2">{t('improve.noInsights')}</p>
      ) : (
        <QueryView query={insights} rows={1}>
          {(all) => (
            <ul className="m-0 flex list-none flex-col gap-3 p-0">
              {p.insights.map((id) => {
                const insight: Insight | undefined = all.find((i) => i.id === id);
                return (
                  <li key={id} className="flex flex-col gap-1">
                    <Link to={`${pipelinePath(p.pipeline, 'insights')}#${enc(id)}`} className="text-small font-bold">
                      {insight === undefined
                        ? t('improve.insightGone', { id })
                        : t('improve.insightLink', { kind: kindLabel(insight.kind), step: insight.scope })}
                    </Link>
                    {insight !== undefined && insight.summary !== '' && (
                      <p lang="en" className="m-0 text-small break-words text-ink-2">
                        {insight.summary}
                      </p>
                    )}
                    {insight !== undefined && <SampleLinks pipeline={p.pipeline} ids={samplesOf(insight)} />}
                  </li>
                );
              })}
            </ul>
          )}
        </QueryView>
      )}
    </Card>
  );
}

// What the proposal went through: created, validated, its state since its last change, and the decision.
function History({ proposal: p }: { proposal: Proposal }) {
  const { t } = useTranslation();
  const f = useFormat();
  const statusLabel = useStatusLabel();
  const events: string[] = [t('improve.history.created', { who: p.createdBy, when: f.dateTime(p.createdAt) })];
  if (p.report !== null) {
    events.push(p.report.passed ? t('improve.history.validated') : t('improve.history.notValidated'));
  }
  if (isDecided(p.state)) {
    const who = p.decidedBy ?? '—';
    const when = f.dateTime(p.decidedAt ?? p.updatedAt);
    events.push(
      p.state === 'accepted'
        ? t('improve.history.accepted', { who, when })
        : t('improve.history.rejected', { who, when, reason: p.reason ?? '' }),
    );
  } else {
    events.push(t('improve.history.since', { state: statusLabel(p.state), when: f.dateTime(p.updatedAt) }));
  }
  return (
    <Card title={t('improve.history.title')}>
      <ol className="m-0 flex flex-col gap-1.5 pl-5 text-small">
        {events.map((e) => (
          <li key={e} className="break-words">
            {e}
          </li>
        ))}
      </ol>
    </Card>
  );
}
