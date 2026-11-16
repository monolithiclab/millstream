import { useTranslation } from 'react-i18next';
import type { ProposalKind, ProposalState } from '../api/schema';

// A proposal still to decide (spec 11.3): drafting, validating or ready; not stale, accepted or rejected.
export const isOpenProposal = (state: ProposalState) =>
  state === 'drafting' || state === 'validating' || state === 'ready';

// A proposal decided for good.
export const isDecided = (state: ProposalState) => state === 'accepted' || state === 'rejected';

export const PROPOSAL_KINDS: readonly ProposalKind[] = ['config', 'lessons', 'topology', 'distill'];

export function useProposalKindLabel() {
  const { t } = useTranslation();
  const labels: Record<ProposalKind, string> = {
    config: t('improve.kind.config'),
    lessons: t('improve.kind.lessons'),
    topology: t('improve.kind.topology'),
    distill: t('improve.kind.distill'),
  };
  return (kind: ProposalKind) => labels[kind];
}
