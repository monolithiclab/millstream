import { useTranslation } from 'react-i18next';

// An insight kind in words (spec section 11.1); a kind this UI does not know yet keeps its name.
export function useInsightKindLabel() {
  const { t } = useTranslation();
  const labels: Record<string, string> = {
    slowStep: t('insights.kind.slowStep'),
    failureCluster: t('insights.kind.failureCluster'),
    cost: t('insights.kind.cost'),
    reviewPattern: t('insights.kind.reviewPattern'),
    unschedulable: t('insights.kind.unschedulable'),
    waste: t('insights.kind.waste'),
    drift: t('insights.kind.drift'),
    distillable: t('insights.kind.distillable'),
  };
  return (kind: string) => labels[kind] ?? kind;
}
