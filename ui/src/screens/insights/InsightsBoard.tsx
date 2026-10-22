import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { Insight } from '../../api/schema';
import { useInsightKindLabel } from '../../common/insightKind';
import { Select } from '../../common/Select';
import { filterLabel } from '../../common/styles';
import { useNow } from '../../common/useNow';
import { InsightCard } from './InsightCard';

// Insights by kind and step (spec 11.1): the kinds to show and a step on the side, the cards beside them. The
// Insights screen shows one state tab of them, the Improve view those needing attention, each with Propose a fix.
export function InsightsBoard({
  insights,
  empty,
  onChanged,
  action,
}: {
  insights: Insight[];
  // What shows when no insight passes the filters.
  empty: ReactNode;
  onChanged: () => void;
  action?: (insight: Insight) => ReactNode;
}) {
  const { t } = useTranslation();
  const kindLabel = useInsightKindLabel();
  const [hidden, setHidden] = useState<string[]>([]);
  const [step, setStep] = useState('');
  const now = useNow();
  const kinds = [...new Set(insights.map((i) => i.kind))].sort();
  const steps = [...new Set(insights.map((i) => i.scope))].sort();
  // A step chosen among other insights (another tab) does not filter these.
  const activeStep = steps.includes(step) ? step : '';
  const shown = insights.filter((i) => !hidden.includes(i.kind) && (activeStep === '' || i.scope === activeStep));
  return (
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
              onChange={() => setHidden(hidden.includes(kind) ? hidden.filter((k) => k !== kind) : [...hidden, kind])}
              className="size-[15px] accent-accent"
            />
            <span className="flex-1">{kindLabel(kind)}</span>
            <span className="font-mono text-tiny text-ink-3">{insights.filter((i) => i.kind === kind).length}</span>
          </label>
        ))}
        {steps.length > 1 && (
          <label className={`${filterLabel} mt-3 px-2`}>
            {t('insights.step')}
            <Select value={activeStep} onChange={(e) => setStep(e.target.value)} className="font-mono font-normal">
              <option value="">{t('insights.allSteps')}</option>
              {steps.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </Select>
          </label>
        )}
      </aside>
      <div className="flex min-w-0 flex-[999_1_640px] flex-col gap-4">
        {shown.length === 0
          ? empty
          : shown.map((insight) => (
              <InsightCard
                key={insight.id}
                insight={insight}
                now={now}
                onChanged={onChanged}
                action={action?.(insight)}
              />
            ))}
      </div>
    </div>
  );
}
