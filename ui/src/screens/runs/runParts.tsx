import { useTranslation } from 'react-i18next';
import type { Run, RunState } from '../../api/schema';
import { formatClock, formatDay } from '../../i18n/format';
import { usePreferences } from '../../preferences';

export const settledStates = new Set<RunState>(['succeeded', 'completedWithErrors', 'cancelled']);

// "Run 06:00 · Thu, Oct 15": a run is named by when it started.
export function useRunName() {
  const { t } = useTranslation();
  const { language } = usePreferences();
  return (run: Pick<Run, 'startedAt'>) => {
    const date = new Date(run.startedAt);
    return t('runs.name', { time: formatClock(date, language), day: formatDay(date, language) });
  };
}
