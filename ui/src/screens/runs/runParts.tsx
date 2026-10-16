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

// What opened a run, as its kind says: a replay, an imported stream from its producer, or a trigger.
export function useRunOrigin() {
  const { t } = useTranslation();
  return (run: Pick<Run, 'kind' | 'trigger' | 'triggerSource' | 'producer'>) => {
    if (run.kind === 'replay') return t('runs.replayRun');
    if (run.kind === 'import') return t('runs.importRun', { stream: run.trigger ?? '', pipeline: run.producer ?? '' });
    return t('runs.trigger', { trigger: run.trigger ?? run.triggerSource });
  };
}
