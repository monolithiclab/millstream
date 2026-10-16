import { useTranslation } from 'react-i18next';
import { enc } from '../../api/api';
import type { Run, RunState } from '../../api/schema';
import { formatClock, formatDay } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { pipelinePath } from '../../routes';
import { StatusBadge, type GlyphName } from '../parts/Status';

const glyph: Record<RunState, GlyphName> = {
  running: 'run',
  waitingReview: 'human',
  succeeded: 'ok',
  completedWithErrors: 'warnFill',
  cancelled: 'stopped',
};

export function useRunStateLabels(): Record<RunState, string> {
  const { t } = useTranslation();
  return {
    running: t('runs.state.running'),
    waitingReview: t('runs.state.waitingReview'),
    succeeded: t('runs.state.succeeded'),
    completedWithErrors: t('runs.state.completedWithErrors'),
    cancelled: t('runs.state.cancelled'),
  };
}

export function RunStateBadge({ state }: { state: RunState }) {
  const labels = useRunStateLabels();
  return <StatusBadge name={glyph[state]}>{labels[state]}</StatusBadge>;
}

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

export const runPath = (pipeline: string, run: string) => pipelinePath(pipeline, `runs/${enc(run)}`);
