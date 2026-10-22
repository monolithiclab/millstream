import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useApi } from '../../api/context';
import type { Insight } from '../../api/schema';
import { useSession } from '../../auth/SessionGate';
import { runAction, type Notice } from '../../common/Notice';
import { requestId } from '../../common/requestId';
import { usePipelines } from '../../common/usePipelines';

// The improve pipeline (spec 11.3): the package millstream/improve, loaded only where improve.enabled is set. Its
// manual trigger is "Propose a fix"; the API fires a pipeline's first manual trigger when none is named. The payload
// names the pipeline to improve and, from a card, the insight.
const IMPROVE_PIPELINE = 'improve';

// Whether Propose a fix can run here, why not (production refuses the improve pipeline; without it, improve is off),
// and how to run it, its outcome a notice.
export function useProposeFix(pipeline: string, setNotice: (notice: Notice | null) => void) {
  const { t } = useTranslation();
  const api = useApi();
  const { session } = useSession();
  const pipelines = usePipelines();
  const improve = pipelines.status === 'ready' ? pipelines.data.find((p) => p.name === IMPROVE_PIPELINE) : undefined;
  let reason: string | null = null;
  if (session?.environment === 'production') reason = t('improve.propose.production');
  else if (pipelines.status === 'ready' && (improve === undefined || improve.state === 'disabled'))
    reason = t('improve.propose.off');
  else if (improve !== undefined && !improve.triggers.some((tr) => tr.kind === 'manual'))
    reason = t('improve.propose.noTrigger');
  const [firing, setFiring] = useState(false);
  const available = reason === null && improve !== undefined && !firing;
  // Each click is its own request id: Retry, or a resent request, returns the same run instead of a second one.
  const propose = (insight?: Insight) => {
    const id = requestId();
    const payload = insight === undefined ? { pipeline } : { pipeline, insight: insight.id };
    setFiring(true);
    void runAction(setNotice, {
      call: async () => {
        const run = await api.triggerRun(IMPROVE_PIPELINE, { payload, requestId: id });
        return t('improve.propose.started', { run: run.id });
      },
      failed: (message) => t('improve.propose.failed', { message }),
      settled: () => setFiring(false),
    });
  };
  return { available, reason, propose };
}
