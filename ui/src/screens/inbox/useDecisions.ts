import { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ApiError, type SendOptions } from '../../api/api';
import { useApi } from '../../api/context';
import type { ReviewDecision, SampleDecision } from '../../api/schema';
import { errorMessage } from '../../common/Notice';
import { useShellNotice } from '../../shell/ShellNotice';
import { useLatest } from '../../useLatest';
import { titleOf } from './ReviewPanel';
import { subject, type Entry } from './useInboxQueue';

// Interaction notes: u undoes the last decision for 10 s. The decision is held here that long before it is sent,
// so an undo never reaches the server.
export const UNDO_MS = 10_000;

export type Outcome = 'approved' | 'edited' | 'rejected' | 'agreed' | 'disagreed';

export type Decision = { entry: Entry; outcome: Outcome } & (
  { kind: 'review'; body: ReviewDecision } | { kind: 'sample'; body: SampleDecision }
);

export interface DecisionEvents {
  // The card leaves the queue at once, and comes back on undo or when the server refused for another reason.
  onRemove: (decision: Decision) => void;
  onRestore: (decision: Decision) => void;
  // 409: decided elsewhere or timed out. The card stays gone; the server's message says what happened.
  onConflict: (decision: Decision, error: ApiError) => void;
  onError: (decision: Decision, error: Error) => void;
}

// The decisions of the inbox, each held for the undo window, then sent. A decision still held is never dropped: it
// is sent when the next one is made, when the screen goes away, and when the tab is hidden or closed (pagehide,
// visibilitychange), then with keepalive so the request outlives the page. An answer that comes back after the
// screen has gone is said in the shell's notice.
export function useDecisions(events: DecisionEvents) {
  const api = useApi();
  const { t } = useTranslation();
  const shellNotice = useShellNotice();
  const latest = useLatest({ events, t, shellNotice });
  const held = useRef<{ decision: Decision; timer: ReturnType<typeof setTimeout> } | null>(null);
  const mounted = useRef(true);
  const [pending, setPending] = useState<Decision | null>(null);

  const send = useCallback(
    (decision: Decision, options?: SendOptions) => {
      const { pipeline, deliveryId } = subject(decision.entry);
      // Options only when there are some, so a plain send is a plain call.
      const extra = options === undefined ? [] : [options];
      const call =
        decision.kind === 'review'
          ? api.decideReview(pipeline, deliveryId, decision.body, ...extra)
          : api.decideSample(pipeline, deliveryId, decision.body, ...extra);
      call.catch((error: unknown) => {
        const { events: on, t: tr, shellNotice: notice } = latest.current;
        const conflict = error instanceof ApiError && error.status === 409;
        if (!mounted.current) {
          const title = titleOf(decision.entry).title;
          const message = errorMessage(error);
          notice({
            kind: 'error',
            message: conflict
              ? tr('inbox.lateConflict', { title, message })
              : tr('inbox.lateFailed', { title, message }),
          });
        } else if (conflict) {
          on.onConflict(decision, error);
        } else {
          on.onRestore(decision);
          on.onError(decision, error instanceof Error ? error : new Error(String(error)));
        }
      });
    },
    [api, latest],
  );

  // Releases the held decision, if any, cancelling its timer.
  const take = useCallback((): Decision | null => {
    const h = held.current;
    if (h === null) return null;
    clearTimeout(h.timer);
    held.current = null;
    return h.decision;
  }, []);

  // Sends the held decision now (its window closed, a new decision, the tab hidden).
  const flush = useCallback(
    (options?: SendOptions) => {
      const decision = take();
      setPending(null);
      if (decision) send(decision, options);
    },
    [send, take],
  );

  const decide = useCallback(
    (decision: Decision) => {
      flush();
      latest.current.events.onRemove(decision);
      held.current = { decision, timer: setTimeout(() => flush(), UNDO_MS) };
      setPending(decision);
    },
    [flush, latest],
  );

  const undo = useCallback((): Decision | null => {
    const decision = take();
    setPending(null);
    if (decision) latest.current.events.onRestore(decision);
    return decision;
  }, [latest, take]);

  // The tab closing or hidden: the page may never run again, so the held decision goes now, with keepalive.
  useEffect(() => {
    const leave = () => flush({ keepalive: true });
    const hidden = () => {
      if (document.visibilityState === 'hidden') leave();
    };
    window.addEventListener('pagehide', leave);
    document.addEventListener('visibilitychange', hidden);
    return () => {
      window.removeEventListener('pagehide', leave);
      document.removeEventListener('visibilitychange', hidden);
    };
  }, [flush]);

  // A decision still held when the screen goes away is sent, never dropped.
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      const decision = take();
      if (decision) send(decision);
    };
  }, [send, take]);

  return { pending, decide, undo };
}
