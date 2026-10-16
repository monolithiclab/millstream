import { useCallback, useEffect, useRef, useState } from 'react';
import { ApiError } from '../../api/api';
import { useApi } from '../../api/context';
import type { ReviewDecision, SampleDecision } from '../../api/schema';
import { useLatest } from '../../useLatest';
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

export function useDecisions(events: DecisionEvents) {
  const api = useApi();
  const latest = useLatest(events);
  const held = useRef<{ decision: Decision; timer: ReturnType<typeof setTimeout> } | null>(null);
  const [pending, setPending] = useState<Decision | null>(null);

  const send = useCallback(
    (decision: Decision) => {
      const { pipeline, deliveryId } = subject(decision.entry);
      const call =
        decision.kind === 'review'
          ? api.decideReview(pipeline, deliveryId, decision.body)
          : api.decideSample(pipeline, deliveryId, decision.body);
      call.catch((error: unknown) => {
        if (error instanceof ApiError && error.status === 409) {
          latest.current.onConflict(decision, error);
          return;
        }
        latest.current.onRestore(decision);
        latest.current.onError(decision, error instanceof Error ? error : new Error(String(error)));
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

  // Sends the held decision now (its window closed, a new decision, leaving the screen).
  const flush = useCallback(() => {
    const decision = take();
    setPending(null);
    if (decision) send(decision);
  }, [send, take]);

  const decide = useCallback(
    (decision: Decision) => {
      flush();
      latest.current.onRemove(decision);
      held.current = { decision, timer: setTimeout(flush, UNDO_MS) };
      setPending(decision);
    },
    [flush, latest],
  );

  const undo = useCallback((): Decision | null => {
    const decision = take();
    setPending(null);
    if (decision) latest.current.onRestore(decision);
    return decision;
  }, [latest, take]);

  // A decision still held when the screen goes away is sent, never dropped.
  useEffect(
    () => () => {
      const decision = take();
      if (decision) send(decision);
    },
    [send, take],
  );

  return { pending, decide, undo };
}
