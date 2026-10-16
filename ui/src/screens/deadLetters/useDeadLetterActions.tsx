import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { DeadLetterSelection } from '../../api/schema';
import { useApi } from '../../api/context';
import { useResource } from '../../common/useResource';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { runAction, type Notice } from '../../common/Notice';
import { useFormat } from '../../common/useFormat';

export type DeadLetterAction = 'replay' | 'discard';

// What an action applies to: the selection the server reads, how many open dead letters it names, the step when
// they share one, and their runs (replay checks whether a run is on an older revision, for migrate).
export interface DeadLetterTarget {
  selection: DeadLetterSelection;
  count: number;
  step?: string | undefined;
  runs: string[];
}

// The target of some open dead letters: the server's selection (their ids unless given), how many, their step when
// they share one, and their runs.
export function targetOf(
  letters: { id: string; runId: string; step: string }[],
  selection: DeadLetterSelection = { ids: letters.map((l) => l.id) },
): DeadLetterTarget {
  const steps = new Set(letters.map((l) => l.step));
  return {
    selection,
    count: letters.length,
    step: steps.size === 1 ? [...steps][0] : undefined,
    runs: letters.map((l) => l.runId),
  };
}

interface Pending {
  action: DeadLetterAction;
  target: DeadLetterTarget;
}

function MigrateOption({
  pipeline,
  runs,
  revision,
  checked,
  onChange,
}: {
  pipeline: string;
  runs: string[];
  revision: number;
  checked: boolean;
  onChange: (on: boolean) => void;
}) {
  const { t } = useTranslation();
  const id = useId();
  // A run's revision is on the run: read the few distinct runs a selection spans.
  const distinct = [...new Set(runs)].slice(0, 10);
  const read = useResource(`runs:${pipeline}:${distinct.join(',')}`, (api) =>
    Promise.all(distinct.map((run) => api.getRun(pipeline, run))),
  );
  if (read.status !== 'ready') return null;
  const older = [...new Set(read.data.map((r) => r.revision).filter((r) => r !== revision))].sort((a, b) => a - b);
  if (older.length === 0) return null;
  return (
    <div className="flex items-start gap-2.5 rounded-1 border border-line px-3 py-2.5 text-ink">
      <input
        id={id}
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        aria-describedby={`${id}-hint`}
        className="mt-1 size-4 flex-none accent-[var(--ms-accent)]"
      />
      <span className="flex flex-col">
        <label htmlFor={id} className="font-bold">
          {t('deadLetters.migrate', { revision: `r${revision}` })}
        </label>
        <span id={`${id}-hint`} className="text-small text-ink-2">
          {t('deadLetters.migrateHint', { revisions: older.map((r) => `r${r}`).join(', ') })}
        </span>
      </span>
    </div>
  );
}

// Replay and discard with their confirmations (interaction notes: the body states the consequence, the button repeats
// the verb and the count; discard opens on Cancel), then a notice with the outcome. onDone reloads what shows them.
export function useDeadLetterActions({
  pipeline,
  revision,
  onDone,
  setNotice,
}: {
  pipeline: string;
  revision: number | undefined;
  onDone: () => void;
  // The screen's notice region, shared with its other actions.
  setNotice: (notice: Notice | null) => void;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const dismiss = () => setNotice(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [migrate, setMigrate] = useState(false);

  const run = ({ action, target }: Pending, withMigrate: boolean) => {
    const n = f.num(target.count);
    return runAction(setNotice, {
      call: async () => {
        const { count } =
          action === 'replay'
            ? await api.replayDeadLetters(pipeline, { ...target.selection, ...(withMigrate ? { migrate: true } : {}) })
            : await api.discardDeadLetters(pipeline, target.selection);
        const done = f.num(count);
        return action === 'replay'
          ? t(f.plural(count, 'deadLetters.replayedOne', 'deadLetters.replayedOther'), { n: done })
          : t(f.plural(count, 'deadLetters.discardedOne', 'deadLetters.discardedOther'), { n: done });
      },
      failed: (message) =>
        action === 'replay'
          ? t('deadLetters.replayFailed', { n, message })
          : t('deadLetters.discardFailed', { n, message }),
      settled: onDone,
    });
  };

  const request = (action: DeadLetterAction, target: DeadLetterTarget) => {
    dismiss();
    setMigrate(false);
    setPending({ action, target });
  };

  let dialog = null;
  if (pending !== null) {
    const { action, target } = pending;
    const n = f.num(target.count);
    const many = target.count !== 1;
    const close = () => setPending(null);
    const confirm = async () => {
      await run(pending, migrate);
      close();
    };
    dialog =
      action === 'replay' ? (
        <ConfirmDialog
          title={
            target.step === undefined
              ? t(f.plural(target.count, 'deadLetters.replayTitleOne', 'deadLetters.replayTitleOther'), { n })
              : t(f.plural(target.count, 'deadLetters.replayTitleStepOne', 'deadLetters.replayTitleStepOther'), {
                  n,
                  step: target.step,
                })
          }
          confirmLabel={t(f.plural(target.count, 'deadLetters.replayConfirmOne', 'deadLetters.replayConfirmOther'), {
            n,
          })}
          onConfirm={confirm}
          onCancel={close}
        >
          <Consequences>
            <li>
              {target.step === undefined
                ? t('deadLetters.replayQueueSteps')
                : t('deadLetters.replayQueue', { step: target.step })}
            </li>
            <li>
              {migrate && revision !== undefined
                ? t('deadLetters.replayMigrated', { revision: `r${revision}` })
                : t('deadLetters.replayRevision')}
            </li>
            <li>{t('deadLetters.replayHistory')}</li>
            <li>{many ? t('deadLetters.replayAgainOther') : t('deadLetters.replayAgainOne')}</li>
          </Consequences>
          {revision !== undefined && (
            <MigrateOption
              pipeline={pipeline}
              runs={target.runs}
              revision={revision}
              checked={migrate}
              onChange={setMigrate}
            />
          )}
        </ConfirmDialog>
      ) : (
        <ConfirmDialog
          destructive
          title={t(f.plural(target.count, 'deadLetters.discardTitleOne', 'deadLetters.discardTitleOther'), { n })}
          confirmLabel={t(f.plural(target.count, 'deadLetters.discardConfirmOne', 'deadLetters.discardConfirmOther'), {
            n,
          })}
          onConfirm={confirm}
          onCancel={close}
        >
          <Consequences>
            <li>{t('deadLetters.discardFinal')}</li>
            <li>{t('deadLetters.discardRuns')}</li>
            <li>{t('deadLetters.discardHistory')}</li>
          </Consequences>
        </ConfirmDialog>
      );
  }

  return { request, dialog };
}
