import { useEffect, useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Pipeline, Revision } from '../../api/schema';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { useFormat } from '../../common/useFormat';
import { useResource } from '../../common/useResource';
import { useLatest } from '../../useLatest';

// The package hash without its algorithm, short enough for a row: 9f2c41d07a.
const shortHash = (hash: string) => hash.replace(/^sha256:/, '').slice(0, 12);

// The revision a rollback picks by default, as the server does: the newest one older than the active one. Else the
// newest other one (the active one is an old revision already rolled back to).
function defaultRevision(others: Revision[], active: number): Revision | undefined {
  return others.find((r) => r.id < active) ?? others[0];
}

// Roll back: pick one of the pipeline's other revisions, newest first, the previous one by default, and confirm
// after the consequence. It opens once the revisions are read, so focus lands on the confirm button; a failed read
// is reported by the caller instead.
export function RollbackDialog({
  pipeline,
  onConfirm,
  onCancel,
  onError,
}: {
  pipeline: Pipeline;
  onConfirm: (revision: number) => Promise<unknown>;
  onCancel: () => void;
  onError: (message: string) => void;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const group = useId();
  const { name } = pipeline;
  const revisions = useResource(`revisions:${name}`, (api) => api.listRevisions(name));
  const [picked, setPicked] = useState<number | null>(null);

  const failed = revisions.status === 'error' ? revisions.error.message : null;
  const reportError = useLatest(onError);
  useEffect(() => {
    if (failed !== null) reportError.current(failed);
  }, [failed, reportError]);
  if (revisions.status !== 'ready') return null;

  const listed = [...revisions.data].sort((a, b) => b.id - a.id);
  const active = listed.find((r) => r.active)?.id ?? pipeline.revision;
  const others = listed.filter((r) => r.id !== active);
  const chosen = others.find((r) => r.id === picked) ?? defaultRevision(others, active);

  return (
    <ConfirmDialog
      title={t('pipelines.rollbackTitle', { name })}
      confirmLabel={
        chosen === undefined ? t('pipelines.rollback') : t('pipelines.rollbackConfirm', { revision: chosen.id })
      }
      confirmDisabled={chosen === undefined}
      onConfirm={() => (chosen === undefined ? Promise.resolve() : onConfirm(chosen.id))}
      onCancel={onCancel}
    >
      {chosen === undefined ? (
        <p className="m-0">{t('pipelines.rollbackNone', { name, revision: active })}</p>
      ) : (
        <>
          <fieldset className="m-0 flex min-w-0 flex-col gap-1.5 border-0 p-0">
            <legend className="mb-1.5 p-0 text-tiny font-bold text-ink-2">
              {t('pipelines.rollbackPick', { revision: active })}
            </legend>
            {others.map((r) => (
              <label
                key={r.id}
                className="flex cursor-pointer items-start gap-2.5 rounded-2 border border-line px-3 py-2 has-checked:border-accent"
              >
                <input
                  type="radio"
                  name={group}
                  value={r.id}
                  checked={r.id === chosen.id}
                  onChange={() => setPicked(r.id)}
                  className="mt-1 size-4 flex-none accent-accent"
                />
                <span className="flex min-w-0 flex-col">
                  <span className="text-small text-ink">
                    <strong className="font-mono">r{r.id}</strong>
                    {' · '}
                    {t('pipelines.rollbackApplied', { when: f.dateTime(r.appliedAt) })}
                    {' · '}
                    <span className="font-mono">{shortHash(r.packageHash)}</span>
                  </span>
                  {r.message !== undefined && <span className="text-tiny text-ink-2">{r.message}</span>}
                </span>
              </label>
            ))}
          </fieldset>
          <Consequences>
            <li>{t('pipelines.rollbackNew', { revision: chosen.id })}</li>
            <li>{t('pipelines.rollbackInFlight', { revision: active })}</li>
            <li>{t('pipelines.rollbackNoRevision', { revision: active })}</li>
          </Consequences>
        </>
      )}
    </ConfirmDialog>
  );
}
