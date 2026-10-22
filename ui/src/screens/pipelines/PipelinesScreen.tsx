import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import type { Pipeline } from '../../api/schema';
import { useApi } from '../../api/context';
import { useResource } from '../../common/useResource';
import { buttons, card } from '../../common/styles';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { NoticeRegion, runAction, useNotice } from '../../common/Notice';
import { ScrollRegion } from '../../common/ScrollRegion';
import { EmptyState, ErrorState, Loading } from '../../common/States';
import { StatusBadge, StatusText, isStatus } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import { pipelinePath, runPath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { RollbackDialog } from './RollbackDialog';

// Pipelines (canvas "Pipelines"): each pipeline's state, active revision, last run and open dead letters, each a door
// to the screen behind it, with Pause or Resume, Disable and Roll back, each after a confirmation that says what
// happens. Pipelines change through their packages: nothing here applies one. Items and cost today are left out: the
// API has no such figures yet.
export function PipelinesScreen() {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const { notice, setNotice, dismiss } = useNotice();
  const [confirm, setConfirm] = useState<{ action: 'lifecycle' | 'disable' | 'rollback'; pipeline: Pipeline } | null>(
    null,
  );
  const close = () => setConfirm(null);
  const list = useResource('pipelines:list', (a) => a.listPipelines());

  const lifecycle = ({ name, state }: Pipeline) => {
    const pausing = state === 'active';
    return runAction(setNotice, {
      call: async () => {
        await (pausing ? api.pause({ pipeline: name }) : api.resume({ pipeline: name }));
        return pausing ? t('pipelines.paused', { name }) : t('pipelines.resumed', { name });
      },
      failed: (message) =>
        pausing ? t('pipelines.pauseFailed', { name, message }) : t('pipelines.resumeFailed', { name, message }),
      settled: list.refresh,
    });
  };

  const disable = ({ name }: Pipeline) =>
    runAction(setNotice, {
      call: async () => {
        await api.disable(name);
        return t('pipelines.disabled', { name });
      },
      failed: (message) => t('pipelines.disableFailed', { name, message }),
      settled: list.refresh,
    });

  const rollback = ({ name }: Pipeline, revision: number) =>
    runAction(setNotice, {
      call: async () => {
        const active = await api.rollback(name, { revision });
        return t('pipelines.rolledBack', { name, revision: active.id });
      },
      failed: (message) => t('pipelines.rollbackFailed', { name, message }),
      settled: list.refresh,
    });

  // The revisions could not be read, so the rollback dialog never opened: say so, with Retry reopening it.
  const revisionsFailed = (pipeline: Pipeline, message: string) => {
    setConfirm(null);
    setNotice({
      kind: 'error',
      message: t('pipelines.revisionsFailed', { name: pipeline.name, message }),
      retry: () => setConfirm({ action: 'rollback', pipeline }),
    });
  };

  const pipelines = list.status === 'ready' ? list.data : [];
  const running = pipelines.filter((p) => p.lastRun?.state === 'running').length;
  const dead = pipelines.reduce((n, p) => n + p.openDeadLetters, 0);

  return (
    <ScreenPage
      title={t('nav.pipelines')}
      help={
        <>
          {t('pipelines.files')} <code className="font-mono">millstream apply</code>
        </>
      }
      summary={
        list.status === 'ready' && pipelines.length > 0 ? (
          <>
            {t(f.plural(pipelines.length, 'pipelines.countOne', 'pipelines.countOther'), {
              n: f.num(pipelines.length),
            })}{' '}
            · <strong className="text-accent">{t('pipelines.running', { n: f.num(running) })}</strong> ·{' '}
            <span className={dead > 0 ? 'font-bold text-bad' : ''}>
              {t(f.plural(dead, 'pipelines.deadOne', 'pipelines.deadOther'), { n: f.num(dead) })}
            </span>
          </>
        ) : undefined
      }
    >
      {list.status === 'loading' && <Loading rows={3} className="h-14" />}
      {list.status === 'error' && (
        <ErrorState title={t('pipelines.error')} message={list.error.message} onRetry={list.reload} />
      )}
      {list.status === 'ready' && pipelines.length === 0 && (
        <EmptyState title={t('pipelines.empty.title')}>
          <p className="m-0">{t('pipelines.empty.body')}</p>
          <code className="block rounded-1 bg-surface-2 px-2.5 py-2 font-mono text-tiny break-all text-ink">
            millstream run ./pipelines/listings-daily
          </code>
        </EmptyState>
      )}
      {pipelines.length > 0 && (
        <ScrollRegion className={`${card} overflow-x-auto`} label={t('pipelines.table')}>
          <table className="w-full min-w-[760px] border-collapse text-small">
            <thead>
              <tr className="bg-surface-2 text-left text-tiny text-ink-3">
                <th scope="col" className="px-4 py-2.5 font-bold">
                  {t('pipelines.columns.pipeline')}
                </th>
                <th scope="col" className="px-2 py-2.5 font-bold">
                  {t('pipelines.columns.state')}
                </th>
                <th scope="col" className="px-2 py-2.5 font-bold">
                  {t('pipelines.columns.revision')}
                </th>
                <th scope="col" className="px-2 py-2.5 font-bold">
                  {t('pipelines.columns.lastRun')}
                </th>
                <th scope="col" className="px-2 py-2.5 text-right font-bold">
                  {t('pipelines.columns.deadLetters')}
                </th>
                <th scope="col" className="px-4 py-2.5 text-right font-bold">
                  {t('pipelines.columns.actions')}
                </th>
              </tr>
            </thead>
            <tbody>
              {pipelines.map((p) => {
                const active = p.state === 'active';
                const run = p.lastRun;
                return (
                  <tr key={p.name} className={`border-t border-line ${active ? 'text-ink' : 'text-ink-2'}`}>
                    <td className="px-4 py-3.5">
                      <Link
                        to={pipelinePath(p.name)}
                        className="font-display text-[22px] leading-[26px] text-current no-underline hover:underline"
                      >
                        {p.name}
                      </Link>
                      {p.description !== undefined && <div className="text-tiny text-ink-3">{p.description}</div>}
                    </td>
                    <td className="px-2 py-3.5">
                      <StatusBadge status={p.state} />
                      <div className="mt-1 text-tiny text-ink-3">
                        {t(f.plural(p.steps.length, 'pipelines.stepsOne', 'pipelines.stepsOther'), {
                          n: f.num(p.steps.length),
                        })}
                      </div>
                    </td>
                    <td className="px-2 py-3.5 font-mono">r{p.revision}</td>
                    <td className="px-2 py-3.5">
                      {run === undefined ? (
                        <span className="text-ink-3">{t('pipelines.noRun')}</span>
                      ) : (
                        <>
                          <Link to={runPath(p.name, run.id)} className="no-underline">
                            {isStatus(run.state) && <StatusText status={run.state} />}
                          </Link>
                          <div className="text-tiny text-ink-3" title={f.dateTime(run.startedAt)}>
                            {f.relative(run.startedAt)}
                            {run.settledAt !== undefined &&
                              ` · ${f.duration(Date.parse(run.settledAt) - Date.parse(run.startedAt))}`}
                          </div>
                        </>
                      )}
                    </td>
                    <td className="px-2 py-3.5 text-right font-mono">
                      <Link
                        to={pipelinePath(p.name, 'dead-letters')}
                        className={p.openDeadLetters > 0 ? 'font-bold text-bad' : 'text-ink-3'}
                        aria-label={t(f.plural(p.openDeadLetters, 'pipelines.deadOne', 'pipelines.deadOther'), {
                          n: f.num(p.openDeadLetters),
                        })}
                      >
                        {f.num(p.openDeadLetters)}
                      </Link>
                    </td>
                    <td className="px-4 py-3.5">
                      <div className="flex flex-wrap justify-end gap-1.5">
                        <button
                          type="button"
                          className={buttons.smallSecondary}
                          aria-label={
                            active
                              ? t('pipelines.pauseName', { name: p.name })
                              : t('pipelines.resumeName', { name: p.name })
                          }
                          onClick={() => setConfirm({ action: 'lifecycle', pipeline: p })}
                        >
                          {active ? t('pipelines.pause') : t('pipelines.resume')}
                        </button>
                        {p.state !== 'disabled' && (
                          <button
                            type="button"
                            className={buttons.smallSecondary}
                            aria-label={t('pipelines.disableName', { name: p.name })}
                            onClick={() => setConfirm({ action: 'disable', pipeline: p })}
                          >
                            {t('pipelines.disable')}
                          </button>
                        )}
                        <button
                          type="button"
                          className={buttons.smallSecondary}
                          aria-label={t('pipelines.rollbackName', { name: p.name })}
                          onClick={() => setConfirm({ action: 'rollback', pipeline: p })}
                        >
                          {t('pipelines.rollback')}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </ScrollRegion>
      )}

      {confirm?.action === 'lifecycle' && (
        <LifecycleDialog
          pipeline={confirm.pipeline}
          onConfirm={() => lifecycle(confirm.pipeline).finally(close)}
          onCancel={close}
        />
      )}
      {confirm?.action === 'disable' && (
        <ConfirmDialog
          title={t('pipelines.disableTitle', { name: confirm.pipeline.name })}
          confirmLabel={t('pipelines.disableConfirm', { name: confirm.pipeline.name })}
          destructive
          onConfirm={() => disable(confirm.pipeline).finally(close)}
          onCancel={close}
        >
          <Consequences>
            <li>{t('pipelines.disableTriggers')}</li>
            <li>{t('pipelines.disableWait')}</li>
            <li>{t('pipelines.disableRunning')}</li>
            <li>{t('pipelines.disableUndo')}</li>
          </Consequences>
        </ConfirmDialog>
      )}
      {confirm?.action === 'rollback' && (
        <RollbackDialog
          pipeline={confirm.pipeline}
          onConfirm={(revision) => rollback(confirm.pipeline, revision).finally(close)}
          onCancel={close}
          onError={(message) => revisionsFailed(confirm.pipeline, message)}
        />
      )}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}

// Pause an active pipeline, or resume a paused or disabled one (resume enables it too).
function LifecycleDialog({
  pipeline: { name, state },
  onConfirm,
  onCancel,
}: {
  pipeline: Pipeline;
  onConfirm: () => Promise<unknown>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const pausing = state === 'active';
  return (
    <ConfirmDialog
      title={pausing ? t('pipelines.pauseTitle', { name }) : t('pipelines.resumeTitle', { name })}
      confirmLabel={pausing ? t('pipelines.pauseConfirm', { name }) : t('pipelines.resumeConfirm', { name })}
      onConfirm={onConfirm}
      onCancel={onCancel}
    >
      {pausing ? (
        <Consequences>
          <li>{t('pipelines.pauseWait')}</li>
          <li>{t('pipelines.pauseRunning')}</li>
          <li>{t('pipelines.pauseTriggers')}</li>
        </Consequences>
      ) : (
        <Consequences>
          {state === 'disabled' && <li>{t('pipelines.resumeTriggers')}</li>}
          <li>{t('pipelines.resumeStart')}</li>
          <li>{t('pipelines.resumeBudget')}</li>
        </Consequences>
      )}
    </ConfirmDialog>
  );
}
