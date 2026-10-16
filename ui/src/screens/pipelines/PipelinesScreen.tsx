import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import type { Pipeline } from '../../api/schema';
import { useApi, useApiQuery } from '../../api/context';
import { buttons, card } from '../../common/buttons';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { NoticeRegion, runAction, useNotice } from '../../common/Notice';
import { ScrollRegion } from '../../common/ScrollRegion';
import { EmptyState, ErrorState, Skeleton } from '../../common/States';
import { StatusBadge, StatusText, isStatus } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import { pipelinePath } from '../../routes';
import { ScreenPage } from '../ScreenPage';

// Pipelines (canvas "Pipelines"): each pipeline's state, active revision, last run and open dead letters, each a door
// to the screen behind it, with Pause or Resume after a confirmation that says what happens. Items and cost today
// are left out: the API has no such figures yet.
export function PipelinesScreen() {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const { notice, setNotice, dismiss } = useNotice();
  const [confirm, setConfirm] = useState<Pipeline | null>(null);
  const list = useApiQuery('pipelines:list', (a) => a.listPipelines());

  const lifecycle = ({ name, state }: Pipeline) => {
    const pausing = state === 'active';
    return runAction(setNotice, {
      call: async () => {
        await (pausing ? api.pause({ pipeline: name }) : api.resume({ pipeline: name }));
        return pausing ? t('pipelines.paused', { name }) : t('pipelines.resumed', { name });
      },
      failed: (message) =>
        pausing ? t('pipelines.pauseFailed', { name, message }) : t('pipelines.resumeFailed', { name, message }),
      settled: list.reload,
    });
  };

  const pipelines = list.status === 'ready' ? list.data : [];
  const running = pipelines.filter((p) => p.lastRun?.state === 'running').length;
  const dead = pipelines.reduce((n, p) => n + p.openDeadLetters, 0);

  return (
    <ScreenPage title={t('nav.pipelines')}>
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 text-small text-ink-2">
        {list.status === 'ready' && pipelines.length > 0 && (
          <span>
            {t(f.plural(pipelines.length, 'pipelines.countOne', 'pipelines.countOther'), {
              n: f.num(pipelines.length),
            })}{' '}
            · <strong className="text-accent">{t('pipelines.running', { n: f.num(running) })}</strong> ·{' '}
            <span className={dead > 0 ? 'font-bold text-bad' : ''}>
              {t(f.plural(dead, 'pipelines.deadOne', 'pipelines.deadOther'), { n: f.num(dead) })}
            </span>
          </span>
        )}
        <span className="text-tiny text-ink-3 md:ml-auto">
          {t('pipelines.files')} <code className="font-mono">millstream apply</code>
        </span>
      </div>

      {list.status === 'loading' && <Skeleton rows={3} className="h-14" />}
      {list.status === 'error' && <ErrorState title={t('pipelines.error')} error={list.error} onRetry={list.reload} />}
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
                const paused = p.state === 'paused';
                const run = p.lastRun;
                return (
                  <tr key={p.name} className={`border-t border-line ${paused ? 'text-ink-2' : 'text-ink'}`}>
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
                      <StatusBadge status={paused ? 'paused' : 'active'} />
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
                          <Link
                            to={`${pipelinePath(p.name, 'runs')}?run=${encodeURIComponent(run.id)}`}
                            className="no-underline"
                          >
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
                    <td className="px-4 py-3.5 text-right">
                      <button
                        type="button"
                        className={buttons.smallSecondary}
                        aria-label={
                          paused
                            ? t('pipelines.resumeName', { name: p.name })
                            : t('pipelines.pauseName', { name: p.name })
                        }
                        onClick={() => setConfirm(p)}
                      >
                        {paused ? t('pipelines.resume') : t('pipelines.pause')}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </ScrollRegion>
      )}

      {confirm !== null && (
        <ConfirmDialog
          title={
            confirm.state === 'active'
              ? t('pipelines.pauseTitle', { name: confirm.name })
              : t('pipelines.resumeTitle', { name: confirm.name })
          }
          confirmLabel={
            confirm.state === 'active'
              ? t('pipelines.pauseConfirm', { name: confirm.name })
              : t('pipelines.resumeConfirm', { name: confirm.name })
          }
          onConfirm={() => lifecycle(confirm).finally(() => setConfirm(null))}
          onCancel={() => setConfirm(null)}
        >
          {confirm.state === 'active' ? (
            <Consequences>
              <li>{t('pipelines.pauseWait')}</li>
              <li>{t('pipelines.pauseRunning')}</li>
              <li>{t('pipelines.pauseTriggers')}</li>
            </Consequences>
          ) : (
            <Consequences>
              <li>{t('pipelines.resumeStart')}</li>
              <li>{t('pipelines.resumeBudget')}</li>
            </Consequences>
          )}
        </ConfirmDialog>
      )}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}
