import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import type { RunQuery } from '../../api/api';
import { useApi } from '../../api/context';
import type { Run, RunPage, RunState } from '../../api/schema';
import { formatDuration, formatMoney } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { runPath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { Segmented } from '../../common/Segmented';
import { NoticeRegion, runAction, useNotice } from '../../common/Notice';
import { StatusBadge, useStatusLabel } from '../../common/Status';
import { EmptyState, ErrorState, QueryView } from '../../common/States';
import { requestId } from '../../common/requestId';
import { buttons, card } from '../../common/styles';
import { useNow } from '../../common/useNow';
import { appendPage, useLoadMore } from '../../common/useLoadMore';
import { useResource } from '../../common/useResource';
import { useRunName } from './runParts';

const PAGE = 50;
type Filter = RunState | 'all';
const filters: Filter[] = ['all', 'running', 'waitingReview', 'succeeded', 'completedWithErrors', 'cancelled'];

const barTone: Record<RunState, string> = {
  running: 'bg-accent',
  waitingReview: 'bg-human',
  succeeded: 'bg-accent',
  completedWithErrors: 'bg-warn',
  cancelled: 'bg-mute',
};

// A pipeline's runs, newest first, as run bars (canvas components board): when and state, the duration as a bar
// against the longest run shown, duration and cost.
export function RunsScreen() {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const { pipeline = '' } = useParams();
  const [search, setSearch] = useSearchParams();
  const state = (filters.find((f) => f === search.get('state')) ?? 'all') as Filter;
  const query: RunQuery = state === 'all' ? { limit: PAGE } : { state, limit: PAGE };
  const key = `runs:${pipeline}:${state}`;
  const runs = useResource(key, (api) => api.listRuns(pipeline, query));
  const api = useApi();
  // The next page by its cursor, appended without the runs an earlier page showed (a run that changed state while
  // paging); one that arrives after the filter changed is dropped.
  const { loadMore, loadingMore, moreError } = useLoadMore<RunPage, string>({
    key,
    mutate: runs.mutate,
    read: (cursor) => api.listRuns(pipeline, { ...query, cursor }),
    merge: appendPage,
  });
  const now = useNow();
  const name = useRunName();
  const statusLabel = useStatusLabel();
  const label = (filter: Filter) => (filter === 'all' ? t('runs.filter.all') : statusLabel(filter));

  // Cron and manual triggers can be fired by hand (spec section 4.6); a webhook is fired by its caller's token, an
  // insight trigger by the insights it takes.
  const triggers = useResource(`pipeline:${pipeline}`, (a) => a.getPipeline(pipeline));
  const fireable =
    triggers.status === 'ready'
      ? triggers.data.triggers.filter((tr) => tr.kind === 'cron' || tr.kind === 'manual')
      : [];
  const [firing, setFiring] = useState(false);
  const { notice, setNotice, dismiss } = useNotice();
  // Each click is its own request id: a retry of the same click (Retry, or a resent request) returns the same run
  // instead of firing twice; a new click fires again.
  const fire = (trigger: string) => {
    const id = requestId();
    setFiring(true);
    void runAction(setNotice, {
      call: async () => t('runs.fired', { name: name(await api.triggerRun(pipeline, { trigger, requestId: id })) }),
      failed: (message) => t('runs.fireFailed', { trigger, message }),
      settled: () => {
        setFiring(false);
        runs.refresh();
      },
    });
  };

  const setState = (value: Filter) => setSearch(value === 'all' ? {} : { state: value });
  const duration = (run: Run) => (run.settledAt ? Date.parse(run.settledAt) : now) - Date.parse(run.startedAt);

  return (
    <ScreenPage
      title={t('nav.runs')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{pipeline}</p>}
      header={
        fireable.length > 0 && (
          <div className="flex flex-wrap gap-2 md:ml-auto">
            {fireable.map((tr) => (
              <button
                key={tr.name}
                type="button"
                disabled={firing}
                onClick={() => fire(tr.name)}
                className={buttons.primary}
              >
                {t('runs.fire', { trigger: tr.name })}
              </button>
            ))}
          </div>
        )
      }
    >
      <Segmented
        label={t('runs.filter.label')}
        options={filters.map((value) => ({ value, label: label(value) }))}
        value={state}
        onChange={setState}
      />
      <QueryView query={runs}>
        {(page) => {
          if (page.items.length === 0) {
            return (
              <EmptyState title={state === 'all' ? t('runs.empty') : t('runs.emptyFiltered')}>
                {state === 'all' && t('runs.emptyBody')}
              </EmptyState>
            );
          }
          const longest = Math.max(...page.items.map(duration));
          const { next } = page;
          return (
            <div className={`${card} flex flex-col px-4 py-2 md:px-7`}>
              <ul className="m-0 list-none p-0" aria-label={t('runs.list')}>
                {page.items.map((run) => (
                  <li key={run.id} className="border-b border-line last:border-b-0">
                    <Link
                      to={runPath(pipeline, run.id)}
                      className="grid grid-cols-1 gap-1.5 py-2.5 text-ink no-underline md:grid-cols-[minmax(0,220px)_minmax(0,1fr)_minmax(0,auto)] md:items-center md:gap-3.5"
                    >
                      <span className="flex flex-col gap-0.5">
                        <strong className="text-small">
                          {name(run)}
                          {run.kind === 'replay' && (
                            <span className="font-normal text-ink-3"> · {t('runs.replay')}</span>
                          )}
                          {run.kind === 'import' && (
                            <span className="font-normal text-ink-3">
                              {' · '}
                              {t('runs.importFrom', { pipeline: run.producer ?? '' })}
                            </span>
                          )}
                        </strong>
                        <span>
                          <StatusBadge status={run.state} />
                        </span>
                      </span>
                      <span aria-hidden="true" className="flex h-2.5 bg-surface-2">
                        <span
                          className={`${barTone[run.state]} ${run.settledAt ? '' : 'opacity-80'}`}
                          style={{ width: `${Math.max(2, (duration(run) / Math.max(1, longest)) * 100)}%` }}
                        />
                      </span>
                      <span className="font-mono text-tiny text-ink-2 md:text-right">
                        {formatDuration(duration(run), language)} · {formatMoney(run.costUsd, language)}
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
              {next !== undefined && (
                <div className="flex flex-col gap-2 py-2.5">
                  {moreError !== null && <ErrorState message={moreError} />}
                  <button
                    type="button"
                    disabled={loadingMore}
                    onClick={() => void loadMore(next)}
                    className={`${buttons.secondary} self-start`}
                  >
                    {t('runs.more')}
                  </button>
                </div>
              )}
            </div>
          );
        }}
      </QueryView>
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}
