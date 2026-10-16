import { useEffect, useRef, useState, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import type { DeadLetter, DeadLetterGroup } from '../../api/schema';
import { buttons, card } from '../../common/styles';
import { ErrorText } from '../../common/Copy';
import { NoticeRegion, useNotice } from '../../common/Notice';
import { EmptyState, ErrorState, Loading } from '../../common/States';
import { useResource } from '../../common/useResource';
import { SH, StatusBadge, StatusGlyph } from '../../common/Status';
import { ScrollRegion } from '../../common/ScrollRegion';
import { Segmented } from '../../common/Segmented';
import { screenKeyAllowed } from '../../shell/useShortcuts';
import { shortId } from '../../common/Id';
import { useFormat } from '../../common/useFormat';
import { usePreferences } from '../../preferences';
import { itemPath, pipelinePath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { targetOf, useDeadLetterActions, type DeadLetterAction } from './useDeadLetterActions';

type State = DeadLetter['state'];
const states: State[] = ['open', 'replayed', 'discarded'];

// The groups of one state: the server groups every state together (state=all), so each group keeps the dead letters
// in that state, with its count and its first and last seen recomputed; empty groups go.
export function groupsIn(groups: DeadLetterGroup[], state: State): DeadLetterGroup[] {
  return groups.flatMap((g) => {
    const deadLetters = g.deadLetters.filter((d) => d.state === state);
    if (deadLetters.length === 0) return [];
    const times = deadLetters.map((d) => d.createdAt).sort();
    return [
      {
        ...g,
        deadLetters,
        count: deadLetters.length,
        firstSeen: times[0] ?? g.firstSeen,
        lastSeen: times.at(-1) ?? g.lastSeen,
      },
    ];
  });
}

// The headline of a group: the first line of its latest error, shortened; the full text is below it.
export function headline(error: string): string {
  const line = error.split('\n')[0] ?? '';
  return line.length > 140 ? `${line.slice(0, 139)}…` : line;
}

const total = (groups: DeadLetterGroup[]) => groups.reduce((n, g) => n + g.count, 0);

type Letter = DeadLetter & { step: string };

// Dead letters (canvas "Dead letters"): grouped by error fingerprint, largest first, each group with its count, step,
// first and last seen and its latest error; replay or discard one, a selection, a group or all, each after a
// confirmation that states the consequence. Lists and tables keys: j and k move between rows, x selects, ⇧R and ⇧D
// replay and discard the selection.
export function DeadLettersScreen() {
  const { pipeline = '' } = useParams();
  const { t } = useTranslation();
  const f = useFormat();
  const { singleKeys } = usePreferences();
  const [params, setParams] = useSearchParams();
  const state: State = states.find((s) => s === params.get('state')) ?? 'open';
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
  const { notice, setNotice, dismiss } = useNotice();
  const list = useRef<HTMLDivElement>(null);

  const data = useResource(`deadLetters:${pipeline}`, async (api) => {
    const [p, groups] = await Promise.all([api.getPipeline(pipeline), api.listDeadLetters(pipeline, { state: 'all' })]);
    return { pipeline: p, groups };
  });
  const ready = data.status === 'ready' ? data.data : null;
  const all = ready?.groups ?? [];
  const byState = Object.fromEntries(states.map((s) => [s, groupsIn(all, s)])) as Record<State, DeadLetterGroup[]>;
  const groups = byState[state];
  const openLetters: Letter[] = byState.open.flatMap((g) => g.deadLetters.map((d) => ({ ...d, step: g.step })));

  const actions = useDeadLetterActions({
    pipeline,
    revision: ready?.pipeline.revision,
    onDone: data.refresh,
    setNotice,
  });

  // The selection counts only dead letters still open: a replayed one leaves it by itself.
  const chosen = openLetters.filter((d) => selected.has(d.id));
  const requestSelection = (action: DeadLetterAction) => {
    if (chosen.length > 0) actions.request(action, targetOf(chosen));
  };
  const requestAll = (action: DeadLetterAction) => actions.request(action, targetOf(openLetters, { all: true }));

  const toggle = (ids: string[], on: boolean) => {
    const next = new Set(selected);
    for (const id of ids) {
      if (on) next.add(id);
      else next.delete(id);
    }
    setSelected(next);
  };

  const onKeyDown = (event: KeyboardEvent) => {
    // A row's checkbox is where they work.
    if (!singleKeys || state !== 'open' || !screenKeyAllowed(event.nativeEvent)) return;
    const target = event.target as HTMLElement;
    const boxes = [...(list.current?.querySelectorAll<HTMLInputElement>('input[data-row]') ?? [])];
    const index = boxes.indexOf(target as HTMLInputElement);
    if (event.key === 'j' || event.key === 'k') {
      const step = event.key === 'j' ? 1 : -1;
      boxes[index === -1 ? 0 : Math.min(boxes.length - 1, Math.max(0, index + step))]?.focus();
      event.preventDefault();
    } else if (event.key === 'x' && index !== -1) {
      boxes[index]?.click();
      event.preventDefault();
    } else if (event.key === 'R' && event.shiftKey) {
      requestSelection('replay');
      event.preventDefault();
    } else if (event.key === 'D' && event.shiftKey) {
      requestSelection('discard');
      event.preventDefault();
    }
  };

  const count = (s: State) => f.num(total(byState[s]));
  const stateLabels: Record<State, string> =
    ready === null
      ? {
          open: t('deadLetters.stateName.open'),
          replayed: t('deadLetters.stateName.replayed'),
          discarded: t('deadLetters.stateName.discarded'),
        }
      : {
          open: t('deadLetters.state.open', { n: count('open') }),
          replayed: t('deadLetters.state.replayed', { n: count('replayed') }),
          discarded: t('deadLetters.state.discarded', { n: count('discarded') }),
        };
  const shown = total(groups);
  const kinds = { dead: 0, unrouted: 0 };
  for (const g of groups) kinds[g.kind] += g.count;

  return (
    <ScreenPage
      title={t('nav.deadLetters')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{t('screen.inPipeline', { pipeline })}</p>}
    >
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
        <Segmented
          label={t('deadLetters.stateFilter')}
          value={state}
          onChange={(s) => {
            setSelected(new Set());
            setParams(s === 'open' ? {} : { state: s }, { replace: true });
          }}
          options={states.map((s) => ({ value: s, label: stateLabels[s] }))}
        />
        {state === 'open' && openLetters.length > 0 && (
          <div className="flex flex-wrap gap-2 md:ml-auto">
            {chosen.length > 0 ? (
              <>
                <button type="button" className={buttons.primary} onClick={() => requestSelection('replay')}>
                  {t('deadLetters.replaySelection', { n: f.num(chosen.length) })}
                </button>
                <button type="button" className={buttons.secondary} onClick={() => requestSelection('discard')}>
                  {t('deadLetters.discardSelection', { n: f.num(chosen.length) })}
                </button>
                <button type="button" className={buttons.link} onClick={() => setSelected(new Set())}>
                  {t('deadLetters.clearSelection')}
                </button>
              </>
            ) : (
              <>
                <button type="button" className={buttons.secondary} onClick={() => requestAll('replay')}>
                  {t('deadLetters.replayAll', { n: f.num(openLetters.length) })}
                </button>
                <button type="button" className={buttons.secondary} onClick={() => requestAll('discard')}>
                  {t('deadLetters.discardAll', { n: f.num(openLetters.length) })}
                </button>
              </>
            )}
          </div>
        )}
      </div>

      {data.status === 'loading' && <Loading rows={2} className="h-48" />}
      {data.status === 'error' && (
        <ErrorState title={t('deadLetters.error')} message={data.error.message} onRetry={data.reload} />
      )}
      {ready !== null && groups.length === 0 && (
        <EmptyState
          title={state === 'open' ? t('deadLetters.empty.open') : t('deadLetters.empty.other')}
          done={state === 'open'}
        >
          <p className="m-0">{state === 'open' ? t('deadLetters.empty.openBody') : t('deadLetters.empty.otherBody')}</p>
        </EmptyState>
      )}
      {ready !== null && groups.length > 0 && (
        // The list takes the lists-and-tables keys of its rows.
        // eslint-disable-next-line jsx-a11y/no-static-element-interactions
        <div ref={list} onKeyDown={onKeyDown} className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-small text-ink-2">
            <span>
              {t('deadLetters.summary', {
                groups: t(f.plural(groups.length, 'deadLetters.groupsOne', 'deadLetters.groupsOther'), {
                  n: f.num(groups.length),
                }),
                items: t(f.plural(shown, 'deadLetters.itemsOne', 'deadLetters.itemsOther'), { n: f.num(shown) }),
              })}
            </span>
            <span className="inline-flex items-center gap-1.5 md:ml-auto">
              <StatusGlyph status="dead" size={11} />
              {t('deadLetters.kindDead', { n: f.num(kinds.dead) })}
            </span>
            <span className="inline-flex items-center gap-1.5">
              <StatusGlyph status="unrouted" size={11} />
              {t('deadLetters.kindUnrouted', { n: f.num(kinds.unrouted) })}
            </span>
            {state === 'open' && <span className="sr-only">{t('deadLetters.keys')}</span>}
          </div>
          {groups.map((g, index) => (
            <Group
              key={g.fingerprint}
              pipeline={pipeline}
              group={g}
              actionable={state === 'open'}
              primary={index === 0}
              selected={selected}
              onToggle={toggle}
              onAction={(action, letters) =>
                actions.request(
                  action,
                  letters === 'group'
                    ? targetOf(
                        g.deadLetters.map((d) => ({ ...d, step: g.step })),
                        { fingerprint: g.fingerprint },
                      )
                    : targetOf(letters.map((d) => ({ ...d, step: g.step }))),
                )
              }
            />
          ))}
        </div>
      )}
      {actions.dialog}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}

function Group({
  pipeline,
  group,
  actionable,
  primary,
  selected,
  onToggle,
  onAction,
}: {
  pipeline: string;
  group: DeadLetterGroup;
  actionable: boolean;
  primary: boolean;
  selected: ReadonlySet<string>;
  onToggle: (ids: string[], on: boolean) => void;
  onAction: (action: DeadLetterAction, letters: DeadLetter[] | 'group') => void;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const box = useRef<HTMLInputElement>(null);
  const ids = group.deadLetters.map((d) => d.id);
  const chosen = ids.filter((id) => selected.has(id)).length;
  const latest = [...group.deadLetters].sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
  const error = latest?.error ?? group.error;
  const title = headline(error);
  const n = f.num(group.count);
  const kindLabel =
    group.kind === 'dead'
      ? t(f.plural(group.count, 'deadLetters.deadItemsOne', 'deadLetters.deadItemsOther'), { n })
      : t(f.plural(group.count, 'deadLetters.unroutedItemsOne', 'deadLetters.unroutedItemsOther'), { n });

  useEffect(() => {
    if (box.current) box.current.indeterminate = chosen > 0 && chosen < ids.length;
  }, [chosen, ids.length]);

  return (
    <section
      aria-label={t('deadLetters.groupLabel', { step: group.step, error: title })}
      className={`${card} overflow-hidden`}
    >
      <div className="flex flex-wrap items-start gap-x-3.5 gap-y-3 px-[18px] py-4">
        {actionable && (
          <input
            ref={box}
            type="checkbox"
            aria-label={t('deadLetters.selectGroup', { n })}
            checked={chosen === ids.length}
            onChange={(e) => onToggle(ids, e.target.checked)}
            className="mt-1 size-4 flex-none accent-[var(--ms-accent)]"
          />
        )}
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-2">
          <div className="flex flex-wrap items-center gap-x-3.5 gap-y-2">
            <StatusBadge status={group.kind === 'dead' ? 'dead' : 'unrouted'} label={kindLabel} />
            <Link
              to={`${pipelinePath(pipeline)}?step=${encodeURIComponent(group.step)}`}
              className="font-mono text-small font-bold text-ink"
            >
              {group.step}
            </Link>
            <span className="text-small text-ink-2">
              <span title={`${f.dateTime(group.firstSeen)} → ${f.dateTime(group.lastSeen)}`}>
                {t('deadLetters.seen', { first: f.relative(group.firstSeen), last: f.relative(group.lastSeen) })}
              </span>{' '}
              · {t('deadLetters.fingerprint')} <span className="font-mono">{group.fingerprint.slice(0, 8)}</span>
            </span>
          </div>
          <p className="m-0 font-display text-[26px] leading-[30px] break-words">{title}</p>
          <ErrorText text={error} />
        </div>
        {actionable && (
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className={primary ? buttons.primary : buttons.secondary}
              onClick={() => onAction('replay', 'group')}
            >
              <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
                <path d={SH.replay} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
              </svg>
              {t('deadLetters.replayGroup', { n })}
            </button>
            <button type="button" className={buttons.secondary} onClick={() => onAction('discard', 'group')}>
              {t('deadLetters.discardGroup', { n })}
            </button>
          </div>
        )}
      </div>
      <ScrollRegion
        className="overflow-x-auto border-t border-line"
        label={t('deadLetters.tableLabel', { step: group.step })}
      >
        <table className="w-full border-collapse text-small">
          <thead>
            <tr className="text-left text-tiny text-ink-3">
              {actionable && (
                <th scope="col" className="w-11 py-2 pl-[18px]">
                  <span className="sr-only">{t('deadLetters.columns.select')}</span>
                </th>
              )}
              <th scope="col" className="px-2 py-2 font-bold">
                {t('deadLetters.columns.item')}
              </th>
              <th scope="col" className="px-2 py-2 font-bold">
                {t('deadLetters.columns.run')}
              </th>
              <th scope="col" className="px-2 py-2 font-bold">
                {t('deadLetters.columns.died')}
              </th>
              {actionable && (
                <th scope="col" className="py-2 pr-[18px] pl-2 text-right font-bold">
                  {t('deadLetters.columns.actions')}
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {group.deadLetters.map((d) => (
              <tr key={d.id} className="border-t border-line">
                {actionable && (
                  <td className="py-2 pl-[18px]">
                    <input
                      type="checkbox"
                      data-row
                      aria-label={t('deadLetters.selectOne', { id: d.id })}
                      checked={selected.has(d.id)}
                      onChange={(e) => onToggle([d.id], e.target.checked)}
                      className="size-4 accent-[var(--ms-accent)]"
                    />
                  </td>
                )}
                <td className="px-2 py-2">
                  <Link to={itemPath(pipeline, d.id)} className="font-mono text-ink" title={d.id}>
                    {shortId(d.id)}
                  </Link>
                </td>
                <td className="px-2 py-2 font-mono text-ink-2">{d.runId}</td>
                <td className="px-2 py-2 whitespace-nowrap text-ink-2">
                  <time dateTime={d.createdAt} title={f.dateTime(d.createdAt)}>
                    {f.relative(d.createdAt)}
                  </time>
                </td>
                {actionable && (
                  <td className="py-2 pr-[18px] pl-2 text-right">
                    <span className="inline-flex gap-3">
                      <button
                        type="button"
                        className={buttons.link}
                        aria-label={t('deadLetters.replayItem', { id: d.id })}
                        onClick={() => onAction('replay', [d])}
                      >
                        {t('deadLetters.replayOne')}
                      </button>
                      <button
                        type="button"
                        className={buttons.linkMuted}
                        aria-label={t('deadLetters.discardItem', { id: d.id })}
                        onClick={() => onAction('discard', [d])}
                      >
                        {t('deadLetters.discardOne')}
                      </button>
                    </span>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </ScrollRegion>
    </section>
  );
}
