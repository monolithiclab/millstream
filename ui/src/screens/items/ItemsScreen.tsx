import { useId, useState, type ChangeEvent, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { ApiError, type ItemQuery } from '../../api/api';
import { useApi } from '../../api/context';
import type { Item, ItemPage } from '../../api/schema';
import { Breadcrumb, pipelineCrumb } from '../../common/Breadcrumb';
import { HelpTip } from '../../common/HelpTip';
import { Id } from '../../common/Id';
import { Select } from '../../common/Select';
import { EmptyState, ErrorState, Loading } from '../../common/States';
import { StatusBadge } from '../../common/Status';
import { buttons, card, field, filterCaption, filterLabel } from '../../common/styles';
import { useFormat } from '../../common/useFormat';
import { appendPage, useLoadMore } from '../../common/useLoadMore';
import { useResource } from '../../common/useResource';
import { itemPath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { itemTitle } from '../item/ItemScreen';

export const ITEMS_PAGE = 50;

type Filters = Pick<ItemQuery, 'stream' | 'run' | 'where'>;

// The filters in the URL: a stream, a run and a CEL expression, each left out when empty.
function filtersOf(search: URLSearchParams): Filters {
  const out: Filters = {};
  for (const name of ['stream', 'run', 'where'] as const) {
    const value = search.get(name)?.trim();
    if (value) out[name] = value;
  }
  return out;
}

// A where the server could not compile or run: 400 invalid, its message said at the field.
function whereError(error: Error, filters: Filters): string | null {
  return filters.where !== undefined && error instanceof ApiError && error.status === 400 && error.code === 'invalid'
    ? error.message
    : null;
}

// Streams and items (spec 12.2, "Browse a stream, filter with CEL"): a pipeline's items in insertion order, by stream
// and run, filtered by a CEL expression over item, envelope, vars and run; each row opens the item. The filters live
// in the URL. A filtered page reads a bounded number of items, so it can come back short, even empty, with more to
// read: the list says so and reads on when asked.
export function ItemsScreen() {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const { pipeline = '' } = useParams();
  const [search, setSearch] = useSearchParams();
  const filters = filtersOf(search);
  const key = `items:${pipeline}:${JSON.stringify(filters)}`;
  const streams = useResource(`streams:${pipeline}`, (a) => a.listStreams(pipeline));
  const items = useResource(key, (a) => a.listItems(pipeline, { ...filters, limit: ITEMS_PAGE }));
  const { loadMore, loadingMore, moreError } = useLoadMore<ItemPage, string>({
    key,
    mutate: items.mutate,
    read: (cursor) => api.listItems(pipeline, { ...filters, limit: ITEMS_PAGE, cursor }),
    merge: appendPage,
  });

  // The run and where fields as typed, until applied (Enter or leaving the field); a new URL (back, a link) starts
  // them over.
  const fromUrl = { key, run: filters.run ?? '', where: filters.where ?? '' };
  const [draft, setDraft] = useState(fromUrl);
  const typed = draft.key === key ? draft : fromUrl;
  const ids = { run: useId(), where: useId(), whereError: useId() };

  const apply = (next: Filters) => {
    const params: Record<string, string> = {};
    for (const [name, value] of Object.entries({ ...filters, ...next })) if (value) params[name] = value;
    setSearch(params);
  };
  // A text field applies what it holds when it changed.
  const commit = (name: 'run' | 'where') => {
    const value = typed[name].trim();
    if (value !== (filters[name] ?? '')) apply({ [name]: value });
  };
  const textField = (name: 'run' | 'where') => ({
    id: ids[name],
    value: typed[name],
    onChange: (e: ChangeEvent<HTMLInputElement>) => setDraft({ ...typed, [name]: e.target.value }),
    onKeyDown: (e: KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') commit(name);
    },
    onBlur: () => commit(name),
    spellCheck: false,
    autoComplete: 'off',
  });

  const invalid = items.status === 'error' ? whereError(items.error, filters) : null;
  const streamList = streams.status === 'ready' ? streams.data : [];
  const filtered = filters.where !== undefined || filters.run !== undefined;
  const shown = items.status === 'ready' ? items.data.items.length : 0;

  return (
    <ScreenPage
      title={t('nav.items')}
      context={<Breadcrumb items={[pipelineCrumb(pipeline)]} />}
      summary={
        shown > 0 ? (
          <span role="status">{t(f.plural(shown, 'items.shownOne', 'items.shownOther'), { n: f.num(shown) })}</span>
        ) : undefined
      }
      toolbar={
        <>
          <label className={`${filterLabel} min-w-0 flex-[1_1_180px]`}>
            {t('items.stream')}
            <Select value={filters.stream ?? ''} onChange={(e) => apply({ stream: e.target.value })}>
              <option value="">{t('items.allStreams')}</option>
              {streamList.map((s) => (
                <option key={s.name} value={s.name}>
                  {t('items.streamOption', { stream: s.name, n: f.num(s.items) })}
                </option>
              ))}
              {filters.stream !== undefined && !streamList.some((s) => s.name === filters.stream) && (
                <option value={filters.stream}>{filters.stream}</option>
              )}
            </Select>
          </label>
          <label className={`${filterLabel} min-w-0 flex-[1_1_160px]`}>
            {t('items.run')}
            <input
              {...textField('run')}
              placeholder={t('items.runPlaceholder')}
              className={`${field} min-h-[34px] font-mono font-normal`}
            />
          </label>
          <div className="flex min-w-0 flex-[3_1_320px] flex-col gap-1">
            <span className="flex items-center gap-1.5">
              <label htmlFor={ids.where} className={filterCaption}>
                {t('items.where')}
              </label>
              <HelpTip label={t('items.whereHelp')}>{t('items.whereHint')}</HelpTip>
            </span>
            <input
              {...textField('where')}
              placeholder="item.priceEur < 300000"
              aria-invalid={invalid !== null}
              aria-describedby={invalid === null ? undefined : ids.whereError}
              className={`${field} min-h-[34px] font-mono ${invalid === null ? '' : 'border-bad'}`}
            />
            {invalid !== null && (
              <p id={ids.whereError} role="alert" className="m-0 text-tiny break-words text-bad">
                {t('items.whereInvalid', { message: invalid })}
              </p>
            )}
          </div>
          {Object.keys(filters).length > 0 && (
            <button
              type="button"
              onClick={() => {
                setDraft({ key, run: '', where: '' });
                setSearch({});
              }}
              className={`${buttons.secondary} ml-auto`}
            >
              {t('items.clear')}
            </button>
          )}
        </>
      }
    >
      <div>
        {items.status === 'loading' && <Loading rows={4} />}
        {items.status === 'error' && invalid === null && (
          <ErrorState message={items.error.message} onRetry={items.reload} />
        )}
        {items.status === 'ready' && (
          <ItemList
            pipeline={pipeline}
            page={items.data}
            filtered={filtered}
            loadingMore={loadingMore}
            moreError={moreError}
            onMore={(cursor) => void loadMore(cursor)}
          />
        )}
      </div>
    </ScreenPage>
  );
}

function ItemList({
  pipeline,
  page,
  filtered,
  loadingMore,
  moreError,
  onMore,
}: {
  pipeline: string;
  page: ItemPage;
  filtered: boolean;
  loadingMore: boolean;
  moreError: string | null;
  onMore: (cursor: string) => void;
}) {
  const { t } = useTranslation();
  const { items, next } = page;
  const more = next !== undefined && (
    <div className="flex flex-col gap-2">
      {moreError !== null && <ErrorState message={moreError} />}
      {filtered && <p className="m-0 text-tiny text-ink-2">{t('items.mayMatchFurther')}</p>}
      <button
        type="button"
        disabled={loadingMore}
        onClick={() => onMore(next)}
        className={`${buttons.secondary} self-start`}
      >
        {filtered ? t('items.searchFurther') : t('items.more')}
      </button>
    </div>
  );

  if (items.length === 0) {
    return (
      <div className="flex flex-col gap-3">
        <EmptyState
          title={next !== undefined ? t('items.noMatchYet') : filtered ? t('items.noMatch') : t('items.empty')}
        >
          {!filtered && next === undefined && t('items.emptyBody')}
        </EmptyState>
        {more}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <ul aria-label={t('items.list')} className={`${card} m-0 flex list-none flex-col px-4 py-1 md:px-6`}>
        {items.map((item) => (
          <ItemRow key={item.id} pipeline={pipeline} item={item} />
        ))}
      </ul>
      {more}
    </div>
  );
}

function ItemRow({ pipeline, item }: { pipeline: string; item: Item }) {
  const f = useFormat();
  const title = itemTitle(item);
  return (
    <li className="border-b border-line last:border-b-0">
      <Link
        to={itemPath(pipeline, item.id)}
        className="grid grid-cols-1 gap-1 py-2.5 text-ink no-underline hover:bg-surface-2 md:grid-cols-[minmax(0,1fr)_minmax(0,auto)] md:items-center md:gap-4"
      >
        <span className="flex min-w-0 flex-col gap-0.5">
          <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <strong className="min-w-0 text-small break-words">{title}</strong>
            {item.tombstone !== undefined && <StatusBadge status={item.tombstone} />}
          </span>
          <span className="flex flex-wrap gap-x-3 gap-y-0.5 text-tiny text-ink-2">
            {title !== item.id && <Id value={item.id} />}
            <span className="font-mono">
              {item.stream} · {item.step}
            </span>
          </span>
        </span>
        <span className="flex flex-wrap gap-x-3 font-mono text-tiny text-ink-2 md:justify-end">
          <Id value={item.runId} />
          <time dateTime={item.ts} title={f.dateTime(item.ts)}>
            {f.time(item.ts)}
          </time>
        </span>
      </Link>
    </li>
  );
}
