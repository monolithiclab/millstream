import { useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useApiQuery } from '../../api/context';
import { Link, useParams } from 'react-router-dom';
import { liveUrls } from '../../api/api';
import type { Delivery, Event, Item, Lineage } from '../../api/schema';
import { buttons, card } from '../../common/buttons';
import { CopyButton } from '../../common/Copy';
import { JsonViewer } from '../../common/JsonViewer';
import { LogTail } from '../../common/LogTail';
import { useMediaQuery, WIDE } from '../../common/motion';
import { ErrorState, Skeleton } from '../../common/States';
import { StatusBadge, type StatusKey } from '../../common/Status';
import { byteSize, formatBytes, useFormat } from '../../common/useFormat';
import { parseData, useLiveStream } from '../../live/useLiveStream';
import { pipelinePath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { Artifacts, LineageTree } from './ItemParts';
import { Journey } from './Journey';

// Raw events kept on the page.
const MAX_EVENTS = 200;
// Ancestors whose deliveries make the journey: as deep as the lineage the server walks by default.
const JOURNEY_DEPTH = 10;

type Tab = 'journey' | 'payload' | 'lineage' | 'logs' | 'events';

// The words an item is called by: its title or name when the payload has one, else its key, else its id.
export function itemTitle(item: Item): string {
  const payload =
    typeof item.payload === 'object' && item.payload !== null ? (item.payload as Record<string, unknown>) : {};
  for (const field of ['title', 'name', 'display']) {
    const value = payload[field];
    if (typeof value === 'string' && value !== '') return value;
  }
  return item.key !== '' ? item.key : item.id;
}

// Where the item stands now: its live deliveries first (in review, running, waiting, dead), else done.
function currentState(deliveries: Delivery[]): { status: StatusKey; step?: string } {
  for (const state of ['awaitingReview', 'leased', 'pending', 'dead'] as const) {
    const d = deliveries.find((x) => x.state === state);
    if (d !== undefined) return { status: state, step: d.step };
  }
  return { status: deliveries.length === 0 ? 'pending' : 'done' };
}

// The deliveries of the item's journey: those of its ancestors that produced the next item of its lineage, then
// its own, oldest first.
export function journeyOf(item: Item, lineage: Lineage, ancestors: Delivery[][], own: Delivery[]): Delivery[] {
  const produced = new Set<string>();
  for (const node of [...lineage.up, item]) for (const parent of node.parents) produced.add(`${parent} ${node.step}`);
  const upstream = ancestors.flat().filter((d) => produced.has(`${d.itemId} ${d.step}`));
  return [...upstream, ...own].sort((a, b) => a.createdAt.localeCompare(b.createdAt));
}

function Tabs({
  tabs,
  current,
  onChange,
  label,
}: {
  tabs: { id: Tab; label: string }[];
  current: Tab;
  onChange: (tab: Tab) => void;
  label: string;
}) {
  const refs = useRef(new Map<Tab, HTMLButtonElement>());
  // Arrow keys move between tabs (WAI-ARIA tabs); only the current one is in the Tab order.
  const onKeyDown = (event: KeyboardEvent) => {
    if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
    const index = tabs.findIndex((tab) => tab.id === current);
    const next = tabs[(index + (event.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
    if (next === undefined) return;
    event.preventDefault();
    onChange(next.id);
    refs.current.get(next.id)?.focus();
  };
  return (
    // The tab list handles the arrow keys of its tabs.
    // eslint-disable-next-line jsx-a11y/interactive-supports-focus
    <div
      role="tablist"
      aria-label={label}
      onKeyDown={onKeyDown}
      className="flex gap-1 border-b border-line px-2 md:px-4"
    >
      {tabs.map((tab) => (
        <button
          key={tab.id}
          ref={(el) => {
            if (el === null) refs.current.delete(tab.id);
            else refs.current.set(tab.id, el);
          }}
          type="button"
          role="tab"
          id={`tab-${tab.id}`}
          aria-selected={current === tab.id}
          aria-controls={`panel-${tab.id}`}
          tabIndex={current === tab.id ? 0 : -1}
          onClick={() => onChange(tab.id)}
          className={`min-h-11 flex-1 border-b-2 px-2 pt-3 pb-2.5 md:flex-none md:px-2.5 ${current === tab.id ? 'border-ink font-bold text-ink' : 'border-transparent text-ink-2'}`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}

function RawEvents({ events }: { events: Event[] }) {
  const { t } = useTranslation();
  const f = useFormat();
  if (events.length === 0) return <p className="m-0 px-4 py-6 text-small text-ink-2">{t('item.events.empty')}</p>;
  return (
    <ol className="m-0 flex list-none flex-col p-0 font-mono text-tiny">
      {events.map((e) => (
        <li key={e.seq} className="flex flex-wrap gap-x-3 border-b border-line px-4 py-2 last:border-b-0">
          <span className="text-ink-3">{f.time(e.ts)}</span>
          <span className="font-bold">{e.kind}</span>
          <span className="min-w-0 break-all text-ink-2">{JSON.stringify(e.data)}</span>
        </li>
      ))}
    </ol>
  );
}

function Meta({ label, children }: { label: string; children: ReactNode }) {
  return (
    <span>
      {label} <span className="font-bold text-ink">{children}</span>
    </span>
  );
}

// Item detail (canvas "Item" and its 375 px French board): what the item is and where it stands, its journey with
// every attempt and the agent's transcript, its payload, artifacts and lineage, its live logs and raw events. Live:
// each state event of the item's stream reloads it. Below the md breakpoint the payload and the lineage move into
// tabs, as the phone board shows.
export function ItemScreen() {
  const { pipeline = '', item: itemId = '' } = useParams();
  const { t } = useTranslation();
  const f = useFormat();
  const wide = useMediaQuery(WIDE);
  const [chosen, setTab] = useState<Tab>('journey');
  const [events, setEvents] = useState<{ item: string; list: Event[] }>({ item: itemId, list: [] });

  const data = useApiQuery(`item:${pipeline}:${itemId}`, async (api) => {
    const [item, lineage, deliveries] = await Promise.all([
      api.getItem(pipeline, itemId),
      api.getLineage(pipeline, itemId),
      api.listItemDeliveries(pipeline, itemId),
    ]);
    return { item, lineage, deliveries, at: Date.now() };
  });
  const ready = data.status === 'ready' ? data.data : null;
  const ancestors = ready?.lineage.up.slice(0, JOURNEY_DEPTH).map((n) => n.id) ?? [];
  const upstream = useApiQuery(ready === null ? null : `journey:${pipeline}:${ancestors.join(',')}`, (api) =>
    Promise.all(ancestors.map((id) => api.listItemDeliveries(pipeline, id))),
  );

  const reload = () => {
    data.reload();
    upstream.reload();
  };
  useLiveStream(liveUrls.item(pipeline, itemId), {
    events: ['state'],
    onMessage: (message) => {
      const event = message.type === 'state' ? parseData<Event>(message) : null;
      if (event === null) return;
      setEvents((current) => ({
        item: itemId,
        list: [event, ...(current.item === itemId ? current.list : []).filter((e) => e.seq !== event.seq)].slice(
          0,
          MAX_EVENTS,
        ),
      }));
      data.reload();
    },
    onReconnect: reload,
    onSnapshotRequired: reload,
  });

  const tabs: { id: Tab; label: string }[] = [];
  const title = ready === null ? itemId : itemTitle(ready.item);
  const breadcrumb = (
    <nav
      aria-label={t('item.breadcrumb')}
      className="flex flex-wrap items-center gap-2 font-mono text-small text-ink-2"
    >
      <Link to={pipelinePath(pipeline)} className="text-ink-2 no-underline">
        {pipeline}
      </Link>
      <span aria-hidden="true" className="text-ink-3">
        /
      </span>
      <Link to={pipelinePath(pipeline, 'items')} className="font-sans text-ink-2 no-underline">
        {t('nav.items')}
      </Link>
    </nav>
  );

  if (ready === null) {
    return (
      <ScreenPage title={title} eyebrow={breadcrumb}>
        {data.status === 'error' ? (
          <ErrorState title={t('item.error')} error={data.error} onRetry={data.reload} />
        ) : (
          <Skeleton rows={3} className="h-40" />
        )}
      </ScreenPage>
    );
  }

  const { item, lineage, deliveries } = ready;
  const journey = journeyOf(item, lineage, upstream.status === 'ready' ? upstream.data : [], deliveries);
  const state = currentState(deliveries);
  const review = deliveries.find((d) => d.state === 'awaitingReview');
  const cost = journey.reduce((sum, d) => sum + d.costUsd, 0);
  const stateKeys = {
    awaitingReview: 'item.state.awaitingReview',
    leased: 'item.state.leased',
    dead: 'item.state.dead',
    pending: 'item.state.pending',
  } as const;
  const stateLabel =
    state.step === undefined || !(state.status in stateKeys)
      ? undefined
      : t(stateKeys[state.status as keyof typeof stateKeys], { step: state.step });

  tabs.push({ id: 'journey', label: t('item.tabs.journey', { n: f.num(journey.length) }) });
  if (!wide)
    tabs.push({ id: 'payload', label: t('item.tabs.payload') }, { id: 'lineage', label: t('item.tabs.lineage') });
  tabs.push({ id: 'logs', label: t('item.tabs.logs') });
  if (wide) tabs.push({ id: 'events', label: t('item.tabs.events') });
  const tab = tabs.some((x) => x.id === chosen) ? chosen : 'journey';

  const payload = (
    <JsonViewer
      value={item.payload}
      title={t('item.payload.title')}
      meta={t('item.payload.meta', {
        stream: item.stream,
        size: formatBytes(byteSize(JSON.stringify(item.payload)), f),
      })}
    />
  );
  const lineageTree = <LineageTree pipeline={pipeline} item={item} lineage={lineage} />;

  return (
    <ScreenPage
      title={title}
      eyebrow={
        <div className="flex flex-col gap-2">
          {breadcrumb}
          <div className="flex flex-wrap items-center gap-2.5">
            <span className="font-mono text-ui break-all text-ink-2">{item.id}</span>
            <CopyButton text={item.id} label={t('item.copyId')} icon />
            <StatusBadge status={state.status} label={stateLabel} />
          </div>
        </div>
      }
    >
      <div className="flex flex-wrap items-end gap-x-8 gap-y-4">
        <div className="flex min-w-0 flex-[1_1_480px] flex-wrap gap-x-[18px] gap-y-1.5 text-small text-ink-2">
          <Meta label={t('item.meta.run')}>
            <Link to={`${pipelinePath(pipeline, 'runs')}?run=${encodeURIComponent(item.runId)}`} className="text-ink">
              {item.runId}
            </Link>
          </Meta>
          <Meta label={t('item.meta.stream')}>
            <span className="font-mono font-normal">{item.stream}</span>
          </Meta>
          <Meta label={t('item.meta.step')}>
            <span className="font-mono font-normal">{item.step}</span>
          </Meta>
          <Meta label={t('item.meta.cost')}>{f.money(cost)}</Meta>
          <span title={f.dateTime(item.ts)}>
            {t('item.meta.created', { time: f.time(item.ts) })}
            {item.parents.length > 0 &&
              ` · ${t(f.plural(item.parents.length, 'item.meta.sourcesOne', 'item.meta.sourcesOther'), { n: f.num(item.parents.length) })}`}
          </span>
        </div>
        {review !== undefined && (
          <Link
            to={`/inbox?pipeline=${encodeURIComponent(pipeline)}&delivery=${encodeURIComponent(review.id)}`}
            className={`${buttons.primary} w-full py-3 text-body no-underline md:w-auto md:py-2 md:text-small`}
          >
            {t('item.reviewNow')}
          </Link>
        )}
      </div>

      <div className="mt-4 flex flex-wrap items-start gap-6">
        <section aria-label={t('item.journey.label')} className={`${card} min-w-0 flex-[999_1_640px]`}>
          <Tabs label={t('item.tabs.label')} current={tab} onChange={setTab} tabs={tabs} />
          <div role="tabpanel" id={`panel-${tab}`} aria-labelledby={`tab-${tab}`}>
            {tab === 'journey' && <Journey key={item.id} pipeline={pipeline} deliveries={journey} now={ready.at} />}
            {tab === 'payload' && <div className="p-3">{payload}</div>}
            {tab === 'lineage' && <div className="p-3">{lineageTree}</div>}
            {tab === 'logs' && (
              <div className="p-4">
                <LogTail url={liveUrls.logs({ pipeline, item: item.id })} title={t('item.logs')} />
              </div>
            )}
            {tab === 'events' && <RawEvents events={events.item === itemId ? events.list : []} />}
          </div>
        </section>

        <div className="flex min-w-0 flex-[1_1_380px] flex-col gap-6">
          {wide && payload}
          <Artifacts pipeline={pipeline} item={item} />
          {wide && lineageTree}
        </div>
      </div>
    </ScreenPage>
  );
}
