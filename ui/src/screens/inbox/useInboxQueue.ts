import { useCallback, useEffect, useRef, useState } from 'react';
import { useLatest } from '../../useLatest';
import type { Api } from '../../api/api';
import { useApi } from '../../api/context';
import type { Review, Sample } from '../../api/schema';
import { appendBy, useLoadMore } from '../../common/useLoadMore';
import { useResource } from '../../common/useResource';

// One card of the queue: an open review, or an open review sample (an auto-approved item a person double-checks).
export type Entry =
  | { kind: 'review'; key: string; at: string; review: Review }
  | { kind: 'sample'; key: string; at: string; sample: Sample };

const reviewEntry = (review: Review): Entry => ({
  kind: 'review',
  key: `review:${review.pipeline}/${review.deliveryId}`,
  at: review.parkedAt,
  review,
});

const sampleEntry = (sample: Sample): Entry => ({
  kind: 'sample',
  key: `sample:${sample.pipeline}/${sample.deliveryId}`,
  at: sample.createdAt,
  sample,
});

export const subject = (entry: Entry): Review | Sample => (entry.kind === 'review' ? entry.review : entry.sample);

// Oldest first, as the canvas orders the queue.
const byAge = (a: Entry, b: Entry) => a.at.localeCompare(b.at) || a.key.localeCompare(b.key);

export interface Filters {
  pipeline?: string;
  inbox?: string;
}

const PAGE = 50;
// The API's largest page: a live refresh reads at most this many of each list.
const MAX_PAGE = 500;
// The API has no inbox stream: the queue reads again this often, silently.
export const INBOX_REFRESH_MS = 15_000;

// The next page of each list; a list without one is complete.
interface Cursors {
  review?: string;
  sample?: string;
}

export interface Queue {
  entries: Entry[];
  next: Cursors;
}

export const hasMore = (queue: Queue) => queue.next.review !== undefined || queue.next.sample !== undefined;

interface Limits {
  review: number;
  sample: number;
}

interface Page<T> {
  items: T[];
  next?: string;
}

// The first page of both lists (cursors null), as long as `limits` asks, or the next page of those that have one.
async function page(
  api: Api,
  filters: Filters,
  cursors: Cursors | null,
  limits: Limits = { review: PAGE, sample: PAGE },
): Promise<Queue> {
  const query = { ...filters, state: 'open' as const };
  const read = <T>(
    cursor: string | undefined,
    limit: number,
    list: (q: typeof query & { cursor?: string; limit: number }) => Promise<Page<T>>,
  ): Promise<Page<T>> =>
    cursors === null
      ? list({ ...query, limit })
      : cursor === undefined
        ? Promise.resolve({ items: [] })
        : list({ ...query, limit, cursor });
  const [reviews, samples] = await Promise.all([
    read(cursors?.review, limits.review, api.listInbox),
    read(cursors?.sample, limits.sample, api.listSamples),
  ]);
  const next: Cursors = {};
  if (reviews.next !== undefined) next.review = reviews.next;
  if (samples.next !== undefined) next.sample = samples.next;
  return { entries: [...reviews.items.map(reviewEntry), ...samples.items.map(sampleEntry)].sort(byAge), next };
}

const matches = (entry: Entry, filters: Filters) => {
  const { pipeline, inbox } = subject(entry);
  return (filters.pipeline ?? pipeline) === pipeline && (filters.inbox ?? inbox) === inbox;
};

// The open reviews and samples of the filters, page by page, read again every INBOX_REFRESH_MS. A decided card is
// hidden at once (the server still lists it while its decision is held for undo); undo shows it again, in the queue
// of the filters it belongs to.
export function useInboxQueue(filters: Filters) {
  const api = useApi();
  const key = `inbox:${JSON.stringify(filters)}`;
  // How many of each list the queue shows, so a refresh reads as deep and pages loaded with "load more" stay.
  const depth = useRef<{ key: string; limits: Limits } | null>(null);
  const resource = useResource(key, (a) =>
    page(a, filters, null, depth.current?.key === key ? depth.current.limits : undefined),
  );
  const { mutate, refresh } = resource;
  const [hidden, setHidden] = useState<ReadonlySet<string>>(new Set());
  const latest = useLatest({ filters });

  const remove = useCallback((entryKey: string) => setHidden((h) => new Set(h).add(entryKey)), []);
  const restore = useCallback(
    (entry: Entry) => {
      setHidden((h) => {
        const next = new Set(h);
        next.delete(entry.key);
        return next;
      });
      if (matches(entry, latest.current.filters))
        mutate((q) =>
          q.entries.some((e) => e.key === entry.key) ? q : { ...q, entries: [...q.entries, entry].sort(byAge) },
        );
    },
    [mutate, latest],
  );

  const more = useLoadMore<Queue, Cursors>({
    key,
    mutate,
    read: (cursors) => page(api, filters, cursors),
    merge: (q, next) => ({ entries: appendBy(q.entries, next.entries, (e) => e.key).sort(byAge), next: next.next }),
  });
  const loadMore = async () => {
    if (resource.status === 'ready' && hasMore(resource.data)) await more.loadMore(resource.data.next);
  };

  useEffect(() => {
    if (resource.status !== 'ready') return;
    const count = (kind: Entry['kind']) =>
      Math.min(MAX_PAGE, Math.max(PAGE, resource.data.entries.filter((e) => e.kind === kind).length));
    depth.current = { key, limits: { review: count('review'), sample: count('sample') } };
  }, [key, resource]);

  // A silent refresh (useResource keeps what is shown on failure and drops an answer for filters left since).
  useEffect(() => {
    const timer = setInterval(() => {
      if (document.visibilityState !== 'hidden') refresh();
    }, INBOX_REFRESH_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const entries = resource.status === 'ready' ? resource.data.entries.filter((e) => !hidden.has(e.key)) : [];
  return { ...resource, entries, remove, restore, loadMore, loadingMore: more.loadingMore, moreError: more.moreError };
}
