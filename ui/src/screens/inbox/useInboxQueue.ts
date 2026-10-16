import { useCallback, useState } from 'react';
import type { Api } from '../../api/api';
import { useApi } from '../../api/context';
import type { Review, Sample } from '../../api/schema';
import { useLatest } from '../../useLatest';
import { errorMessage } from '../../common/Notice';
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

interface Page<T> {
  items: T[];
  next?: string;
}

// The first page of both lists (cursors null), or the next page of those that have one.
async function page(api: Api, filters: Filters, cursors: Cursors | null): Promise<Queue> {
  const query = { ...filters, state: 'open' as const, limit: PAGE };
  const read = <T>(
    cursor: string | undefined,
    list: (q: typeof query & { cursor?: string }) => Promise<Page<T>>,
  ): Promise<Page<T>> =>
    cursors === null ? list(query) : cursor === undefined ? Promise.resolve({ items: [] }) : list({ ...query, cursor });
  const [reviews, samples] = await Promise.all([
    read(cursors?.review, api.listInbox),
    read(cursors?.sample, api.listSamples),
  ]);
  const next: Cursors = {};
  if (reviews.next !== undefined) next.review = reviews.next;
  if (samples.next !== undefined) next.sample = samples.next;
  return { entries: [...reviews.items.map(reviewEntry), ...samples.items.map(sampleEntry)].sort(byAge), next };
}

// The open reviews and samples of the filters, page by page; cards leave on a decision and come back on undo.
export function useInboxQueue(filters: Filters) {
  const api = useApi();
  const key = `inbox:${JSON.stringify(filters)}`;
  const resource = useResource(key, (a) => page(a, filters, null));
  const { mutate } = resource;
  const keyRef = useLatest(key);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState<string | null>(null);

  const remove = useCallback(
    (entryKey: string) => mutate((q) => ({ ...q, entries: q.entries.filter((e) => e.key !== entryKey) })),
    [mutate],
  );
  const restore = useCallback(
    (entry: Entry) =>
      mutate((q) => ({ ...q, entries: [...q.entries.filter((e) => e.key !== entry.key), entry].sort(byAge) })),
    [mutate],
  );

  const loadMore = async () => {
    if (resource.status !== 'ready' || !hasMore(resource.data)) return;
    const asked = key;
    setLoadingMore(true);
    setMoreError(null);
    try {
      const more = await page(api, filters, resource.data.next);
      // A page that arrives after the filters changed belongs to the old queue.
      if (keyRef.current !== asked) return;
      mutate((q) => {
        const known = new Set(q.entries.map((e) => e.key));
        return {
          entries: [...q.entries, ...more.entries.filter((e) => !known.has(e.key))].sort(byAge),
          next: more.next,
        };
      });
    } catch (error) {
      setMoreError(errorMessage(error));
    } finally {
      setLoadingMore(false);
    }
  };

  return { ...resource, remove, restore, loadMore, loadingMore, moreError };
}
