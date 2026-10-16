import {
  memo,
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { withQuery } from '../../api/api';
import type { ReviewDecision } from '../../api/schema';
import { formatDuration, formatNumber } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { Kbd } from '../../shell/Kbd';
import { screenKeyAllowed } from '../../shell/useShortcuts';
import { useLatest } from '../../useLatest';
import { ScreenPage } from '../ScreenPage';
import { PipelineSelect } from '../../common/PipelineSelect';
import { EmptyState, ErrorState, Loading } from '../../common/States';
import { StatusGlyph, useStatusLabel } from '../../common/Status';
import { useNow } from '../../common/useNow';
import { buttons, field as fieldClass } from '../../common/styles';
import { changes, fieldKind, initialText, lookup, parseField, payloadEntries, toPatch } from './patch';
import { ReasonDialog } from './ReasonDialog';
import { ChangeList, fieldId, ReviewPanel, titleOf, type Field } from './ReviewPanel';
import { useDecisions, type Decision, type Outcome } from './useDecisions';
import { hasMore, subject, useInboxQueue, type Entry } from './useInboxQueue';

const noEntries: Entry[] = [];

// The review inbox: the queue on the left, the item on the right, decided from the keyboard (a, r then 1–9, e, u,
// j and k) or the buttons. On a phone the queue folds into a list button and the actions sit at the bottom.
export function InboxScreen() {
  const { t } = useTranslation();
  const { language, singleKeys } = usePreferences();
  const navigate = useNavigate();
  const params = useParams();
  const [search] = useSearchParams();
  const pipelineFilter = search.get('pipeline') ?? undefined;
  const inboxFilter = params.inbox ?? search.get('inbox') ?? undefined;
  const filters = useMemo(() => {
    const f: { pipeline?: string; inbox?: string } = {};
    if (pipelineFilter) f.pipeline = pipelineFilter;
    if (inboxFilter) f.inbox = inboxFilter;
    return f;
  }, [pipelineFilter, inboxFilter]);
  const queue = useInboxQueue(filters);
  const now = useNow();
  const entries = queue.status === 'ready' ? queue.entries : noEntries;

  const [chosen, setChosen] = useState<string | null>(null);
  // A review notification links to /inbox/<inbox>/<delivery>; the item screen to /inbox?pipeline=&delivery=.
  const linked = params.delivery ?? search.get('delivery') ?? undefined;
  // The chosen card, else the one a notification links to, else the oldest.
  const byKey = entries.findIndex((e) => e.key === chosen);
  const byLink =
    chosen === null && linked !== undefined ? entries.findIndex((e) => subject(e).deliveryId === linked) : -1;
  const index = byKey >= 0 ? byKey : byLink >= 0 ? byLink : entries.length > 0 ? 0 : -1;
  const selected: Entry | undefined = entries[index];
  const linkGone = queue.status === 'ready' && chosen === null && linked !== undefined && byLink < 0;

  // The editable fields' text, for the selected card only.
  const [texts, setTexts] = useState<{ key: string | undefined; values: Record<string, string> }>({
    key: undefined,
    values: {},
  });
  const values = texts.key === selected?.key ? texts.values : {};
  const review = selected?.kind === 'review' ? selected.review : undefined;
  const fields: Field[] = (review?.editable ?? []).map((pointer) => {
    const original = lookup(review?.item.payload, pointer).value;
    const kind = fieldKind(original);
    const text = values[pointer] ?? initialText(original, language === 'fr');
    return { pointer, kind, original, text, parsed: parseField(kind, text) };
  });
  const changeList = changes(
    review?.item.payload,
    Object.fromEntries(fields.filter((f) => values[f.pointer] !== undefined).map((f) => [f.pointer, f.parsed])),
  );
  const invalid = fields.findIndex((f) => !f.parsed.ok);

  const [announcement, setAnnouncement] = useState('');
  const [alert, setAlert] = useState<{ title: string; message: string } | null>(null);
  const [reasonFor, setReasonFor] = useState<Entry | null>(null);
  const [listOpen, setListOpen] = useState(false);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const focusTitle = useRef(false);
  const queueId = useId();

  useEffect(() => {
    if (focusTitle.current) titleRef.current?.focus();
    focusTitle.current = false;
  }, [selected?.key]);

  const select = useCallback((key: string | undefined) => {
    if (key === undefined) return;
    focusTitle.current = true;
    setChosen(key);
    setListOpen(false);
  }, []);

  const { pending, decide, undo } = useDecisions({
    onRemove: (d) => {
      const at = entries.findIndex((e) => e.key === d.entry.key);
      const next = entries[at + 1] ?? entries[at - 1];
      focusTitle.current = true;
      setChosen(next?.key ?? null);
      queue.remove(d.entry.key);
    },
    onRestore: (d) => {
      queue.restore(d.entry);
      focusTitle.current = true;
      setChosen(d.entry.key);
    },
    onConflict: (d, error) =>
      setAlert({ title: t('inbox.conflict', { title: titleOf(d.entry).title }), message: error.message }),
    onError: (d, error) =>
      setAlert({ title: t('inbox.decisionFailed', { title: titleOf(d.entry).title }), message: error.message }),
  });

  const outcomeLabel: Record<Outcome, string> = {
    approved: t('inbox.outcome.approved'),
    edited: t('inbox.outcome.edited'),
    rejected: t('inbox.outcome.rejected'),
    agreed: t('inbox.outcome.agreed'),
    disagreed: t('inbox.outcome.disagreed'),
  };

  const commit = (decision: Decision) => {
    setAlert(null);
    setReasonFor(null);
    decide(decision);
    const left = t('inbox.left', { count: entries.length - 1 });
    setAnnouncement(`${outcomeLabel[decision.outcome]} ${left}${singleKeys ? ` ${t('inbox.undoHint')}` : ''}`);
  };

  const approve = () => {
    if (selected === undefined) return;
    if (selected.kind === 'sample') {
      commit({ entry: selected, kind: 'sample', outcome: 'agreed', body: { decision: 'agree' } });
      return;
    }
    if (invalid >= 0) {
      setAnnouncement(t('inbox.fixFields'));
      document.getElementById(fieldId(invalid))?.focus();
      return;
    }
    const patch = toPatch(changeList);
    const body: ReviewDecision = { decision: 'approve', version: selected.review.version };
    if (patch.length > 0) body.patch = patch;
    commit({ entry: selected, kind: 'review', outcome: patch.length > 0 ? 'edited' : 'approved', body });
  };

  const pickReason = (entry: Entry, reason: string | undefined, note: string) => {
    if (entry.kind === 'sample') {
      const body = note === '' ? { decision: 'disagree' as const } : { decision: 'disagree' as const, reason: note };
      commit({ entry, kind: 'sample', outcome: 'disagreed', body });
      return;
    }
    const body: ReviewDecision = { decision: 'reject', version: entry.review.version };
    if (reason !== undefined) body.reason = reason;
    if (note !== '') body.text = note;
    commit({ entry, kind: 'review', outcome: 'rejected', body });
  };

  const undoLast = () => {
    const decision = undo();
    if (decision) setAnnouncement(t('inbox.undone', { title: titleOf(decision.entry).title }));
  };

  const edit = () => document.getElementById(fieldId(0))?.focus();
  const discard = () => setTexts({ key: selected?.key, values: {} });
  const move = (step: number) => select(entries[Math.min(entries.length - 1, Math.max(0, index + step))]?.key);

  const onFieldKey = (event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      titleRef.current?.focus();
    } else if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      approve();
    }
  };

  const queueRef = useRef<HTMLElement>(null);
  const hasEdits = Object.keys(values).length > 0;
  const reject = () => selected && setReasonFor(selected);
  const keys = useLatest({ approve, reject, edit, undoLast, move, discard, hasEdits });
  useEffect(() => {
    if (!singleKeys) return;
    const onKey = (event: KeyboardEvent) => {
      if (!screenKeyAllowed(event)) return;
      const inQueue = queueRef.current?.contains(event.target as Node) ?? false;
      const k = keys.current;
      const actions: Record<string, (() => void) | undefined> = {
        a: k.approve,
        r: k.reject,
        e: k.edit,
        u: k.undoLast,
        j: () => k.move(1),
        k: () => k.move(-1),
        ArrowDown: inQueue ? () => k.move(1) : undefined,
        ArrowUp: inQueue ? () => k.move(-1) : undefined,
        Escape: k.hasEdits ? k.discard : undefined,
      };
      const action = actions[event.key];
      if (action === undefined) return;
      event.preventDefault();
      action();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [singleKeys, keys]);

  const reviews = entries.filter((e) => e.kind === 'review').length;
  const samples = entries.length - reviews;
  const oldest = entries[0];
  const inboxes = [...new Set([...entries.map((e) => subject(e).inbox), ...(inboxFilter ? [inboxFilter] : [])])].sort();

  const setFilter = (name: 'pipeline' | 'inbox', value: string) => {
    setChosen(null);
    void navigate(withQuery('/inbox', { pipeline: pipelineFilter, inbox: inboxFilter, [name]: value || undefined }));
  };

  const header = (
    <>
      {queue.status === 'ready' && (
        <p className="m-0 text-small text-ink-2" data-live>
          <strong className="text-human">{t('inbox.toReview', { count: reviews })}</strong>
          {' · '}
          {t('inbox.samples', { count: samples })}
          {oldest && ` · ${t('inbox.oldest', { age: formatDuration(now - Date.parse(oldest.at), language) })}`}
        </p>
      )}
      <div className="flex flex-wrap gap-2">
        <label className="flex items-center gap-1.5 text-tiny text-ink-2">
          <span className="sr-only">{t('inbox.pipelineFilter')}</span>
          <PipelineSelect value={pipelineFilter ?? ''} onChange={(p) => setFilter('pipeline', p)} />
        </label>
        <label className="flex items-center gap-1.5 text-tiny text-ink-2">
          <span className="sr-only">{t('inbox.inboxFilter')}</span>
          <select value={inboxFilter ?? ''} onChange={(e) => setFilter('inbox', e.target.value)} className={fieldClass}>
            <option value="">{t('inbox.allInboxes')}</option>
            {inboxes.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
      </div>
    </>
  );

  return (
    <ScreenPage title={t('nav.inbox')} header={header}>
      <p role="status" aria-live="polite" className="sr-only">
        {announcement}
      </p>
      {queue.status === 'loading' && <Loading />}
      {queue.status === 'error' && <ErrorState message={queue.error.message} onRetry={queue.reload} />}
      {alert && <ErrorState title={alert.title} message={alert.message} onDismiss={() => setAlert(null)} />}
      {linkGone && (
        <p role="status" className="m-0 rounded-2 bg-surface-2 px-3.5 py-2.5 text-small">
          {t('inbox.linkGone')}
        </p>
      )}
      {queue.status === 'ready' && entries.length === 0 && (
        <EmptyState title={t('inbox.zero')} done>
          {t('inbox.zeroBody')}
        </EmptyState>
      )}
      {queue.status === 'ready' && selected !== undefined && (
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:gap-6">
          <div className="flex items-center gap-1 rounded-2 border border-line bg-surface px-1 py-1 md:hidden">
            <button
              type="button"
              aria-expanded={listOpen}
              aria-controls={queueId}
              onClick={() => setListOpen(!listOpen)}
              className="inline-flex size-11 items-center justify-center rounded-1"
            >
              <span className="sr-only">{t('inbox.list')}</span>
              <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
                <path d="M3 5H15M3 9H15M3 13H15" stroke="currentColor" strokeWidth="1.6" />
              </svg>
            </button>
            <span className="flex-1 text-small text-ink-2">
              <strong className="text-human">{index + 1}</strong> {t('inbox.ofTotal', { total: entries.length })}
            </span>
            {[
              { label: t('inbox.previous'), step: -1, disabled: index <= 0, d: 'M11 3.5L5.5 9L11 14.5' },
              { label: t('inbox.next'), step: 1, disabled: index >= entries.length - 1, d: 'M7 3.5L12.5 9L7 14.5' },
            ].map((b) => (
              <button
                key={b.step}
                type="button"
                aria-label={b.label}
                disabled={b.disabled}
                onClick={() => move(b.step)}
                className="inline-flex size-11 items-center justify-center rounded-1 text-ink disabled:text-control-line"
              >
                <svg width="16" height="16" viewBox="0 0 18 18" aria-hidden="true">
                  <path d={b.d} fill="none" stroke="currentColor" strokeWidth="1.8" />
                </svg>
              </button>
            ))}
          </div>

          <section
            ref={queueRef}
            id={queueId}
            aria-label={t('inbox.queue')}
            className={`${listOpen ? 'flex' : 'hidden'} flex-col rounded-3 border border-line bg-surface md:sticky md:top-4 md:flex md:w-[340px] md:flex-none`}
          >
            <div className="flex items-center gap-2 border-b border-line px-4 py-2.5 text-tiny text-ink-2">
              <span className="flex-1">{t('inbox.oldestFirst')}</span>
              <Kbd>j</Kbd>
              <Kbd>k</Kbd>
              <span>{t('inbox.move')}</span>
            </div>
            <ul
              aria-label={t('inbox.items')}
              className="m-0 flex max-h-[70vh] list-none flex-col gap-1 overflow-y-auto p-2.5"
            >
              {entries.map((entry) => (
                <QueueRow
                  key={entry.key}
                  entry={entry}
                  now={now}
                  current={entry.key === selected.key}
                  onPick={select}
                />
              ))}
            </ul>
            {hasMore(queue.data) && (
              <div className="flex flex-col gap-2 border-t border-line p-2.5">
                {queue.moreError !== null && <ErrorState message={queue.moreError} />}
                <button
                  type="button"
                  onClick={() => void queue.loadMore()}
                  disabled={queue.loadingMore}
                  className={`${buttons.secondary} w-full`}
                >
                  {t('inbox.loadMore')}
                </button>
              </div>
            )}
          </section>

          <section aria-label={t('inbox.underReview')} className="flex min-w-0 flex-1 flex-col">
            <ReviewPanel
              ref={titleRef}
              entry={selected}
              now={now}
              position={index + 1}
              total={entries.length}
              fields={fields}
              onEdit={(pointer, text) => setTexts({ key: selected.key, values: { ...values, [pointer]: text } })}
              onFieldKey={onFieldKey}
            />
            <div className="sticky bottom-0 z-10 mt-6 -mx-4 flex flex-col gap-3 border-t border-line bg-surface px-3 pt-2.5 pb-3.5 md:mx-0 md:rounded-b-2 md:px-8 md:py-3">
              {changeList.length > 0 && <ChangeList list={changeList} />}
              <div className="grid grid-cols-[1fr_1fr_1.4fr] items-center gap-2 md:flex md:flex-wrap md:gap-2.5">
                <button
                  type="button"
                  onClick={() => setReasonFor(selected)}
                  className={`${buttons.danger} h-12 px-2 text-[15px] whitespace-normal md:h-11 md:px-4 md:text-ui`}
                >
                  {selected.kind === 'sample' ? t('inbox.disagree') : t('inbox.reject')}
                  <span className="hidden md:inline">
                    <Kbd>r</Kbd>
                  </span>
                </button>
                {selected.kind === 'review' && fields.length > 0 && (
                  <button
                    type="button"
                    onClick={edit}
                    className={`${buttons.secondary} h-12 px-2 text-[15px] md:hidden`}
                  >
                    {t('inbox.edit')}
                  </button>
                )}
                {hasEdits && (
                  <button
                    type="button"
                    onClick={discard}
                    className={`${buttons.secondary} hidden h-11 px-4 text-ui md:inline-flex`}
                  >
                    {t('inbox.discard')}
                    <Kbd>Esc</Kbd>
                  </button>
                )}
                <span className="hidden flex-1 md:block" />
                <span className="hidden text-tiny text-ink-3 md:inline">{t('inbox.nextOpens')}</span>
                <button
                  type="button"
                  onClick={approve}
                  className={`${buttons.primary} col-start-3 h-12 px-2 text-[15px] whitespace-normal md:h-11 md:px-[18px] md:text-ui`}
                >
                  {selected.kind === 'sample'
                    ? t('inbox.agree')
                    : changeList.length > 0
                      ? t('inbox.approveWithChanges', { count: changeList.length })
                      : t('inbox.approve')}
                  <span className="hidden md:inline">
                    <Kbd>a</Kbd>
                  </span>
                </button>
              </div>
            </div>
          </section>
        </div>
      )}

      {pending && (
        <div className="fixed inset-x-4 bottom-[96px] z-30 flex justify-center md:bottom-6">
          <div className="flex max-w-full flex-wrap items-center gap-3 rounded-2 bg-ink px-3.5 py-2.5 text-small text-bg shadow-2">
            <StatusGlyph status="done" size={12} tone={false} />
            <span className="min-w-0 break-words">
              {outcomeLabel[pending.outcome]} {titleOf(pending.entry).title}
            </span>
            <button type="button" onClick={undoLast} className="inline-flex items-center gap-2 font-bold underline">
              {t('inbox.undo')}
              <span className="rounded-[6px] border border-bg px-1.5 font-mono text-tiny no-underline">u</span>
            </button>
          </div>
        </div>
      )}

      {reasonFor && (
        <ReasonDialog
          title={reasonFor.kind === 'sample' ? t('inbox.disagreeTitle') : t('inbox.rejectTitle')}
          reasons={reasonFor.kind === 'review' ? reasonFor.review.reasons : []}
          confirm={reasonFor.kind === 'sample' ? t('inbox.disagreeConfirm') : t('inbox.rejectItem')}
          onPick={(reason, note) => pickReason(reasonFor, reason, note)}
          onClose={() => setReasonFor(null)}
        />
      )}
    </ScreenPage>
  );
}

// A queue card: title, how long it has waited, its first figures, and the sample mark. Memoised: typing in a field
// re-renders the screen, not the queue.
const QueueRow = memo(function QueueRow({
  entry,
  now,
  current,
  onPick,
}: {
  entry: Entry;
  now: number;
  current: boolean;
  onPick: (key: string) => void;
}) {
  const { language } = usePreferences();
  const statusLabel = useStatusLabel();
  const { title } = titleOf(entry);
  const figures = payloadEntries(subject(entry).item)
    .filter((e): e is [string, number] => typeof e[1] === 'number')
    .slice(0, 3);
  return (
    <li>
      <button
        type="button"
        aria-current={current ? 'true' : undefined}
        onClick={() => onPick(entry.key)}
        className={`flex w-full flex-col gap-1.5 rounded-[12px] px-3.5 py-3 text-left text-ink ${current ? 'bg-human-soft shadow-[inset_0_0_0_1.5px_var(--ms-human)]' : 'bg-surface hover:bg-surface-2'}`}
      >
        <span className="flex w-full items-baseline gap-2">
          <span className="min-w-0 flex-1 truncate font-bold">{title}</span>
          <span className="font-mono text-tiny text-ink-3">{formatDuration(now - Date.parse(entry.at), language)}</span>
        </span>
        <span className="flex w-full flex-wrap items-center gap-x-2.5 gap-y-1 text-tiny text-ink-2">
          {figures.map(([key, value]) => (
            <span key={key}>
              {key} <strong className="text-ink">{formatNumber(value, language, { maximumFractionDigits: 2 })}</strong>
            </span>
          ))}
          {entry.kind === 'sample' && (
            <span className="ml-auto inline-flex items-center gap-1 rounded-1 border border-human px-1.5 font-bold text-human">
              <StatusGlyph status="sample" size={10} />
              {statusLabel('sample')}
            </span>
          )}
        </span>
      </button>
    </li>
  );
});
