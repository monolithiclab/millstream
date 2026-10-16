import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import type { Review } from '../../api/schema';
import { QueryView } from '../../common/States';
import { buttons } from '../../common/styles';
import { useFormat } from '../../common/useFormat';
import { useResource } from '../../common/useResource';
import { itemPath } from '../../routes';
import { Dialog } from '../../shell/Dialog';
import { titleOf } from '../inbox/ReviewPanel';

// A lesson citing a review opens that review's decision in a panel: who decided what, when, why, and the edit, with
// a link to the item; not the raw JSON of the API.
export function ReviewSource({ pipeline, delivery }: { pipeline: string; delivery: string }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)} className={`${buttons.link} text-[length:inherit]`}>
        {t('lessons.fromReview', { id: delivery })}
      </button>
      {open && <ReviewPanel pipeline={pipeline} delivery={delivery} onClose={() => setOpen(false)} />}
    </>
  );
}

function ReviewPanel({ pipeline, delivery, onClose }: { pipeline: string; delivery: string; onClose: () => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  const review = useResource(`review:${pipeline}:${delivery}`, (api) => api.getReview(pipeline, delivery));
  return (
    <Dialog labelledBy={titleId} onClose={onClose}>
      <div className="flex flex-col gap-4 p-6">
        <div className="flex items-start gap-4">
          <h2 id={titleId} className="m-0 flex-1 font-display text-[28px] leading-8 font-normal break-words">
            {t('lessons.review.title', { id: delivery })}
          </h2>
          <button type="button" onClick={onClose} className={buttons.secondary}>
            {t('dialog.close')}
          </button>
        </div>
        <QueryView query={review} rows={2}>
          {(r) => <ReviewFacts review={r} onNavigate={onClose} />}
        </QueryView>
      </div>
    </Dialog>
  );
}

function ReviewFacts({ review, onNavigate }: { review: Review; onNavigate: () => void }) {
  const { t } = useTranslation();
  const f = useFormat();
  const { title, lines } = titleOf({ kind: 'review', key: review.deliveryId, at: review.parkedAt, review });
  const edited = (review.patch?.length ?? 0) > 0;
  const decisions = {
    approve: edited ? t('lessons.review.edited') : t('lessons.review.approved'),
    reject: t('lessons.review.rejected'),
  };
  const decision = review.decision === undefined ? t('lessons.review.undecided') : decisions[review.decision];
  const facts: [string, string][] = [[t('lessons.review.decision'), decision]];
  if (review.reviewer !== undefined) facts.push([t('lessons.review.reviewer'), review.reviewer]);
  if (review.decidedAt !== undefined) facts.push([t('lessons.review.decidedAt'), f.dateTime(review.decidedAt)]);
  if (review.reason !== undefined) facts.push([t('lessons.review.reason'), review.reason]);
  if (review.text !== undefined && review.text !== '') facts.push([t('lessons.review.note'), review.text]);
  facts.push([t('lessons.review.step'), `${review.step} · ${review.inbox}`]);
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-0.5">
        <Link to={itemPath(review.pipeline, review.item.id)} onClick={onNavigate} className="font-bold break-words">
          {title}
        </Link>
        {lines.length > 0 && <p className="m-0 text-small break-words text-ink-2">{lines.join(' · ')}</p>}
      </div>
      <dl className="m-0 grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-4 gap-y-2 text-small">
        {facts.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-ink-3">{label}</dt>
            <dd className="m-0 break-words">{value}</dd>
          </div>
        ))}
      </dl>
      {edited && (
        <div className="flex flex-col gap-1.5">
          <h3 className="m-0 text-small font-bold">{t('lessons.review.changes')}</h3>
          <ul className="m-0 flex list-none flex-col gap-1 p-0 font-mono text-tiny">
            {(review.patch ?? []).map((op, i) => (
              <li key={i} className="break-all">
                {String(op.op)} {String(op.path)}
                {'value' in op && ` → ${JSON.stringify(op.value)}`}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
