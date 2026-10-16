import { forwardRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { enc } from '../../api/api';
import type { ArtifactRef, Item } from '../../api/schema';
import { formatDuration, formatNumber } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { pipelinePath } from '../../routes';
import { Id } from '../parts/Id';
import { StatusBadge } from '../parts/Status';
import { payloadEntries, segments, type Change, type FieldKind, type Parsed } from './patch';
import { subject, type Entry } from './useInboxQueue';

export interface Field {
  pointer: string;
  kind: FieldKind;
  original: unknown;
  text: string;
  parsed: Parsed;
}

// The title and the lines under it: the review's display template rendered by the server, else the item's key.
export function titleOf(entry: Entry): { title: string; lines: string[] } {
  const s = subject(entry);
  const [first, ...rest] = s.display.split('\n').filter((line) => line.trim() !== '');
  return { title: first ?? s.item.key, lines: rest };
}

const isUrl = (v: unknown): v is string => typeof v === 'string' && /^https?:\/\//.test(v);

// The payload's top-level scalars, as fact cards; editable fields and the title are shown elsewhere.
export function factsOf(item: Item, skip: Set<string>, title: string): [string, unknown][] {
  return payloadEntries(item).filter(
    ([key, value]) =>
      !skip.has(key) &&
      value !== title &&
      !isUrl(value) &&
      (value === null || ['number', 'string', 'boolean'].includes(typeof value)),
  );
}

export function FactValue({ value }: { value: unknown }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  if (typeof value === 'number') {
    return (
      <span className="font-display text-[32px] leading-9">
        {formatNumber(value, language, { maximumFractionDigits: 2 })}
      </span>
    );
  }
  if (typeof value === 'boolean') return <span className="text-body">{value ? t('common.yes') : t('common.no')}</span>;
  if (value === null || value === '') return <span className="text-body text-ink-3">{t('common.none')}</span>;
  return <span className="text-body break-words">{String(value)}</span>;
}

// A short rendering of a value for the diff line.
export function shortValue(value: unknown, none: string, language: 'en' | 'fr'): string {
  if (value === null || value === undefined || value === '') return none;
  if (typeof value === 'number') return formatNumber(value, language, { maximumFractionDigits: 6 });
  const text = typeof value === 'string' ? `"${value}"` : JSON.stringify(value);
  return text.length > 32 ? `${text.slice(0, 30)}…${text.endsWith('"') ? '"' : ''}` : text;
}

function artifactUrl(item: Item, artifact: ArtifactRef): string {
  return `/api/v1/pipelines/${enc(item.pipeline)}/items/${enc(item.id)}/artifacts/${enc(artifact.name)}`;
}

export const fieldId = (index: number) => `review-field-${index}`;

interface PanelProps {
  entry: Entry;
  now: number;
  position: number;
  total: number;
  fields: Field[];
  onEdit: (pointer: string, text: string) => void;
  // Esc in a field leaves it, ⌘↵ or Ctrl+↵ approves.
  onFieldKey: (event: KeyboardEvent<HTMLElement>) => void;
}

// The item under review, rendered for a person: title, photos, facts, the editable fields marked as such.
export const ReviewPanel = forwardRef<HTMLHeadingElement, PanelProps>(function ReviewPanel(
  { entry, now, position, total, fields, onEdit, onFieldKey },
  titleRef,
) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const s = subject(entry);
  const item = s.item;
  const { title, lines } = titleOf(entry);
  const editableTop = new Set(fields.map((f) => segments(f.pointer)[0] ?? ''));
  const facts = factsOf(item, editableTop, title);
  const links = payloadEntries(item).filter((e): e is [string, string] => isUrl(e[1]));
  const images = item.artifacts.filter((a) => a.type.startsWith('image/'));
  // The payload's JSON is built only once someone opens it.
  const [showJson, setShowJson] = useState(false);
  const waited = now - Date.parse(entry.at);

  return (
    <div className="flex max-w-[860px] flex-col gap-[22px]">
      <div className="flex flex-col gap-1.5">
        <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-tiny text-ink-3">
          <span>
            {t('inbox.position', { position, total })} · <span className="font-mono">{s.step}</span> ·{' '}
            <span className="font-mono">{s.pipeline}</span> ·{' '}
            {t('inbox.waiting', { age: formatDuration(waited, language) })}
          </span>
          <Link to={pipelinePath(s.pipeline, `items/${enc(item.id)}`)} className="text-ink-2">
            <Id value={item.id} />
          </Link>
          {entry.kind === 'sample' && <StatusBadge name="sample">{t('inbox.sample')}</StatusBadge>}
          {links.map(([key, url]) => (
            <a key={key} href={url} target="_blank" rel="noreferrer" className="font-bold md:ml-auto">
              {t('inbox.openLink', { field: key })}
            </a>
          ))}
        </div>
        <h2
          ref={titleRef}
          tabIndex={-1}
          className="m-0 font-display text-[38px] leading-10 font-normal tracking-[-0.01em] break-words focus-visible:outline-none md:text-[46px] md:leading-[50px]"
        >
          {title}
        </h2>
        {lines.map((line) => (
          <p key={line} className="m-0 text-body text-ink-2">
            {line}
          </p>
        ))}
      </div>

      {entry.kind === 'sample' && (
        <p className="m-0 rounded-2 border border-dashed border-human bg-human-soft px-4 py-3 text-small text-ink">
          {t('inbox.sampleExplained')}
        </p>
      )}

      {images.length > 0 && (
        <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
          {images.slice(0, 4).map((a) => (
            <img
              key={a.name}
              src={artifactUrl(item, a)}
              alt={a.name}
              className="aspect-[4/3] w-full border border-line bg-surface-2 object-cover"
            />
          ))}
        </div>
      )}

      {facts.length > 0 && (
        <dl className="m-0 grid grid-cols-2 gap-2 md:grid-cols-[repeat(auto-fit,minmax(150px,1fr))] md:gap-2.5">
          {facts.map(([key, value]) => (
            <div
              key={key}
              className="flex min-w-0 flex-col gap-0.5 rounded-[16px] bg-surface px-3.5 py-3 shadow-1 md:px-4 md:py-3.5"
            >
              <dt className="font-mono text-tiny text-ink-3">{key}</dt>
              <dd className="m-0 min-w-0">
                <FactValue value={value} />
              </dd>
            </div>
          ))}
        </dl>
      )}

      {fields.length > 0 && (
        <fieldset className="m-0 flex flex-col gap-3.5 rounded-[16px] border-[1.5px] border-dashed border-human bg-surface p-3.5 md:p-4">
          <legend className="inline-flex items-center gap-1.5 px-1.5 text-small font-bold text-human">
            <svg width="12" height="12" viewBox="0 0 14 14" aria-hidden="true">
              <path
                d="M9.5 2L12 4.5L5 11.5L2 12L2.5 9Z"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.4"
                strokeLinejoin="round"
              />
            </svg>
            {t('inbox.editable')}
          </legend>
          <div className="flex flex-wrap gap-4">
            {fields.map((field, index) => (
              <EditField key={field.pointer} id={fieldId(index)} field={field} onEdit={onEdit} onKeyDown={onFieldKey} />
            ))}
          </div>
        </fieldset>
      )}

      <details
        className="rounded-2 border border-line bg-surface px-3 py-2 text-small"
        onToggle={(e) => setShowJson(e.currentTarget.open)}
      >
        <summary className="cursor-pointer font-bold text-ink-2">{t('inbox.payload')}</summary>
        {showJson && (
          <pre className="m-0 mt-2 overflow-x-auto font-mono text-tiny break-words whitespace-pre-wrap">
            {JSON.stringify(item.payload, null, 2)}
          </pre>
        )}
      </details>
    </div>
  );
});

function EditField({
  id,
  field,
  onEdit,
  onKeyDown,
}: {
  id: string;
  field: Field;
  onEdit: (pointer: string, text: string) => void;
  onKeyDown: (event: KeyboardEvent<HTMLElement>) => void;
}) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const name = segments(field.pointer).at(-1) ?? field.pointer;
  const error = field.parsed.ok ? null : field.parsed.error === 'number' ? t('inbox.badNumber') : t('inbox.badJson');
  const changed = field.parsed.ok && JSON.stringify(field.parsed.value) !== JSON.stringify(field.original);
  const hintId = `${id}-hint`;
  const label: ReactNode = (
    <>
      {name} <span className="font-mono text-tiny font-normal text-ink-3">{field.pointer}</span>
    </>
  );
  const control = 'w-full rounded-1 border bg-bg px-2.5 text-ink focus-visible:outline-offset-0';
  const tone = error ? 'border-bad' : changed ? 'border-2 border-accent' : 'border-control-line';
  const describedBy = error || changed ? hintId : undefined;

  if (field.kind === 'boolean') {
    return (
      <label className="flex min-h-11 items-center gap-2.5 text-small font-bold">
        <input
          id={id}
          type="checkbox"
          checked={field.text === 'true'}
          onChange={(e) => onEdit(field.pointer, String(e.target.checked))}
          onKeyDown={onKeyDown}
          className="size-[18px] accent-accent"
        />
        {label}
      </label>
    );
  }
  const long = field.kind === 'json' || (field.kind === 'text' && (field.text.length > 40 || name === 'notes'));
  return (
    <div className={`flex min-w-0 flex-col gap-1.5 ${long ? 'flex-[1_1_320px]' : 'w-full md:w-[180px]'}`}>
      <label htmlFor={id} className="text-small font-bold">
        {label}
      </label>
      {long ? (
        <textarea
          id={id}
          rows={field.kind === 'json' ? 4 : 2}
          value={field.text}
          aria-invalid={error !== null}
          aria-describedby={describedBy}
          onChange={(e) => onEdit(field.pointer, e.target.value)}
          onKeyDown={onKeyDown}
          className={`${control} ${tone} min-h-11 resize-y py-2 text-[16px] md:text-ui ${field.kind === 'json' ? 'font-mono' : ''}`}
        />
      ) : (
        <input
          id={id}
          value={field.text}
          inputMode={field.kind === 'number' ? 'decimal' : undefined}
          aria-invalid={error !== null}
          aria-describedby={describedBy}
          onChange={(e) => onEdit(field.pointer, e.target.value)}
          onKeyDown={onKeyDown}
          className={`${control} ${tone} h-11 font-mono text-[16px] md:h-9 md:text-[15px]`}
        />
      )}
      {(error || changed) && (
        <span id={hintId} className={`text-tiny ${error ? 'text-bad' : 'text-ink-3'}`}>
          {error ?? t('inbox.was', { value: shortValue(field.original, t('common.none'), language) })}
        </span>
      )}
    </div>
  );
}

// The edits before they are sent: each pointer, its old value struck out and the new one, with − and + words for
// those who cannot see the colours.
export function ChangeList({ list }: { list: Change[] }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const none = t('common.none');
  return (
    <div
      role="group"
      aria-label={t('inbox.yourChanges')}
      className="flex flex-wrap items-center gap-x-4 gap-y-2 text-small"
    >
      <strong>{t('inbox.changes', { count: list.length })}</strong>
      {list.map((c) => (
        <span key={c.pointer} className="inline-flex min-w-0 items-center gap-1.5 font-mono text-tiny">
          <span className="text-ink-2">{segments(c.pointer).at(-1) ?? c.pointer}</span>
          <del className="bg-bad-soft px-1 text-bad">
            <span className="sr-only">{t('inbox.before')} </span>
            {shortValue(c.before, none, language)}
          </del>
          <span aria-hidden="true">→</span>
          <ins className="bg-ok-soft px-1 text-ok no-underline">
            <span className="sr-only">{t('inbox.after')} </span>
            {shortValue(c.after, none, language)}
          </ins>
        </span>
      ))}
    </div>
  );
}
