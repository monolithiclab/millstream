import { memo, useId, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useMediaQuery } from '../../common/motion';
import { ScrollRegion } from '../../common/ScrollRegion';
import { Tag } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import {
  highlight,
  languageOf,
  parsePatch,
  sideBySide,
  type DiffFile,
  type DiffLine,
  type FileOp,
  type Hunk,
  type Language,
  type Row,
  type TokenKind,
} from './diff';

// Side by side from Tailwind's lg breakpoint; unified below it.
const SIDE_BY_SIDE = '(min-width: 1024px)';

// The syntax tones, each checked against the diff's soft grounds in tokens.test.ts.
const tokenClass: Record<TokenKind, string> = {
  key: 'font-bold text-accent',
  keyword: 'font-bold text-accent',
  heading: 'font-bold text-accent',
  string: 'text-human',
  number: 'text-warn',
  comment: 'italic text-ink-2',
  plain: '',
};
const lineClass = { add: 'bg-ok-soft', del: 'bg-bad-soft', context: '' } as const;
const markClass = { add: 'text-ok', del: 'text-bad', context: '' } as const;
// Removed and added lines carry − and + marks, not only colour (canvas components board).
const markGlyph = { add: '+', del: '−', context: '' } as const;

function Code({ text, language }: { text: string; language: Language }) {
  return (
    <>
      {highlight(text, language).map((token, i) => (
        <span key={i} className={tokenClass[token.kind]}>
          {token.text}
        </span>
      ))}
    </>
  );
}

// One line: its number, its mark (and, for screen readers, the word), its code.
function Line({ line, number, language }: { line: DiffLine; number: number | undefined; language: Language }) {
  const { t } = useTranslation();
  return (
    <div className={`flex ${lineClass[line.kind]}`}>
      <span aria-hidden="true" className="w-10 flex-none pr-2 text-right text-ink-3 select-none">
        {number}
      </span>
      <span aria-hidden="true" className={`w-4 flex-none font-bold select-none ${markClass[line.kind]}`}>
        {markGlyph[line.kind]}
      </span>
      {line.kind !== 'context' && (
        <span className="sr-only">{line.kind === 'add' ? t('improve.diff.added') : t('improve.diff.removed')} </span>
      )}
      <span className="pr-4 whitespace-pre">
        <Code text={line.text} language={language} />
      </span>
    </div>
  );
}

// A side with nothing on this row: hatched, so the eye reads the gap.
const Gap = () => <div aria-hidden="true" className="h-5 bg-sunken" />;

function HunkHeader({ hunk }: { hunk: Hunk }) {
  return <div className="bg-surface-2 px-3 text-ink-3 whitespace-pre">{hunk.header}</div>;
}

const rows = 'min-w-full w-max py-1';
const code = 'font-mono text-tiny leading-5';

function Unified({ file, language }: { file: DiffFile; language: Language }) {
  return (
    <ScrollRegion label={file.path} className={`overflow-x-auto rounded-1 border border-line ${code}`}>
      <div className={rows}>
        {file.hunks.map((hunk, h) => (
          <div key={h}>
            <HunkHeader hunk={hunk} />
            {hunk.lines.map((line, i) => (
              <Line key={i} line={line} number={line.kind === 'add' ? line.new : line.old} language={language} />
            ))}
          </div>
        ))}
      </div>
    </ScrollRegion>
  );
}

// One column of the side-by-side view: the old lines (left) or the new (right), a gap where the other side has more.
function Side({
  label,
  hunks,
  pairs,
  side,
  language,
}: {
  label: string;
  hunks: Hunk[];
  pairs: Row[][];
  side: 'left' | 'right';
  language: Language;
}) {
  const number = side === 'left' ? 'old' : 'new';
  return (
    <ScrollRegion label={label} className={`min-w-0 overflow-x-auto ${code}`}>
      <div className={rows}>
        {hunks.map((hunk, h) => (
          <div key={h}>
            <HunkHeader hunk={hunk} />
            {(pairs[h] ?? []).map((row, i) => {
              const line = row[side];
              return line === undefined ? (
                <Gap key={i} />
              ) : (
                <Line key={i} line={line} number={line[number]} language={language} />
              );
            })}
          </div>
        ))}
      </div>
    </ScrollRegion>
  );
}

function SideBySide({ file, language }: { file: DiffFile; language: Language }) {
  const { t } = useTranslation();
  const pairs = useMemo(() => file.hunks.map(sideBySide), [file]);
  const props = { hunks: file.hunks, pairs, language };
  return (
    <div className="grid grid-cols-2 divide-x divide-line rounded-1 border border-line">
      <Side {...props} side="left" label={t('improve.diff.before', { path: file.path })} />
      <Side {...props} side="right" label={t('improve.diff.after', { path: file.path })} />
    </div>
  );
}

function useOpLabel() {
  const { t } = useTranslation();
  const labels: Record<FileOp, string> = {
    modify: t('improve.diff.op.modify'),
    create: t('improve.diff.op.create'),
    delete: t('improve.diff.op.delete'),
  };
  return (op: FileOp) => labels[op];
}

// The patch: its files (each a link to its diff, with what it adds and removes), then each file's hunks.
// Memoised: the page's dialogs and notices do not parse and highlight the patch again.
export const DiffViewer = memo(function DiffViewer({ patch }: { patch: string }) {
  const { t } = useTranslation();
  const f = useFormat();
  const prefix = useId();
  const wide = useMediaQuery(SIDE_BY_SIDE);
  const opLabel = useOpLabel();
  const files = useMemo(() => parsePatch(patch), [patch]);
  const counts = (file: DiffFile) =>
    t('improve.diff.counts', { added: f.num(file.added), removed: f.num(file.removed) });
  if (files.length === 0) return <p className="m-0 text-small text-ink-2">{t('improve.diff.empty')}</p>;
  return (
    <div className="flex flex-col gap-4">
      <nav aria-label={t('improve.diff.files')}>
        <ul className="m-0 flex list-none flex-col gap-1 p-0 text-small">
          {files.map((file, i) => (
            <li key={file.path} className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
              <a href={`#${prefix}-${i}`} className="font-mono break-all text-ink">
                {file.path}
              </a>
              <Tag tone={file.op === 'create' ? 'ok' : file.op === 'delete' ? 'bad' : 'mute'}>{opLabel(file.op)}</Tag>
              <span className="text-tiny text-ink-2">{counts(file)}</span>
            </li>
          ))}
        </ul>
      </nav>
      {files.map((file, i) => {
        const language = languageOf(file.path);
        return (
          <section key={file.path} aria-labelledby={`${prefix}-${i}`} className="flex flex-col gap-2">
            <h3
              id={`${prefix}-${i}`}
              tabIndex={-1}
              className="m-0 flex scroll-mt-4 flex-wrap items-center gap-x-2.5 gap-y-1 text-small font-bold"
            >
              <span className="font-mono break-all">{file.path}</span>
              <span className="text-tiny font-normal text-ink-2">{counts(file)}</span>
            </h3>
            {wide ? <SideBySide file={file} language={language} /> : <Unified file={file} language={language} />}
          </section>
        );
      })}
    </div>
  );
});
