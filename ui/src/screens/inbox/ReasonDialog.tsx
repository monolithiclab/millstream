import { useId, useState, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog } from '../../shell/Dialog';
import { Kbd } from '../../shell/Kbd';
import { isEditable } from '../../shell/useShortcuts';
import { button } from '../parts/styles';

// Reject (or disagree with a sample) with a reason: 1 to 9 picks a listed reason code, a note is optional. Picking
// decides; the undo window covers a slip. Without reason codes, the button (or 1) decides with the note alone.
export function ReasonDialog({
  title,
  reasons,
  confirm,
  onPick,
  onClose,
}: {
  title: string;
  reasons: string[];
  confirm: string;
  onPick: (reason: string | undefined, note: string) => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const titleId = useId();
  const noteId = useId();
  const [note, setNote] = useState('');

  const pick = (reason: string | undefined) => onPick(reason, note.trim());

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (isEditable(event.target) || event.metaKey || event.ctrlKey || event.altKey) return;
    const n = Number(event.key);
    if (!Number.isInteger(n) || n < 1) return;
    if (reasons.length === 0 && n === 1) {
      event.preventDefault();
      pick(undefined);
    } else if (n <= reasons.length) {
      event.preventDefault();
      pick(reasons[n - 1]);
    }
  };

  return (
    <Dialog labelledBy={titleId} onClose={onClose} sheet>
      {/* eslint-disable-next-line jsx-a11y/no-static-element-interactions -- digits pick a reason, bubbling up from the buttons */}
      <div className="flex flex-col gap-3 px-4 pt-[18px] pb-4" onKeyDown={onKeyDown}>
        <h2 id={titleId} className="m-0 font-display text-[30px] leading-[34px] font-normal">
          {title}
        </h2>
        {reasons.length > 0 && (
          <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
            {reasons.map((reason, i) => (
              <li key={reason}>
                <button
                  type="button"
                  onClick={() => pick(reason)}
                  className="flex min-h-[46px] w-full items-center gap-3 rounded-1 border border-line bg-surface px-3 text-left text-[15px] text-ink hover:bg-surface-2"
                >
                  {i < 9 && <Kbd>{i + 1}</Kbd>}
                  <span className="min-w-0 flex-1 break-words">{reason}</span>
                </button>
              </li>
            ))}
          </ul>
        )}
        <label htmlFor={noteId} className="text-small font-bold">
          {t('inbox.note')}
        </label>
        <input
          id={noteId}
          value={note}
          onChange={(e) => setNote(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && reasons.length === 0) pick(undefined);
          }}
          placeholder={t('inbox.notePlaceholder')}
          className="h-11 rounded-1 border border-control-line bg-bg px-3 text-[16px] text-ink"
        />
        <div className="grid grid-cols-[1fr_1.4fr] gap-2">
          <button type="button" onClick={onClose} className={`${button.secondary} h-12`}>
            {t('common.cancel')}
          </button>
          {reasons.length === 0 ? (
            <button type="button" onClick={() => pick(undefined)} className={`${button.danger} h-12`}>
              {confirm}
              <Kbd>1</Kbd>
            </button>
          ) : (
            <p className="m-0 self-center text-tiny text-ink-3">{t('inbox.pickHint')}</p>
          )}
        </div>
      </div>
    </Dialog>
  );
}
