import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { usePreferences } from '../preferences';
import { Dialog } from './Dialog';
import { Kbd } from './Kbd';

export function ShortcutHelp({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const { singleKeys, setSingleKeys } = usePreferences();
  const titleId = useId();
  const toggleId = useId();
  const rows: [string[], string][] = [
    [['⌘K', 'Ctrl+K'], t('shortcuts.palette')],
    [['g', '…'], t('shortcuts.goTo')],
    [['?'], t('shortcuts.help')],
    [[t('shortcuts.esc')], t('shortcuts.close')],
  ];
  return (
    <Dialog labelledBy={titleId} onClose={onClose}>
      <div className="flex flex-col gap-4 p-6">
        <div className="flex items-start gap-4">
          <h2 id={titleId} className="flex-1 font-display text-figure">
            {t('shortcuts.title')}
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-1 border border-control-line px-2.5 py-1 text-small font-bold text-ink"
          >
            {t('dialog.close')}
          </button>
        </div>
        <h3 className="text-tiny font-bold tracking-wide text-ink-3 uppercase">{t('shortcuts.everywhere')}</h3>
        <dl className="grid grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-4 gap-y-2.5">
          {rows.map(([keys, label]) => (
            <div key={label} className="contents">
              <dt className="flex flex-wrap gap-1">
                {keys.map((key) => (
                  <Kbd key={key}>{key}</Kbd>
                ))}
              </dt>
              <dd className="m-0 text-ui">{label}</dd>
            </div>
          ))}
        </dl>
        <div className="flex items-start gap-3 border-t border-line pt-4">
          <input
            id={toggleId}
            type="checkbox"
            checked={singleKeys}
            onChange={(event) => setSingleKeys(event.target.checked)}
            aria-describedby={`${toggleId}-hint`}
            className="mt-1 size-4 accent-accent"
          />
          <div className="flex flex-col">
            <label htmlFor={toggleId} className="font-bold">
              {t('shortcuts.singleKeys')}
            </label>
            <p id={`${toggleId}-hint`} className="m-0 text-small text-ink-2">
              {t('shortcuts.singleKeysHint')}
            </p>
          </div>
        </div>
      </div>
    </Dialog>
  );
}
