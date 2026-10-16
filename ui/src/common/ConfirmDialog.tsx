import { useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog } from '../shell/Dialog';
import { buttons } from './styles';

// A confirmation per the interaction notes: the body states the consequence, the button repeats the verb and the
// count ("Replay 2 items"). A destructive dialog opens on Cancel, any other on its primary action. The confirm button
// stays busy until onConfirm settles; the caller closes the dialog and reports the outcome.
export function ConfirmDialog({
  title,
  children,
  confirmLabel,
  cancelLabel,
  destructive = false,
  onConfirm,
  onCancel,
}: {
  title: string;
  children: ReactNode;
  confirmLabel: string;
  // When "Cancel" would be ambiguous (cancelling a run): what keeping things as they are means.
  cancelLabel?: string;
  destructive?: boolean;
  onConfirm: () => Promise<unknown>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const id = useId();
  const [busy, setBusy] = useState(false);
  const confirm = () => {
    setBusy(true);
    void onConfirm().finally(() => setBusy(false));
  };
  return (
    <Dialog labelledBy={id} onClose={busy ? () => undefined : onCancel}>
      <div className="flex flex-col gap-3.5 px-6 pt-[22px] pb-5">
        <h2 id={id} className="m-0 font-display text-[32px] leading-9 font-normal text-balance">
          {title}
        </h2>
        <div className="flex flex-col gap-2 text-ui text-ink-2">{children}</div>
        <div className="mt-1 flex flex-wrap justify-end gap-2">
          <button type="button" className={buttons.secondary} onClick={onCancel} disabled={busy}>
            {cancelLabel ?? t('common.cancel')}
          </button>
          <button
            type="button"
            className={destructive ? buttons.danger : buttons.primary}
            onClick={confirm}
            disabled={busy}
            aria-busy={busy}
            {...(destructive ? {} : { 'data-autofocus': true })}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </Dialog>
  );
}

// The consequences as a list: one sentence each.
export function Consequences({ children }: { children: ReactNode }) {
  return <ul className="m-0 flex list-disc flex-col gap-1.5 pl-[18px]">{children}</ul>;
}
