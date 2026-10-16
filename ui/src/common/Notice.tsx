import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { SH } from './Status';

export interface Notice {
  kind: 'ok' | 'error';
  message: string;
  retry?: () => void;
}

// The outcome of an action, as the canvas toasts: a dark success line, or an error outlined in red that says
// nothing changed and offers Retry. The polite region is always there, so a success is announced; an error is an
// alert.
export function NoticeRegion({ notice, onDismiss }: { notice: Notice | null; onDismiss: () => void }) {
  const { t } = useTranslation();
  return (
    <div
      aria-live="polite"
      className="pointer-events-none fixed inset-x-4 bottom-4 z-40 flex justify-center md:right-6 md:left-auto"
    >
      {notice?.kind === 'ok' && (
        <div className="pointer-events-auto flex max-w-md items-center gap-3 rounded-2 bg-ink px-3.5 py-2.5 text-small text-bg shadow-2">
          <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true" className="flex-none">
            <path d={SH.check} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          </svg>
          <span className="min-w-0 flex-1">{notice.message}</span>
          <button type="button" onClick={onDismiss} className="font-bold underline">
            {t('common.dismiss')}
          </button>
        </div>
      )}
      {notice?.kind === 'error' && (
        <div
          role="alert"
          className="pointer-events-auto flex max-w-md items-start gap-3 rounded-2 border border-bad bg-surface px-3.5 py-2.5 text-small text-ink shadow-2"
        >
          <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true" className="mt-1 flex-none text-bad">
            <path d={SH.square} fill="currentColor" />
          </svg>
          <span className="min-w-0 flex-1 break-words">{notice.message}</span>
          {notice.retry && (
            <button type="button" onClick={notice.retry} className="font-bold underline">
              {t('common.retry')}
            </button>
          )}
          <button type="button" onClick={onDismiss} className="font-bold underline">
            {t('common.dismiss')}
          </button>
        </div>
      )}
    </div>
  );
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// An action's outcome as a notice: its success message, or its failure (from the error's message) with Retry, which
// runs it again; settled runs after either, to reload what shows the result.
export async function runAction(
  setNotice: (notice: Notice | null) => void,
  action: {
    // Does it and says what was done.
    call: () => Promise<string>;
    failed: (message: string) => string;
    settled?: () => void;
  },
): Promise<void> {
  const { call, failed, settled } = action;
  const again = () => runAction(setNotice, action);
  setNotice(null);
  try {
    setNotice({ kind: 'ok', message: await call() });
  } catch (error) {
    setNotice({ kind: 'error', message: failed(errorMessage(error)), retry: () => void again() });
  } finally {
    settled?.();
  }
}

export function useNotice() {
  const [notice, setNotice] = useState<Notice | null>(null);
  const dismiss = useCallback(() => setNotice(null), []);
  return { notice, setNotice, dismiss };
}
