import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { QueryState } from '../../api/context';
import { StatusGlyph } from './Status';
import { button } from './styles';

// Loading without layout shift: a quiet status line over placeholder blocks of the content's height.
export function Loading({ lines = 3 }: { lines?: number | undefined }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3" data-testid="loading">
      <p role="status" className="m-0 text-small text-ink-2">
        {t('common.loading')}
      </p>
      {Array.from({ length: lines }, (_, i) => (
        <div key={i} aria-hidden="true" className="h-14 rounded-2 bg-surface-2" />
      ))}
    </div>
  );
}

// The canvas's "empty states that teach": a dashed frame, two channel banks, a title and what to do.
export function EmptyState({ title, children, tick = false }: { title: string; children?: ReactNode; tick?: boolean }) {
  return (
    <div className="flex flex-col gap-2 rounded-2 border border-dashed border-control-line p-[18px]">
      <svg width="64" height="22" viewBox="0 0 64 22" aria-hidden="true">
        <path d="M1 5.5H63M1 16.5H63" className="stroke-channel-bank" strokeWidth="1.6" />
        {tick && (
          <path d="M24 11.4L29 15.5L40 6.5" fill="none" className="stroke-ok" strokeWidth="2" strokeLinecap="round" />
        )}
      </svg>
      <p className="m-0 text-[15px] font-bold">{title}</p>
      {children !== undefined && <div className="text-small text-ink-2">{children}</div>}
    </div>
  );
}

// Something failed: what happened, the server's message, and Retry or Dismiss.
export function ErrorState({
  message,
  title,
  onRetry,
  onDismiss,
}: {
  message: string;
  title?: string;
  onRetry?: () => void;
  onDismiss?: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div
      role="alert"
      className="flex flex-wrap items-start gap-3 rounded-2 border border-bad bg-surface px-3.5 py-2.5 text-small"
    >
      <span className="mt-1 text-bad">
        <StatusGlyph name="open" size={12} />
      </span>
      <span className="min-w-0 flex-1 break-words">
        <strong>{title ?? t('common.loadFailed')}</strong> {message}
      </span>
      {onRetry && (
        <button type="button" onClick={onRetry} className={button.secondary}>
          {t('common.retry')}
        </button>
      )}
      {onDismiss && (
        <button type="button" onClick={onDismiss} className={button.small}>
          {t('common.dismiss')}
        </button>
      )}
    </div>
  );
}

// A read's three states: loading, error with retry, or the data.
export function QueryView<T>({
  state,
  onRetry,
  lines,
  children,
}: {
  state: QueryState<T>;
  onRetry: () => void;
  lines?: number | undefined;
  children: (data: T) => ReactNode;
}) {
  if (state.status === 'loading') return <Loading lines={lines} />;
  if (state.status === 'error') return <ErrorState message={state.error.message} onRetry={onRetry} />;
  return <>{children(state.data)}</>;
}
