import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { SH } from './Status';
import { buttons } from './styles';
import type { Query } from './useResource';

// Loading without layout shift: blocks in the shape of what loads, so nothing moves when it arrives, and a status
// word for screen readers.
export function Loading({
  rows = 3,
  className = 'h-14',
}: {
  rows?: number | undefined;
  className?: string | undefined;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3" aria-busy="true" data-testid="loading">
      <span className="sr-only">{t('common.loading')}</span>
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} aria-hidden="true" className={`rounded-2 bg-sunken ${className}`} />
      ))}
    </div>
  );
}

// Something failed: what (by default, that it could not be loaded), the server's message, and Retry or Dismiss.
export function ErrorState({
  title,
  message,
  onRetry,
  onDismiss,
}: {
  title?: string | undefined;
  message: string;
  onRetry?: () => void;
  onDismiss?: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div role="alert" className="flex flex-wrap items-start gap-3 rounded-2 border border-bad bg-surface px-4 py-3">
      <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true" className="mt-1 flex-none text-bad">
        <path d={SH.square} fill="currentColor" />
      </svg>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <strong className="text-ui">{title ?? t('common.loadFailed')}</strong>{' '}
        <span className="text-small break-words text-ink-2">{message}</span>
      </div>
      {onRetry && (
        <button type="button" onClick={onRetry} className={buttons.secondary}>
          {t('common.retry')}
        </button>
      )}
      {onDismiss && (
        <button type="button" onClick={onDismiss} className={buttons.smallSecondary}>
          {t('common.dismiss')}
        </button>
      )}
    </div>
  );
}

// Empty states teach (Components board): a dashed frame, two channel banks, what is missing and how it comes; done
// draws the quiet success mark.
export function EmptyState({ title, children, done }: { title: string; children?: ReactNode; done?: boolean }) {
  return (
    <div className="flex flex-col gap-2 rounded-2 border border-dashed border-control-line p-[18px]">
      <svg width="64" height="22" viewBox="0 0 64 22" aria-hidden="true">
        <path d="M1 5.5H63M1 16.5H63" stroke="var(--ms-channel-bank)" strokeWidth="1.6" />
        {done && (
          <path d="M24 11.4L29 15.5L40 6.5" fill="none" stroke="var(--ms-ok)" strokeWidth="2" strokeLinecap="round" />
        )}
      </svg>
      <strong className="text-body font-bold">{title}</strong>
      {children !== undefined && <div className="flex flex-col gap-2 text-small text-ink-2">{children}</div>}
    </div>
  );
}

// A read's three states: loading, the error with Retry, or the data.
export function QueryView<T>({
  query,
  title,
  rows,
  className,
  children,
}: {
  query: Query<T>;
  // What failed, over the server's message.
  title?: string;
  rows?: number;
  className?: string;
  children: (data: T) => ReactNode;
}) {
  if (query.status === 'loading') return <Loading rows={rows} className={className} />;
  if (query.status === 'error')
    return <ErrorState title={title} message={query.error.message} onRetry={query.reload} />;
  return <>{children(query.data)}</>;
}
