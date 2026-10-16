import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { SH } from './Status';
import { buttons } from './buttons';

// Grey blocks in the shape of what loads, so nothing shifts when it arrives; a status word for screen readers.
export function Skeleton({ rows = 3, className = 'h-16' }: { rows?: number; className?: string }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3" aria-busy="true" data-testid="skeleton">
      <span className="sr-only">{t('kit.loading')}</span>
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} aria-hidden="true" className={`rounded-2 bg-sunken ${className}`} />
      ))}
    </div>
  );
}

// What failed, the server's message, and Retry.
export function ErrorState({ title, error, onRetry }: { title: string; error: Error; onRetry?: () => void }) {
  const { t } = useTranslation();
  return (
    <div role="alert" className="flex flex-wrap items-start gap-3 rounded-2 border border-bad bg-surface px-4 py-3">
      <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true" className="mt-1 flex-none text-bad">
        <path d={SH.square} fill="currentColor" />
      </svg>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <strong className="text-ui">{title}</strong>
        <span className="text-small break-words text-ink-2">{error.message}</span>
      </div>
      {onRetry && (
        <button type="button" onClick={onRetry} className={buttons.secondary}>
          {t('kit.retry')}
        </button>
      )}
    </div>
  );
}

// Empty states teach: what is missing and how it comes (Components board); done draws the quiet success mark.
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
