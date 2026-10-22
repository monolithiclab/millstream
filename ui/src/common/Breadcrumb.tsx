import { Fragment } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';

// A screen's parents, above its title: links separated by `/`, pipeline names in mono.
export function Breadcrumb({ items }: { items: { label: string; to?: string; mono?: boolean }[] }) {
  const { t } = useTranslation();
  return (
    <nav aria-label={t('item.breadcrumb')} className="flex flex-wrap items-center gap-2 text-small text-ink-2">
      {items.map(({ label, to, mono }, i) => (
        <Fragment key={i}>
          {i > 0 && (
            <span aria-hidden="true" className="text-ink-3">
              /
            </span>
          )}
          {to === undefined ? (
            <span className={mono ? 'font-mono' : undefined}>{label}</span>
          ) : (
            <Link to={to} className={`text-ink-2 no-underline hover:underline ${mono ? 'font-mono' : ''}`}>
              {label}
            </Link>
          )}
        </Fragment>
      ))}
    </nav>
  );
}
