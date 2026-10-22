import { Fragment } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { pipelinePath } from '../routes';

export interface Crumb {
  label: string;
  to?: string;
  mono?: boolean;
}

// The first crumb of every pipeline screen: the pipeline's name, linking to its graph.
export const pipelineCrumb = (pipeline: string): Crumb => ({ label: pipeline, to: pipelinePath(pipeline), mono: true });

// A screen's parents, above its title: links separated by `/`, pipeline names in mono.
export function Breadcrumb({ items }: { items: Crumb[] }) {
  const { t } = useTranslation();
  return (
    <nav aria-label={t('screen.breadcrumb')} className="flex flex-wrap items-center gap-2 text-small text-ink-2">
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
