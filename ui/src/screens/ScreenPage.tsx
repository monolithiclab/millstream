import { useEffect, useRef, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import { HelpTip } from '../common/HelpTip';

// The first screen of a page load keeps focus where the browser put it; after that, each navigation focuses the
// new screen's heading so screen readers announce it.
let loaded = false;

// Every screen's frame, one anatomy top to bottom: the context line (its parents), the title row (h1, help toggle,
// status, then the actions at the right end), the summary line (figures), the toolbar (state tabs, filters,
// secondary actions), then the content. It also sets the document title and moves focus on navigation.
// `eyebrow` and `header` are the free-form slots the screens used before the anatomy; they go once all migrate.
export function ScreenPage({
  title,
  context,
  help,
  status,
  actions,
  summary,
  toolbar,
  eyebrow,
  header,
  children,
}: {
  title: string;
  context?: ReactNode;
  help?: ReactNode;
  status?: ReactNode;
  actions?: ReactNode;
  summary?: ReactNode;
  toolbar?: ReactNode;
  eyebrow?: ReactNode;
  header?: ReactNode;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const { pathname } = useLocation();
  const heading = useRef<HTMLHeadingElement>(null);
  const documentTitle = t('app.pageTitle', { page: title });

  useEffect(() => {
    document.title = documentTitle;
  }, [documentTitle]);

  useEffect(() => {
    if (loaded) heading.current?.focus();
    loaded = true;
  }, [pathname]);

  const h1 = (
    <h1
      ref={heading}
      id="page-title"
      tabIndex={-1}
      className="m-0 font-display text-title font-normal focus-visible:outline-none"
    >
      {title}
    </h1>
  );
  const row = help !== undefined || status !== undefined || actions !== undefined || header !== undefined;
  return (
    <section aria-labelledby="page-title" className="flex flex-col gap-2">
      {context ?? eyebrow}
      {row ? (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
          <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-2">
            {h1}
            {help !== undefined && <HelpTip>{help}</HelpTip>}
            {status}
          </div>
          {header}
          {actions !== undefined && (
            <div className="ml-auto flex max-w-full min-w-0 flex-wrap items-center gap-2">{actions}</div>
          )}
        </div>
      ) : (
        h1
      )}
      {summary !== undefined && <div className="text-small text-ink-2">{summary}</div>}
      {toolbar !== undefined && <div className="flex flex-wrap items-end gap-x-4 gap-y-2.5">{toolbar}</div>}
      {children}
    </section>
  );
}

// For tests: the next screen counts as the first of a page load.
export function resetLoaded(): void {
  loaded = false;
}
