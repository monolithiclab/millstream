import { useEffect, useRef, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';

// The first screen of a page load keeps focus where the browser put it; after that, each navigation focuses the
// new screen's heading so screen readers announce it.
let loaded = false;

// Every screen's frame: its h1, the document title, and focus on navigation. `header` sits beside the h1 (state,
// figures, filters, actions) and wraps under it on narrow screens.
export function ScreenPage({
  title,
  eyebrow,
  header,
  children,
}: {
  title: string;
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
  return (
    <section aria-labelledby="page-title" className="flex flex-col gap-2">
      {eyebrow}
      {header === undefined ? (
        h1
      ) : (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
          {h1}
          {header}
        </div>
      )}
      {children}
    </section>
  );
}

// For tests: the next screen counts as the first of a page load.
export function resetLoaded(): void {
  loaded = false;
}
