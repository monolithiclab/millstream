import { useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { usePipelines } from '../common/usePipelines';
import { pipelinePath } from '../routes';
import { useDismiss } from './useDismiss';

// A disclosure listing every pipeline: picking one opens its graph, "All pipelines" the list. Opening it reads the
// list again, so a pipeline applied since shows up.
export function PipelineSwitcher({ current }: { current: string | undefined }) {
  const { t } = useTranslation();
  const pipelines = usePipelines();
  const [open, setOpen] = useState(false);
  const button = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const panelId = useId();
  const name = current ?? t('switcher.allPipelines');

  useDismiss(open, () => setOpen(false), { inside: [panel, button], returnFocus: button });

  const close = () => setOpen(false);

  return (
    <div className="relative">
      <button
        ref={button}
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        aria-label={t('switcher.label', { name })}
        onClick={() => {
          if (!open) pipelines.refresh();
          setOpen(!open);
        }}
        className={`flex w-full items-center gap-2 rounded-1 border bg-surface px-2.5 py-2 text-left text-small font-bold text-ink ${current ? 'border-line font-mono' : 'border-ink'}`}
      >
        <span className="min-w-0 flex-1 truncate">{name}</span>
        <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
          <path d="M3 4.5L6 7.5L9 4.5" fill="none" stroke="currentColor" strokeWidth="1.5" className="text-ink-2" />
        </svg>
      </button>
      {open && (
        <div
          ref={panel}
          id={panelId}
          role="group"
          aria-label={t('switcher.list')}
          className="absolute inset-x-0 top-full z-40 mt-1 rounded-2 border border-control-line bg-surface p-1.5 shadow-2"
        >
          <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
            <li>
              <Link
                to="/pipelines"
                onClick={close}
                className="block rounded-1 px-2.5 py-1.5 text-small font-bold text-ink no-underline hover:bg-surface-2"
              >
                {t('switcher.allPipelines')}
              </Link>
            </li>
            {pipelines.status === 'ready' &&
              pipelines.data.map((pipeline) => (
                <li key={pipeline.name}>
                  <Link
                    to={pipelinePath(pipeline.name)}
                    onClick={close}
                    aria-current={pipeline.name === current ? 'true' : undefined}
                    className="block rounded-1 px-2.5 py-1.5 font-mono text-small text-ink no-underline hover:bg-surface-2 aria-[current]:bg-accent-soft"
                  >
                    {pipeline.name}
                  </Link>
                </li>
              ))}
          </ul>
          {pipelines.status === 'loading' && (
            <p className="m-0 px-2.5 py-1.5 text-small text-ink-2">{t('switcher.loading')}</p>
          )}
          {pipelines.status === 'error' && (
            <p role="alert" className="m-0 px-2.5 py-1.5 text-small text-bad">
              {t('switcher.error')}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
