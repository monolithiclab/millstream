import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router-dom';
import { liveUrls } from '../../api/api';
import { LogTail } from '../../common/LogTail';
import { PipelineSelect } from '../../common/PipelineSelect';
import { field } from '../../common/styles';
import { ScreenPage } from '../ScreenPage';

const scopes = ['run', 'step', 'item'] as const;
type Scope = (typeof scopes)[number];

// The live log tail (/live/logs), filtered by pipeline, run, step and item in the URL.
export function LogsScreen() {
  const { t } = useTranslation();
  const [search, setSearch] = useSearchParams();
  const ids = useId();
  const filter = {
    pipeline: search.get('pipeline') ?? '',
    run: search.get('run') ?? '',
    step: search.get('step') ?? '',
    item: search.get('item') ?? '',
  };
  const url = liveUrls.logs({
    ...(filter.pipeline && { pipeline: filter.pipeline }),
    ...(filter.run && { run: filter.run }),
    ...(filter.step && { step: filter.step }),
    ...(filter.item && { item: filter.item }),
  });

  const setFilter = (name: keyof typeof filter, value: string) => {
    const next = new URLSearchParams();
    for (const [key, v] of Object.entries({ ...filter, [name]: value.trim() })) if (v) next.set(key, v);
    setSearch(next);
  };
  const scopeLabels: Record<Scope, string> = { run: t('logs.run'), step: t('logs.step'), item: t('logs.item') };
  const toolbar = (
    <>
      <label className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
        {t('logs.pipeline')}
        <PipelineSelect value={filter.pipeline} onChange={(p) => setFilter('pipeline', p)} className="font-normal" />
      </label>
      {scopes.map((scope) => (
        <label key={scope} htmlFor={`${ids}-${scope}`} className="flex flex-col gap-1 text-tiny font-bold text-ink-2">
          {scopeLabels[scope]}
          <input
            id={`${ids}-${scope}`}
            key={`${scope}-${filter[scope]}`}
            defaultValue={filter[scope]}
            placeholder={t('logs.any')}
            onBlur={(e) => e.currentTarget.value.trim() !== filter[scope] && setFilter(scope, e.currentTarget.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') setFilter(scope, e.currentTarget.value);
            }}
            className={`${field} min-h-[34px] w-40 font-mono font-normal`}
          />
        </label>
      ))}
    </>
  );
  return (
    <ScreenPage title={t('nav.logs')} toolbar={toolbar}>
      <LogTail url={url} title={t('logs.tail')} className="h-[min(60vh,560px)]" />
    </ScreenPage>
  );
}
