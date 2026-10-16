import { useTranslation } from 'react-i18next';
import { field } from './styles';
import { useResource } from './useResource';

// A pipeline filter: every pipeline, or "All pipelines" for the empty value.
export function PipelineSelect({
  value,
  onChange,
  className = '',
}: {
  value: string;
  onChange: (pipeline: string) => void;
  className?: string;
}) {
  const { t } = useTranslation();
  const pipelines = useResource('pipelines', (api) => api.listPipelines());
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)} className={`${field} ${className}`}>
      <option value="">{t('switcher.allPipelines')}</option>
      {(pipelines.status === 'ready' ? pipelines.data : []).map((p) => (
        <option key={p.name} value={p.name}>
          {p.name}
        </option>
      ))}
    </select>
  );
}
