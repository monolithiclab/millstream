import { useTranslation } from 'react-i18next';
import { Select } from './Select';
import { usePipelines } from './usePipelines';

// A pipeline filter: every pipeline, or "All pipelines" for the empty value. `className` sizes the wrapper.
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
  const pipelines = usePipelines();
  return (
    <Select value={value} onChange={(e) => onChange(e.target.value)} className={className}>
      <option value="">{t('switcher.allPipelines')}</option>
      {(pipelines.status === 'ready' ? pipelines.data : []).map((p) => (
        <option key={p.name} value={p.name}>
          {p.name}
        </option>
      ))}
    </Select>
  );
}
