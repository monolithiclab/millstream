import { useTranslation } from 'react-i18next';
import { useResource } from '../common/useResource';

// The deployment's environment (spec section 11.2), from the engine's status: production is where data is, and
// where the improve pipeline never runs; development is where changes are made. Nothing shows until it is known.
export function EnvironmentBadge() {
  const { t } = useTranslation();
  const nodes = useResource('environment', (api) => api.listNodes());
  if (nodes.status !== 'ready') return null;
  const production = nodes.data.engine.environment === 'production';
  return (
    <p
      className={`m-0 inline-flex items-center rounded-pill border px-2.5 py-0.5 text-tiny font-bold ${
        production ? 'border-ink text-ink' : 'border-line-strong text-ink-2'
      }`}
    >
      {production ? t('shell.production') : t('shell.development')}
    </p>
  );
}
