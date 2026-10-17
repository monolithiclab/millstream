import { useTranslation } from 'react-i18next';
import type { Session } from '../api/schema';

// The deployment's environment (spec section 11.2), from the session: production is where data is, and where the
// improve pipeline never runs; development is where changes are made. Nothing shows until the session is known.
export function EnvironmentBadge({ session }: { session: Session | null }) {
  const { t } = useTranslation();
  if (session === null) return null;
  const production = session.environment === 'production';
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
