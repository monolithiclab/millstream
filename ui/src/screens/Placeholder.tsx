import { useTranslation } from 'react-i18next';
import { useParams } from 'react-router-dom';
import type { Screen } from '../routes';
import { ScreenPage } from './ScreenPage';

// Stands in for the screens of M3 Tasks 4 and 5.
export function Placeholder({ screen }: { screen: Screen }) {
  const { t } = useTranslation();
  const { pipeline } = useParams();
  return (
    <ScreenPage
      title={t(screen.label)}
      eyebrow={
        pipeline !== undefined && (
          <p className="m-0 font-mono text-small text-ink-2">{t('screen.inPipeline', { pipeline })}</p>
        )
      }
    >
      <p className="m-0 text-body text-ink-2">{t('screen.placeholder')}</p>
    </ScreenPage>
  );
}
