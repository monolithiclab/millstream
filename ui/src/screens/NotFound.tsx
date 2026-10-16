import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { ScreenPage } from './ScreenPage';

export function NotFound() {
  const { t } = useTranslation();
  return (
    <ScreenPage title={t('screen.notFound.title')}>
      <p className="m-0 text-body text-ink-2">{t('screen.notFound.body')}</p>
      <p className="m-0">
        <Link to="/pipelines" className="font-bold">
          {t('screen.notFound.back')}
        </Link>
      </p>
    </ScreenPage>
  );
}
