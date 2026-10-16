import { useTranslation } from 'react-i18next';
import { DEMO_KEY, removeStored } from '../storage';

// Leaves demo mode: forgets the flag and reloads without ?demo, onto the server's API.
export function exitDemo(location: Pick<Location, 'href' | 'assign'> = window.location): void {
  removeStored(DEMO_KEY, 'sessionStorage');
  const url = new URL(location.href);
  url.searchParams.delete('demo');
  location.assign(url.toString());
}

// Always in the header while the UI runs on demo data, so nobody mistakes it for a pipeline's real state.
export function DemoBadge({ onExit = () => exitDemo() }: { onExit?: () => void }) {
  const { t } = useTranslation();
  return (
    <p className="m-0 inline-flex items-center gap-2 rounded-pill border border-warn px-2.5 py-0.5 text-tiny font-bold text-warn">
      <span>{t('shell.demo')}</span>
      <button type="button" onClick={onExit} className="font-bold text-ink underline underline-offset-2">
        {t('shell.exitDemo')}
      </button>
    </p>
  );
}
