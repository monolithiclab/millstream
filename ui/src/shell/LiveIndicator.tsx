import { useTranslation } from 'react-i18next';
import { useNow } from '../common/useNow';
import type { LiveState } from '../live/useLiveStream';

const circle = 'M6 1.5a4.5 4.5 0 1 1 0 9a4.5 4.5 0 1 1 0-9Z';

// Live, or "Reconnecting · 4 s" (dashed, amber) with the seconds to the next attempt.
export function LiveIndicator({ state }: { state: LiveState }) {
  const { t } = useTranslation();
  // Each attempt counts down from its own start: the clock is read when its countdown mounts.
  if (state.status === 'reconnecting') return <Reconnecting key={state.retryAt ?? 0} retryAt={state.retryAt} />;
  const live = state.status === 'live';
  return (
    <span
      role="status"
      className={`inline-flex items-center gap-1.5 text-tiny font-bold ${live ? 'text-accent' : 'text-ink-3'}`}
    >
      <svg width="10" height="10" viewBox="0 0 12 12" aria-hidden="true">
        <path d={circle} fill={live ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth={live ? 0 : 1.6} />
      </svg>
      {live ? t('live.live') : t('live.connecting')}
    </span>
  );
}

function Reconnecting({ retryAt }: { retryAt: number | null }) {
  const { t } = useTranslation();
  const now = useNow(1000);
  const seconds = Math.max(0, Math.ceil(((retryAt ?? now) - now) / 1000));
  return (
    <span
      role="status"
      className="inline-flex items-center gap-1.5 rounded-1 border border-dashed border-warn px-2 py-0.5 text-tiny font-bold text-warn"
    >
      <svg width="10" height="10" viewBox="0 0 12 12" aria-hidden="true">
        <path d={circle} fill="none" stroke="currentColor" strokeWidth="1.6" strokeDasharray="2.4 2" />
      </svg>
      {t('live.reconnecting', { seconds })}
    </span>
  );
}
