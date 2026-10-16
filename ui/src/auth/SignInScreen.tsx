import { useEffect, useId, useState, type FormEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { ApiError, type Api } from '../api/api';
import type { Session } from '../api/schema';
import { buttons, card, field } from '../common/styles';
import { Logo } from '../shell/Logo';

// The sign-in screen of a server with a token: the token for a session cookie (spec section 12.1). The token is
// sent once and kept nowhere in the page.
export function SignInScreen({ api, onSignedIn }: { api: Api; onSignedIn: (session: Session) => void }) {
  const { t } = useTranslation();
  const [token, setToken] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const errorId = useId();
  const title = t('signIn.title');

  useEffect(() => {
    document.title = t('app.pageTitle', { page: title });
  }, [t, title]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const session = await api.signIn(token);
      setToken('');
      onSignedIn(session);
    } catch (e) {
      setBusy(false);
      setError(
        e instanceof ApiError && e.status === 401
          ? t('signIn.wrongToken')
          : t('signIn.failed', { message: e instanceof Error ? e.message : String(e) }),
      );
    }
  };

  return (
    <main className="flex min-h-dvh items-center justify-center bg-bg px-4 py-10 text-ink">
      <form
        onSubmit={(e) => void submit(e)}
        aria-labelledby="sign-in-title"
        className={`${card} flex w-full max-w-[400px] flex-col gap-4 p-6`}
      >
        <div className="flex items-center gap-2.5">
          <Logo />
          <span className="font-display text-[26px] leading-[26px] tracking-[-0.01em]">{t('app.name')}</span>
        </div>
        <h1 id="sign-in-title" className="m-0 font-display text-[32px] leading-9 font-normal">
          {title}
        </h1>
        <p className="m-0 text-small text-ink-2">{t('signIn.explain')}</p>
        <label className="flex flex-col gap-1 text-small font-bold">
          {t('signIn.token')}
          <input
            type="password"
            name="token"
            autoComplete="current-password"
            required
            value={token}
            onChange={(e) => setToken(e.target.value)}
            aria-invalid={error !== null}
            aria-describedby={error === null ? undefined : errorId}
            className={`${field} font-normal`}
          />
        </label>
        {error !== null && (
          <p id={errorId} role="alert" className="m-0 text-small text-bad">
            {error}
          </p>
        )}
        <button type="submit" disabled={busy || token === ''} className={buttons.primary}>
          {busy ? t('signIn.signingIn') : t('signIn.submit')}
        </button>
      </form>
    </main>
  );
}
