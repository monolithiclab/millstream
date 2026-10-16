import { createContext, Fragment, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { ApiError, type Api } from '../api/api';
import type { Session } from '../api/schema';
import { SignInScreen } from './SignInScreen';

interface SessionValue {
  // Null until the server says, or when it could not.
  session: Session | null;
  signOut: () => Promise<void>;
}

const SessionContext = createContext<SessionValue>({ session: null, signOut: () => Promise.resolve() });

// The session of a server with a token (spec section 12.1), and how to end it.
export const useSession = () => useContext(SessionContext);

// Wraps every method of api so that an unauthorized answer, of any read or action, calls onUnauthorized.
export function guardApi(api: Api, onUnauthorized: () => void): Api {
  return new Proxy(api, {
    get(target, prop, receiver) {
      const value: unknown = Reflect.get(target, prop, receiver);
      if (typeof value !== 'function') return value;
      return (...args: unknown[]) => {
        const result: unknown = (value as (...a: unknown[]) => unknown).apply(target, args);
        if (!(result instanceof Promise)) return result;
        return result.catch((error: unknown) => {
          if (error instanceof ApiError && error.status === 401) onUnauthorized();
          throw error;
        });
      };
    },
  });
}

// Shows the sign-in screen whenever the API answers unauthorized, and the app otherwise: a server without a token
// never does. Signing in remounts the app, so every screen reads again; an unauthorized answer to a request of an
// earlier session does not sign the new one out.
export function SessionGate({ api, children }: { api: Api; children: (api: Api) => ReactNode }) {
  const [auth, setAuth] = useState({ epoch: 0, signedIn: true });
  const [session, setSession] = useState<Session | null>(null);
  const { epoch } = auth;

  const guarded = useMemo(
    () => guardApi(api, () => setAuth((a) => (a.epoch === epoch ? { epoch, signedIn: false } : a))),
    [api, epoch],
  );

  useEffect(() => {
    let live = true;
    guarded.getSession().then(
      (s) => {
        if (live) setSession(s);
      },
      () => undefined, // unauthorized signs out through the guard; any other failure leaves the screens to say
    );
    return () => {
      live = false;
    };
  }, [guarded]);

  const onSignedIn = useCallback((s: Session) => {
    setSession(s);
    setAuth((a) => ({ epoch: a.epoch + 1, signedIn: true }));
  }, []);

  // Signed out here even when the server cannot be told: the session then ends when its cookie expires.
  const signOut = useCallback(async () => {
    await api.signOut().catch(() => undefined);
    setSession(null);
    setAuth((a) => ({ ...a, signedIn: false }));
  }, [api]);

  const value = useMemo(() => ({ session, signOut }), [session, signOut]);

  if (!auth.signedIn) return <SignInScreen api={api} onSignedIn={onSignedIn} />;
  return (
    <SessionContext.Provider value={value}>
      <Fragment key={epoch}>{children(guarded)}</Fragment>
    </SessionContext.Provider>
  );
}
