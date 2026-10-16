import { createContext, useContext, type ReactNode } from 'react';
import { NoticeRegion, useNotice, type Notice } from '../common/Notice';

const ShellNoticeContext = createContext<(notice: Notice | null) => void>(() => undefined);

// A notice owned by the app shell, for outcomes that arrive after their screen has gone: a review decision sent as
// the inbox closed that the server then refused.
export function ShellNoticeProvider({ children }: { children: ReactNode }) {
  const { notice, setNotice, dismiss } = useNotice();
  return (
    <ShellNoticeContext.Provider value={setNotice}>
      {children}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ShellNoticeContext.Provider>
  );
}

export const useShellNotice = () => useContext(ShellNoticeContext);
