// localStorage and sessionStorage can be missing or throw (private windows, blocked site data): preferences then
// last for the session only.
type Kind = 'localStorage' | 'sessionStorage';

// The sessionStorage flag that keeps demo mode on for the tab.
export const DEMO_KEY = 'millstream.demo';

export function readStored(key: string, kind: Kind = 'localStorage'): string | null {
  try {
    return window[kind].getItem(key);
  } catch {
    return null;
  }
}

export function writeStored(key: string, value: string, kind: Kind = 'localStorage'): void {
  try {
    window[kind].setItem(key, value);
  } catch {
    // Storage unavailable: keep the value in memory only.
  }
}

export function removeStored(key: string, kind: Kind = 'localStorage'): void {
  try {
    window[kind].removeItem(key);
  } catch {
    // Storage unavailable: nothing was kept.
  }
}
