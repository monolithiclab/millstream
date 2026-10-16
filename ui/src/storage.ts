// localStorage and sessionStorage can be missing or throw (private windows, blocked site data): preferences then
// last for the session only.
type Kind = 'localStorage' | 'sessionStorage';

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
