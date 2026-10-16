import { describe, expect, it, vi } from 'vitest';
import { initialLanguage, initialTheme } from './preferences';

describe('initial preferences', () => {
  it('take the stored language, else the browser language', () => {
    vi.spyOn(navigator, 'language', 'get').mockReturnValue('fr-FR');
    expect(initialLanguage()).toBe('fr');
    window.localStorage.setItem('millstream.language', 'en');
    expect(initialLanguage()).toBe('en');
    window.localStorage.setItem('millstream.language', 'de');
    vi.spyOn(navigator, 'language', 'get').mockReturnValue('de-DE');
    expect(initialLanguage()).toBe('en');
  });

  it('take the stored theme, else the system', () => {
    expect(initialTheme()).toBe('system');
    window.localStorage.setItem('millstream.theme', 'dark');
    expect(initialTheme()).toBe('dark');
    window.localStorage.setItem('millstream.theme', 'sepia');
    expect(initialTheme()).toBe('system');
  });
});
