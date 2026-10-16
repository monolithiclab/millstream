import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { I18nextProvider } from 'react-i18next';
import { createI18n, isLanguage, type Language } from './i18n';
import { readStored, writeStored } from './storage';

export const themes = ['system', 'light', 'dark'] as const;
export type Theme = (typeof themes)[number];

export const storageKeys = {
  language: 'millstream.language',
  theme: 'millstream.theme',
  singleKeys: 'millstream.singleKeys',
} as const;

function isTheme(value: unknown): value is Theme {
  return typeof value === 'string' && (themes as readonly string[]).includes(value);
}

export function initialLanguage(): Language {
  const stored = readStored(storageKeys.language);
  if (isLanguage(stored)) return stored;
  return navigator.language.toLowerCase().startsWith('fr') ? 'fr' : 'en';
}

export function initialTheme(): Theme {
  const stored = readStored(storageKeys.theme);
  return isTheme(stored) ? stored : 'system';
}

// The theme is the color-scheme of <html>: no data-theme follows the system, data-theme="light|dark" overrides it.
export function applyTheme(theme: Theme): void {
  if (theme === 'system') document.documentElement.removeAttribute('data-theme');
  else document.documentElement.setAttribute('data-theme', theme);
}

interface Preferences {
  language: Language;
  setLanguage: (language: Language) => void;
  theme: Theme;
  setTheme: (theme: Theme) => void;
  // Single-key shortcuts can be turned off (WCAG 2.1.4); ⌘K stays.
  singleKeys: boolean;
  setSingleKeys: (on: boolean) => void;
}

const PreferencesContext = createContext<Preferences | null>(null);

export function PreferencesProvider({
  children,
  language: startLanguage,
  theme: startTheme,
}: {
  children: ReactNode;
  language?: Language | undefined;
  theme?: Theme | undefined;
}) {
  const [language, setLanguageState] = useState(() => startLanguage ?? initialLanguage());
  const [theme, setThemeState] = useState(() => startTheme ?? initialTheme());
  const [singleKeys, setSingleKeysState] = useState(() => readStored(storageKeys.singleKeys) !== 'off');
  const [i18n] = useState(() => createI18n(language));

  useEffect(() => {
    document.documentElement.lang = language;
    void i18n.changeLanguage(language);
  }, [i18n, language]);

  useEffect(() => applyTheme(theme), [theme]);

  const value = useMemo<Preferences>(
    () => ({
      language,
      setLanguage: (next) => {
        writeStored(storageKeys.language, next);
        setLanguageState(next);
      },
      theme,
      setTheme: (next) => {
        writeStored(storageKeys.theme, next);
        setThemeState(next);
      },
      singleKeys,
      setSingleKeys: (on) => {
        writeStored(storageKeys.singleKeys, on ? 'on' : 'off');
        setSingleKeysState(on);
      },
    }),
    [language, theme, singleKeys],
  );

  return (
    <PreferencesContext.Provider value={value}>
      <I18nextProvider i18n={i18n}>{children}</I18nextProvider>
    </PreferencesContext.Provider>
  );
}

export function usePreferences(): Preferences {
  const value = useContext(PreferencesContext);
  if (value === null) throw new Error('usePreferences outside PreferencesProvider');
  return value;
}
