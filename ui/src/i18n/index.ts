import i18next, { type i18n } from 'i18next';
import { initReactI18next } from 'react-i18next';
import en from './en.json';
import fr from './fr.json';

export const languages = ['en', 'fr'] as const;
export type Language = (typeof languages)[number];

export const catalogues = { en, fr } satisfies Record<Language, unknown>;

declare module 'i18next' {
  interface CustomTypeOptions {
    // t() accepts only keys of the English catalogue; the i18n test keeps French identical in shape.
    resources: { translation: typeof en };
    returnNull: false;
  }
}

export function isLanguage(value: unknown): value is Language {
  return typeof value === 'string' && (languages as readonly string[]).includes(value);
}

// One instance per app (and per test), initialised synchronously from the bundled catalogues.
export function createI18n(language: Language): i18n {
  const instance = i18next.createInstance();
  void instance.use(initReactI18next).init({
    resources: { en: { translation: en }, fr: { translation: fr } },
    lng: language,
    fallbackLng: false,
    initAsync: false,
    interpolation: { escapeValue: false },
  });
  return instance;
}
