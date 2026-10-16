import { useCallback, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { NavLink, Outlet, useMatch, useNavigate } from 'react-router-dom';
import { useResource } from '../common/useResource';
import { liveUrls } from '../api/api';
import { useDemo } from '../api/context';
import { languages, type Language } from '../i18n';
import { useLiveStream } from '../live/useLiveStream';
import { themes, usePreferences, type Theme } from '../preferences';
import { pipelinePath, pipelineScreens, screenPath, screens, workspaceScreens, type Screen } from '../routes';
import { CommandPalette, type Command } from './CommandPalette';
import { DemoBadge } from './DemoBadge';
import { Kbd } from './Kbd';
import { LiveIndicator } from './LiveIndicator';
import { PipelineSwitcher } from './PipelineSwitcher';
import { ShortcutHelp } from './ShortcutHelp';
import { useDismiss } from './useDismiss';
import { useShortcuts } from './useShortcuts';

const languageNames: Record<Language, 'language.en' | 'language.fr'> = { en: 'language.en', fr: 'language.fr' };

// Written out so the i18n test sees each key as a quoted literal.
const themeNames: Record<Theme, 'theme.system' | 'theme.light' | 'theme.dark'> = {
  system: 'theme.system',
  light: 'theme.light',
  dark: 'theme.dark',
};
const themeCommands: Record<Theme, 'palette.useSystem' | 'palette.useLight' | 'palette.useDark'> = {
  system: 'palette.useSystem',
  light: 'palette.useLight',
  dark: 'palette.useDark',
};

const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform);

function Logo() {
  return (
    <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
      <path
        d="M1 6C6 3.5 11 8.5 21 5.5M1 16.5C6 14 11 19 21 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <circle cx="7" cy="11.2" r="1.9" className="fill-accent" opacity="0.45" />
      <circle cx="14" cy="11" r="2.3" className="fill-accent" />
    </svg>
  );
}

function NavGroup({
  title,
  items,
  pipeline,
  onNavigate,
}: {
  title: string;
  items: Screen[];
  pipeline: string | undefined;
  onNavigate: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-0.5">
      <h2 className="px-2.5 pb-1.5 font-display text-[17px] leading-6 font-normal text-ink-3 italic">{title}</h2>
      <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
        {items.map((screen) => (
          <li key={screen.id}>
            <NavLink
              to={screenPath(screen, pipeline)}
              onClick={onNavigate}
              className="flex items-center gap-2 rounded-1 px-2.5 py-[7px] font-medium text-ink-2 no-underline hover:text-ink aria-[current=page]:bg-surface aria-[current=page]:font-bold aria-[current=page]:text-ink aria-[current=page]:shadow-[inset_0_0_0_1px_var(--ms-line)]"
            >
              {t(screen.label)}
            </NavLink>
          </li>
        ))}
      </ul>
    </div>
  );
}

const selectClass = 'w-full rounded-1 border border-control-line bg-surface px-2 py-1 text-small text-ink';

// The frame around every screen: skip link, main navigation, header with the live mark and the palette, the
// command palette and the shortcut help.
export function AppShell() {
  const { t } = useTranslation();
  const prefs = usePreferences();
  const navigate = useNavigate();
  const pipeline = useMatch('/p/:pipeline/*')?.params.pipeline;
  const pipelines = useResource('pipelines', (api) => api.listPipelines());
  const demo = useDemo();
  const live = useLiveStream(pipeline === undefined ? null : liveUrls.pipeline(pipeline));
  const [palette, setPalette] = useState(false);
  const [help, setHelp] = useState(false);
  const [menu, setMenu] = useState(false);
  const nav = useRef<HTMLElement>(null);
  const menuButton = useRef<HTMLButtonElement>(null);
  useDismiss(menu, () => setMenu(false), { inside: [nav, menuButton], returnFocus: menuButton });

  const go = useCallback((screen: Screen) => navigate(screenPath(screen, pipeline)), [navigate, pipeline]);

  useShortcuts(
    {
      openPalette: () => setPalette(true),
      openHelp: () => setHelp(true),
      go: (key) => {
        const screen = screens.find((s) => s.goKey === key);
        if (screen) go(screen);
      },
    },
    prefs.singleKeys,
  );

  const commands = useMemo<Command[]>(() => {
    const goTo = t('palette.goTo');
    const action = t('palette.action');
    const otherLanguage: Language = prefs.language === 'en' ? 'fr' : 'en';
    return [
      ...screens
        .filter((screen) => screen.scope === 'workspace' || pipeline !== undefined)
        .map((screen) => ({
          id: `go-${screen.id}`,
          group: goTo,
          label: t(screen.label),
          hint: screen.goKey && `g ${screen.goKey}`,
          run: () => go(screen),
        })),
      ...(pipelines.status === 'ready' ? pipelines.data : []).map((p) => ({
        id: `pipeline-${p.name}`,
        group: goTo,
        label: t('palette.pipelineGraph', { pipeline: p.name }),
        run: () => navigate(pipelinePath(p.name)),
      })),
      ...themes
        .filter((theme) => theme !== prefs.theme)
        .map((theme) => ({
          id: `theme-${theme}`,
          group: action,
          label: t(themeCommands[theme]),
          run: () => prefs.setTheme(theme),
        })),
      {
        id: 'language',
        group: action,
        label: t('palette.switchLanguage'),
        run: () => prefs.setLanguage(otherLanguage),
      },
      { id: 'shortcuts', group: action, label: t('palette.showShortcuts'), hint: '?', run: () => setHelp(true) },
    ];
  }, [t, pipeline, pipelines, prefs, navigate, go]);

  const closeMenu = () => setMenu(false);

  return (
    <div className="flex min-h-dvh bg-bg text-ink">
      <a
        href="#main"
        className="fixed top-2 left-2 z-[60] -translate-y-24 rounded-1 bg-accent px-3 py-2 font-bold text-accent-ink focus-visible:translate-y-0"
      >
        {t('shell.skipToContent')}
      </a>

      <nav
        ref={nav}
        id="main-nav"
        aria-label={t('shell.mainNav')}
        className={`${menu ? 'fixed inset-y-0 left-0 z-40 flex shadow-2' : 'hidden'} w-[228px] flex-none flex-col gap-[22px] overflow-y-auto border-r border-line bg-surface-2 px-3 py-[18px] md:sticky md:top-0 md:flex md:h-dvh md:shadow-none`}
      >
        <div className="flex items-center gap-2.5 px-2">
          <Logo />
          <span className="font-display text-[26px] leading-[26px] tracking-[-0.01em]">{t('app.name')}</span>
        </div>
        <PipelineSwitcher current={pipeline} pipelines={pipelines} />
        <NavGroup title={t('shell.workspace')} items={workspaceScreens} pipeline={pipeline} onNavigate={closeMenu} />
        {pipeline !== undefined && (
          <NavGroup title={t('shell.pipeline')} items={pipelineScreens} pipeline={pipeline} onNavigate={closeMenu} />
        )}
        <div className="mt-auto flex flex-col gap-2 border-t border-line px-2.5 pt-3 text-small text-ink-2">
          <h2 className="sr-only">{t('shell.preferences')}</h2>
          <label className="flex flex-col gap-1">
            {t('theme.label')}
            <select
              value={prefs.theme}
              onChange={(e) => prefs.setTheme(e.target.value as Theme)}
              className={selectClass}
            >
              {themes.map((theme) => (
                <option key={theme} value={theme}>
                  {t(themeNames[theme])}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1">
            {t('language.label')}
            <select
              value={prefs.language}
              onChange={(e) => prefs.setLanguage(e.target.value as Language)}
              className={selectClass}
            >
              {languages.map((language) => (
                <option key={language} value={language} lang={language}>
                  {t(languageNames[language])}
                </option>
              ))}
            </select>
          </label>
        </div>
      </nav>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex min-h-14 flex-none flex-wrap items-center gap-x-4 gap-y-2 border-b border-line bg-surface px-3 py-2 md:px-6">
          <button
            ref={menuButton}
            type="button"
            aria-expanded={menu}
            aria-controls="main-nav"
            onClick={() => setMenu(!menu)}
            className="inline-flex size-11 items-center justify-center rounded-1 md:hidden"
          >
            <span className="sr-only">{menu ? t('shell.closeMenu') : t('shell.openMenu')}</span>
            <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
              <path d="M3 5H15M3 9H15M3 13H15" stroke="currentColor" strokeWidth="1.6" />
            </svg>
          </button>
          {demo && <DemoBadge />}
          <div className="flex-1" />
          {pipeline !== undefined && <LiveIndicator state={live} />}
          <button
            type="button"
            onClick={() => setPalette(true)}
            className="inline-flex min-w-0 items-center gap-2.5 rounded-1 border border-line bg-bg px-2.5 py-1.5 text-left text-small text-ink-2 md:w-[260px]"
          >
            <span className="min-w-0 flex-1 truncate">{t('shell.search')}</span>
            <Kbd>{isMac ? '⌘K' : 'Ctrl+K'}</Kbd>
          </button>
        </header>
        <main
          id="main"
          tabIndex={-1}
          className={`flex-1 px-4 py-6 focus-visible:outline-none md:px-7 ${live.stale ? 'live-stale' : ''}`}
        >
          <Outlet />
        </main>
      </div>

      {palette && <CommandPalette commands={commands} onClose={() => setPalette(false)} />}
      {help && <ShortcutHelp onClose={() => setHelp(false)} />}
    </div>
  );
}
