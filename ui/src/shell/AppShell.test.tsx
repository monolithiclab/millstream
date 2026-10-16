import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { FakeApi } from '../api/fake';
import { FakeEventSource } from '../live/fakeEventSource';
import { renderApp } from '../test/render';

const nav = () => within(screen.getByRole('navigation'));

describe('app shell', () => {
  it('shows the workspace navigation and the page, with landmarks and a skip link', async () => {
    renderApp();
    expect(screen.getByRole('link', { name: 'Skip to main content' })).toHaveAttribute('href', '#main');
    expect(screen.getByRole('main')).toHaveAttribute('id', 'main');
    expect(screen.getByRole('banner')).toBeInTheDocument();
    expect(
      nav()
        .getAllByRole('link')
        .map((a) => a.textContent),
    ).toEqual(['Pipelines', 'Inbox', 'Nodes', 'Metrics', 'Logs']);
    expect(nav().getByRole('link', { name: 'Pipelines' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('heading', { level: 1, name: 'Pipelines' })).toBeInTheDocument();
    expect(document.title).toBe('Pipelines · Millstream');
    // No pipeline: no live stream, no live mark.
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    await screen.findByRole('button', { name: 'Switch pipeline, showing All pipelines' });
  });

  it('adds the pipeline navigation and the live mark inside a pipeline', () => {
    vi.useFakeTimers();
    renderApp({ route: '/p/listings-daily/runs' });
    expect(nav().getByRole('link', { name: 'Runs' })).toHaveAttribute('aria-current', 'page');
    expect(nav().getByRole('link', { name: 'Dead letters' })).toHaveAttribute('href', '/p/listings-daily/dead-letters');
    expect(screen.getByText('Pipeline listings-daily')).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Connecting');
    const es = FakeEventSource.latest();
    expect(es.url).toBe('/api/v1/live/pipelines/listings-daily');
    act(() => es.open());
    expect(screen.getByRole('status')).toHaveTextContent('Live');
    act(() => es.fail());
    act(() => vi.advanceTimersByTime(1000));
    act(() => FakeEventSource.latest().fail());
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.getByRole('status')).toHaveTextContent('Reconnecting · 1 s');
    act(() => vi.advanceTimersByTime(1000));
    act(() => FakeEventSource.latest().fail());
    expect(screen.getByRole('status')).toHaveTextContent('Reconnecting · 4 s');
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.getByRole('status')).toHaveTextContent('Reconnecting · 3 s');
    expect(screen.getByRole('main')).not.toHaveClass('live-stale');
    act(() => vi.advanceTimersByTime(1000));
    expect(screen.getByRole('main')).toHaveClass('live-stale');
  });

  it('switches pipeline from the switcher', async () => {
    const { user, location } = renderApp();
    const button = screen.getByRole('button', { name: /Switch pipeline/ });
    await user.click(button);
    expect(button).toHaveAttribute('aria-expanded', 'true');
    await user.click(await nav().findByRole('link', { name: 'photo-tagging' }));
    expect(location()).toBe('/p/photo-tagging/graph');
    expect(screen.getByRole('button', { name: 'Switch pipeline, showing photo-tagging' })).toHaveAttribute(
      'aria-expanded',
      'false',
    );
    await user.click(screen.getByRole('button', { name: /Switch pipeline/ }));
    expect(screen.getByRole('link', { name: 'photo-tagging' })).toHaveAttribute('aria-current', 'true');
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('group', { name: 'Pipelines' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Switch pipeline/ }));
    await user.click(screen.getByRole('main'));
    expect(screen.queryByRole('group', { name: 'Pipelines' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Switch pipeline/ }));
    await user.click(screen.getByRole('link', { name: 'All pipelines' }));
    expect(location()).toBe('/pipelines');
  });

  it('says when pipelines cannot be loaded, and while they load', async () => {
    const api = new FakeApi();
    let fail: (e: Error) => void = () => undefined;
    api.listPipelines = () => new Promise((_, reject) => (fail = reject));
    const { user } = renderApp({ api });
    await user.click(screen.getByRole('button', { name: /Switch pipeline/ }));
    expect(screen.getByText('Loading pipelines…')).toBeInTheDocument();
    act(() => fail(new Error('down')));
    expect(await screen.findByRole('alert')).toHaveTextContent('Pipelines could not be loaded.');
  });

  it('opens the command palette with Ctrl+K and runs a command', async () => {
    const { user, location } = renderApp();
    const opener = screen.getByRole('button', { name: /Search items/ });
    opener.focus();
    await user.keyboard('{Control>}k{/Control}');
    const dialog = screen.getByRole('dialog', { name: 'Command palette' });
    const input = within(dialog).getByRole('combobox', { name: 'Command' });
    expect(input).toHaveFocus();
    await user.type(input, 'inbox');
    expect(
      within(dialog)
        .getAllByRole('option')
        .map((o) => o.textContent),
    ).toEqual(['Go toInboxg i']);
    await user.keyboard('{Enter}');
    expect(location()).toBe('/inbox');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('moves through commands with the arrows, finds pipelines and actions, and closes on Esc', async () => {
    const { user } = renderApp();
    const opener = screen.getByRole('button', { name: /Search items/ });
    await user.click(opener);
    const input = screen.getByRole('combobox', { name: 'Command' });
    await screen.findByRole('option', { name: /listings-daily · graph/ });
    const options = () => within(screen.getByRole('listbox')).getAllByRole('option');
    expect(options()[0]).toHaveAttribute('aria-selected', 'true');
    await user.keyboard('{ArrowUp}');
    expect(options().at(-1)).toHaveAttribute('aria-selected', 'true');
    expect(input).toHaveAttribute('aria-activedescendant', options().at(-1)!.id);
    await user.keyboard('{ArrowDown}');
    expect(options()[0]).toHaveAttribute('aria-selected', 'true');
    await user.type(input, 'zzz');
    expect(screen.getByRole('status')).toHaveTextContent('No command matches.');
    await user.keyboard('{Enter}');
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it('runs actions from the palette: theme, language, shortcuts', async () => {
    const { user } = renderApp();
    await user.click(screen.getByRole('button', { name: /Search items/ }));
    await user.type(screen.getByRole('combobox', { name: 'Command' }), 'dark theme');
    await user.click(screen.getByRole('option', { name: /Use the dark theme/ }));
    expect(document.documentElement).toHaveAttribute('data-theme', 'dark');
    await user.click(screen.getByRole('button', { name: /Search items/ }));
    await user.type(screen.getByRole('combobox', { name: 'Command' }), 'français');
    await user.keyboard('{Enter}');
    expect(await screen.findByRole('heading', { level: 1, name: 'Pipelines' })).toBeInTheDocument();
    expect(nav().getByRole('link', { name: 'Revue' })).toBeInTheDocument();
    expect(document.documentElement.lang).toBe('fr');
    await user.click(screen.getByRole('button', { name: /Chercher/ }));
    await user.type(screen.getByRole('combobox', { name: 'Commande' }), 'raccourcis');
    await user.keyboard('{Enter}');
    expect(screen.getByRole('dialog', { name: 'Raccourcis clavier' })).toBeInTheDocument();
  });

  it('navigates with g sequences and opens help with ?, never from inside a field', async () => {
    const { user, location } = renderApp({ route: '/p/listings-daily/graph' });
    fireEvent.keyDown(window, { key: 'g' });
    fireEvent.keyDown(window, { key: 'i' });
    expect(location()).toBe('/inbox');
    fireEvent.keyDown(window, { key: 'g' });
    fireEvent.keyDown(window, { key: 'z' });
    expect(location()).toBe('/inbox');
    // Inside a pipeline-less screen, g g goes to the pipelines list.
    fireEvent.keyDown(window, { key: 'g' });
    fireEvent.keyDown(window, { key: 'g' });
    expect(location()).toBe('/pipelines');

    const select = screen.getByRole('combobox', { name: 'Theme' });
    fireEvent.keyDown(select, { key: '?' });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    fireEvent.keyDown(window, { key: '?' });
    expect(screen.getByRole('dialog', { name: 'Keyboard shortcuts' })).toBeInTheDocument();
    // Single keys are suspended while a dialog is open.
    fireEvent.keyDown(window, { key: 'g' });
    fireEvent.keyDown(window, { key: 'i' });
    expect(location()).toBe('/pipelines');
    await user.click(screen.getByRole('button', { name: 'Close' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('forgets a lone g after a moment', () => {
    vi.useFakeTimers();
    const { location } = renderApp();
    fireEvent.keyDown(window, { key: 'g' });
    act(() => vi.advanceTimersByTime(1500));
    fireEvent.keyDown(window, { key: 'i' });
    expect(location()).toBe('/pipelines');
  });

  it('lets single-key shortcuts be turned off, keeping Ctrl+K', async () => {
    const { user } = renderApp();
    fireEvent.keyDown(window, { key: '?' });
    await user.click(screen.getByRole('checkbox', { name: /Single-key shortcuts/ }));
    expect(window.localStorage.getItem('millstream.singleKeys')).toBe('off');
    await user.keyboard('{Escape}');
    fireEvent.keyDown(window, { key: '?' });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'k', metaKey: true });
    expect(screen.getByRole('dialog', { name: 'Command palette' })).toBeInTheDocument();
  });

  it('changes and stores the theme and the language', async () => {
    const { user } = renderApp();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Theme' }), 'light');
    expect(document.documentElement).toHaveAttribute('data-theme', 'light');
    expect(window.localStorage.getItem('millstream.theme')).toBe('light');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Theme' }), 'system');
    expect(document.documentElement).not.toHaveAttribute('data-theme');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Language' }), 'fr');
    expect(window.localStorage.getItem('millstream.language')).toBe('fr');
    expect(screen.getByRole('link', { name: 'Aller au contenu principal' })).toBeInTheDocument();
    expect(document.title).toBe('Pipelines · Millstream');
  });

  it('opens and closes the navigation drawer on small screens', async () => {
    const { user } = renderApp();
    const toggle = screen.getByRole('button', { name: 'Open navigation' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await user.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(toggle).toHaveAccessibleName('Close navigation');
    await user.click(nav().getByRole('link', { name: 'Nodes' }));
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await user.click(toggle);
    await user.keyboard('{Escape}');
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(toggle).toHaveFocus();
  });

  it('focuses the heading after navigating', async () => {
    const { user } = renderApp();
    await user.click(nav().getByRole('link', { name: 'Metrics' }));
    expect(screen.getByRole('heading', { level: 1, name: 'Metrics' })).toHaveFocus();
  });

  it('serves unknown addresses a not-found page', async () => {
    const { user, location } = renderApp({ route: '/nowhere' });
    expect(screen.getByRole('heading', { level: 1, name: 'Page not found' })).toBeInTheDocument();
    await user.click(screen.getByRole('link', { name: 'Go to pipelines' }));
    expect(location()).toBe('/pipelines');
    await waitFor(() => expect(document.title).toBe('Pipelines · Millstream'));
  });

  it.each([
    ['/', '/pipelines'],
    ['/p/listings-daily', '/p/listings-daily/graph'],
  ])('redirects %s to %s', (route, target) => {
    expect(renderApp({ route }).location()).toBe(target);
  });
});
