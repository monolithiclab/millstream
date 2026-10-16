import { screen, waitFor, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ApiError } from '../api/api';
import { FakeApi } from '../api/fake';
import { renderApp } from '../test/render';
import { guardApi } from './SessionGate';

describe('sign in', () => {
  it('asks for the token when the server answers unauthorized, signs in, and signs out from the menu', async () => {
    const api = new FakeApi(undefined, { token: 's3cret' });
    const { user } = renderApp({ api });
    expect(await screen.findByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();
    expect(document.title).toBe('Sign in · Millstream');
    const field = screen.getByLabelText('Token');
    const submit = screen.getByRole('button', { name: 'Sign in' });
    expect(submit).toBeDisabled();

    await user.type(field, 'wrong');
    await user.click(submit);
    expect(await screen.findByRole('alert')).toHaveTextContent("This token does not match the server's.");
    expect(field).toHaveAttribute('aria-invalid', 'true');
    expect(field).toHaveAccessibleDescription("This token does not match the server's.");

    await user.clear(field);
    await user.type(field, 's3cret');
    await user.keyboard('{Enter}');
    expect(await screen.findByRole('heading', { level: 1, name: 'Pipelines' })).toBeInTheDocument();
    expect(await screen.findByRole('table')).toBeInTheDocument();

    await user.click(within(screen.getByRole('navigation')).getByRole('button', { name: 'Sign out' }));
    expect(await screen.findByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();
    expect(api.calls.map((c) => c.method)).toEqual(['signIn', 'signOut']);
  });

  it('shows the sign-in screen again when the session ends, and signs out from the palette', async () => {
    const api = new FakeApi(undefined, { token: 's3cret' });
    const { user } = renderApp({ api, route: '/nodes' });
    await user.type(await screen.findByLabelText('Token'), 's3cret');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    await screen.findByRole('heading', { level: 1, name: 'Nodes' });

    api.expireSession();
    await user.click(screen.getByRole('button', { name: /Search/ }));
    await user.type(screen.getByRole('combobox', { name: 'Command' }), 'Pipelines');
    await user.keyboard('{Enter}');
    expect(await screen.findByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();

    await user.type(screen.getByLabelText('Token'), 's3cret');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    await screen.findByRole('heading', { level: 1, name: 'Pipelines' });
    await user.click(screen.getByRole('button', { name: /Search/ }));
    await user.type(screen.getByRole('combobox', { name: 'Command' }), 'Sign out');
    await user.keyboard('{Enter}');
    expect(await screen.findByRole('heading', { level: 1, name: 'Sign in' })).toBeInTheDocument();
  });

  it('says when signing in fails for another reason, in French', async () => {
    const api = new FakeApi(undefined, { token: 's3cret' });
    api.signIn = () => Promise.reject(new ApiError(503, 'unavailable', 'the server is closing'));
    const { user } = renderApp({ api, language: 'fr' });
    await user.type(await screen.findByLabelText('Jeton'), 'x');
    await user.click(screen.getByRole('button', { name: 'Se connecter' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(/^La connexion a échoué\s: the server is closing$/);
  });

  it('has no sign-out on a server without a token', async () => {
    renderApp();
    await screen.findByRole('table');
    expect(screen.queryByRole('button', { name: 'Sign out' })).not.toBeInTheDocument();
  });

  it('guards every method, leaving other answers and values alone', async () => {
    let unauthorized = 0;
    const fake = new FakeApi(undefined, { token: 't' });
    const api = guardApi(fake, () => (unauthorized += 1));
    await expect(api.listPipelines()).rejects.toMatchObject({ status: 401 });
    await expect(api.signIn('nope')).rejects.toMatchObject({ status: 401 });
    expect(unauthorized).toBe(2);
    await api.signIn('t');
    await expect(api.getPipeline('nope')).rejects.toMatchObject({ status: 404 });
    expect(api.artifactUrl({ pipeline: 'p', item: 'i', name: 'n' })).toBe('about:blank');
    expect((api as FakeApi).calls).toBe(fake.calls);
    await waitFor(() => expect(unauthorized).toBe(2));
  });
});
