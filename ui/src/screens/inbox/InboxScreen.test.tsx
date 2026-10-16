import { act, fireEvent, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { ReviewPage } from '../../api/schema';
import { renderApp } from '../../test/render';
import { UNDO_MS } from './useDecisions';
import { INBOX_REFRESH_MS } from './useInboxQueue';

const press = (key: string, target: Element = document.body) => fireEvent.keyDown(target, { key });
const panel = () => screen.getByRole('region', { name: /^(Item under review|Item en revue)$/ });
const title = () => within(panel()).getByRole('heading', { level: 2 });
const decisions = (api: FakeApi) => api.calls.filter((c) => c.method.startsWith('decide'));

async function openInbox(options: Parameters<typeof renderApp>[0] = {}) {
  const view = renderApp({ route: '/inbox', ...options });
  await screen.findByRole('region', { name: options.language === 'fr' ? 'Item en revue' : 'Item under review' });
  return view;
}

// Runs the timers, then lets the decision's promise settle.
const elapse = (ms: number) => act(() => vi.advanceTimersByTimeAsync(ms));

describe('InboxScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  it('shows the queue oldest first, the counts, and the first item rendered for a person', async () => {
    await openInbox();
    expect(screen.getByRole('heading', { level: 1, name: 'Inbox' })).toBeInTheDocument();
    expect(screen.getByText('8 to review')).toBeInTheDocument();
    expect(screen.getByText(/1 sampled · oldest 14 min/)).toBeInTheDocument();
    const queue = screen.getByRole('list', { name: 'Items to review' });
    const rows = within(queue).getAllByRole('button');
    expect(rows).toHaveLength(9);
    expect(rows[0]).toHaveAttribute('aria-current', 'true');
    expect(within(rows[5]!).getByText('Sample')).toBeInTheDocument();
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
    expect(screen.getByText('Rue de Crimée, 75019 Paris · 4th floor, no lift')).toBeInTheDocument();
    expect(screen.getByText(/1 of 9/)).toHaveTextContent('1 of 9 · analyst-review · listings-daily · waiting 14 min');
    expect(within(panel()).getByText('priceEur').nextElementSibling).toHaveTextContent('412,000');
    expect(screen.getByRole('group', { name: 'You can edit these fields' })).toBeInTheDocument();
    expect(screen.getByLabelText(/marginPct/)).toHaveValue('17.8');
    expect(screen.getByRole('link', { name: 'Open url' })).toHaveAttribute(
      'href',
      'https://listings.example/annonces/7f3a9c21',
    );
  });

  it('approves from the keyboard, focuses the next title, and sends the decision once the undo window closes', async () => {
    const api = new FakeApi();
    await openInbox({ api });
    press('a');
    expect(title()).toHaveTextContent('T2 · 44 m² · Pantin');
    expect(title()).toHaveFocus();
    expect(screen.getByText('Approved. 8 left. Press u to undo.')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /^Undo/ })).toHaveLength(1);
    expect(decisions(api)).toEqual([]);
    await elapse(UNDO_MS - 500);
    expect(decisions(api)).toEqual([]);
    await elapse(500);
    expect(decisions(api)).toEqual([
      { method: 'decideReview', args: ['listings-daily', 'dl_4b2e', { decision: 'approve', version: 1 }] },
    ]);
    expect(screen.queryByRole('button', { name: /^Undo/ })).not.toBeInTheDocument();
  });

  it('undoes the last decision with u: the card comes back and nothing reaches the server', async () => {
    const api = new FakeApi();
    await openInbox({ api });
    press('a');
    await elapse(4000);
    press('u');
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
    expect(screen.getByText('Undone: T3 · 68 m² · Paris 19e is back in the queue.')).toBeInTheDocument();
    await elapse(UNDO_MS);
    expect(decisions(api)).toEqual([]);
    expect(within(screen.getByRole('list', { name: 'Items to review' })).getAllByRole('button')).toHaveLength(9);
    press('u');
    expect(decisions(api)).toEqual([]);
  });

  it('sends a held decision at once when the next one is made, so only the last can be undone', async () => {
    const api = new FakeApi();
    await openInbox({ api });
    press('a');
    press('a');
    await elapse(0);
    expect(decisions(api)).toHaveLength(1);
    press('u');
    expect(title()).toHaveTextContent('T2 · 44 m² · Pantin');
    await elapse(UNDO_MS);
    expect(decisions(api)).toHaveLength(1);
  });

  it('rejects with r then a digit, with the note typed first', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    press('r');
    const dialog = screen.getByRole('dialog', { name: 'Why reject?' });
    expect(within(dialog).getByRole('button', { name: /overpriced/ })).toHaveFocus();
    await user.type(within(dialog).getByLabelText('Note (optional)'), 'Agency listing');
    press('2', within(dialog).getByRole('button', { name: /overpriced/ }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByText('Rejected. 8 left. Press u to undo.')).toBeInTheDocument();
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toEqual({
      decision: 'reject',
      version: 1,
      reason: 'wrong-data',
      text: 'Agency listing',
    });
  });

  it('ignores digits typed in the note and a digit with no reason, and closes on Cancel', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    await user.click(screen.getByRole('button', { name: /Reject…/ }));
    const dialog = screen.getByRole('dialog');
    press('3', within(dialog).getByLabelText('Note (optional)'));
    press('9', within(dialog).getByRole('button', { name: /duplicate/ }));
    press('x', within(dialog).getByRole('button', { name: /duplicate/ }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /Reject…/ }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: /duplicate/ }));
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toEqual({ decision: 'reject', version: 1, reason: 'duplicate' });
  });

  it('edits the editable fields, shows the diff, and approves with the patch', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    press('e');
    const margin = screen.getByLabelText(/marginPct/);
    expect(margin).toHaveFocus();
    await user.clear(margin);
    await user.type(margin, '16.5');
    await user.type(screen.getByLabelText(/notes/), 'No lift');
    expect(screen.getByText('Was 17.8')).toBeInTheDocument();
    const diff = screen.getByRole('group', { name: 'Your changes' });
    expect(diff).toHaveTextContent('Changes: 2');
    expect(diff).toHaveTextContent('marginPctbefore 17.8→after 16.5');
    expect(diff).toHaveTextContent('notesbefore empty→after "No lift"');
    press('Escape', screen.getByLabelText(/notes/));
    expect(title()).toHaveFocus();
    press('a');
    expect(screen.getByText('Approved with changes. 8 left. Press u to undo.')).toBeInTheDocument();
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toEqual({
      decision: 'approve',
      version: 1,
      patch: [
        { op: 'replace', path: '/marginPct', value: 16.5 },
        { op: 'replace', path: '/notes', value: 'No lift' },
      ],
    });
  });

  it('refuses to approve an invalid field, approves with ⌘↵ from a field, and discards edits with Esc', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    const margin = screen.getByLabelText(/marginPct/);
    await user.clear(margin);
    await user.type(margin, 'abc');
    expect(screen.getByText('Type a number, such as 16.5.')).toBeInTheDocument();
    expect(margin).toHaveAttribute('aria-invalid', 'true');
    await user.click(screen.getByRole('button', { name: /^Approve/ }));
    expect(screen.getByText('Fix the highlighted field before approving.')).toBeInTheDocument();
    expect(margin).toHaveFocus();
    await user.clear(margin);
    await user.type(margin, '18');
    margin.blur();
    press('Escape');
    expect(screen.getByLabelText(/marginPct/)).toHaveValue('17.8');
    await user.type(screen.getByLabelText(/marginPct/), '5');
    expect(screen.getByRole('button', { name: /Discard edits/ })).toBeInTheDocument();
    fireEvent.keyDown(screen.getByLabelText(/marginPct/), { key: 'Enter', ctrlKey: true });
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toMatchObject({ patch: [{ path: '/marginPct', value: 17.85 }] });
  });

  // Two reviewers on the same item: the loser's decision answers 409 "already decided", sent once, never retried.
  it('shows the server message and keeps the card gone on a 409', async () => {
    const api = new FakeApi();
    await openInbox({ api });
    press('j');
    press('j');
    expect(title()).toHaveTextContent('T4 · 91 m² · Montreuil');
    press('a');
    await elapse(UNDO_MS);
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(
      'T4 · 91 m² · Montreuil was decided elsewhere or timed out. review of delivery dl_7c1d was already decided by claire at 06:29',
    );
    expect(screen.queryByRole('button', { name: /T4 · 91 m² · Montreuil/ })).not.toBeInTheDocument();
    await elapse(UNDO_MS * 2);
    expect(decisions(api).map((c) => [c.method, c.args[1]])).toEqual([['decideReview', 'dl_7c1d']]);
    fireEvent.click(within(alert).getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('shows the server timeout message and keeps the card gone when the review timed out while open (409)', async () => {
    const timedOut = 'review of delivery dl_7c1d timed out at 2026-10-16T06:29:00Z, applied reject: conflict';
    const api = new FakeApi({ ...demoFixtures, conflicts: { dl_7c1d: timedOut } });
    await openInbox({ api });
    press('j');
    press('j');
    expect(title()).toHaveTextContent('T4 · 91 m² · Montreuil');
    press('a');
    await elapse(UNDO_MS);
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(`T4 · 91 m² · Montreuil was decided elsewhere or timed out. ${timedOut}`);
    expect(screen.queryByRole('button', { name: /T4 · 91 m² · Montreuil/ })).not.toBeInTheDocument();
    expect(title()).not.toHaveTextContent('T4 · 91 m² · Montreuil');
    await elapse(UNDO_MS * 2);
    expect(decisions(api).map((c) => [c.method, c.args[1]])).toEqual([['decideReview', 'dl_7c1d']]);
  });

  it('brings the card back when the server refuses for another reason', async () => {
    const api = new FakeApi();
    api.decideReview = () => Promise.reject(new ApiError(400, 'invalid', 'edit outside /marginPct'));
    await openInbox({ api });
    press('a');
    await elapse(UNDO_MS);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Your decision on T3 · 68 m² · Paris 19e was not recorded, and the card is back. edit outside /marginPct',
    );
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
  });

  it('decides review samples: agree with a, disagree with a note', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    await user.click(screen.getByRole('button', { name: /T2 · 39 m² · Paris 18e/ }));
    expect(title()).toHaveTextContent('T2 · 39 m² · Paris 18e');
    expect(screen.getByText(/Your verdict only measures the rule/)).toBeInTheDocument();
    expect(screen.queryByRole('group', { name: 'You can edit these fields' })).not.toBeInTheDocument();
    press('r');
    const dialog = screen.getByRole('dialog', { name: 'Why do you disagree with the rule?' });
    await user.type(within(dialog).getByLabelText('Note (optional)'), 'Too small{Enter}');
    await elapse(UNDO_MS);
    expect(decisions(api)).toEqual([
      { method: 'decideSample', args: ['listings-daily', 'dl_d3f5', { decision: 'disagree', reason: 'Too small' }] },
    ]);
    press('u');
  });

  it('agrees with a sample from the keyboard', async () => {
    const api = new FakeApi();
    await openInbox({ api, route: '/inbox/analysts/dl_d3f5' });
    expect(title()).toHaveTextContent('T2 · 39 m² · Paris 18e');
    press('a');
    expect(screen.getByText('Agreed. 8 left. Press u to undo.')).toBeInTheDocument();
    await elapse(UNDO_MS);
    expect(decisions(api)[0]).toEqual({
      method: 'decideSample',
      args: ['listings-daily', 'dl_d3f5', { decision: 'agree' }],
    });
  });

  it('disagrees with a sample without a note from the dialog button', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api, route: '/inbox/analysts/dl_d3f5' });
    await user.click(screen.getByRole('button', { name: /Disagree…/ }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: /^Disagree/ }));
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toEqual({ decision: 'disagree' });
  });

  it('opens the review a notification links to, and says when it is no longer open', async () => {
    await openInbox({ route: '/inbox/analysts/dl_88f2' });
    expect(title()).toHaveTextContent('Studio · 19 m² · Paris 20e');
  });

  it('says when a linked review is gone', async () => {
    await openInbox({ route: '/inbox/analysts/dl_gone' });
    expect(screen.getByText(/The review this link points to is no longer open/)).toBeInTheDocument();
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
  });

  it('moves with j, k and the arrows in the queue, and leaves g sequences to the shell', async () => {
    const { location } = await openInbox();
    press('j');
    expect(title()).toHaveTextContent('T2 · 44 m² · Pantin');
    press('k');
    press('k');
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
    const firstRow = within(screen.getByRole('list', { name: 'Items to review' })).getAllByRole('button')[0]!;
    press('ArrowDown', firstRow);
    expect(title()).toHaveTextContent('T2 · 44 m² · Pantin');
    press('ArrowUp', firstRow);
    press('ArrowDown');
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
    press('g');
    press('r');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(location()).toBe('/pipelines');
  });

  it('sends a held decision when the screen goes away', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    press('a');
    await user.click(screen.getByRole('link', { name: 'Nodes' }));
    await elapse(0);
    expect(decisions(api)).toHaveLength(1);
  });

  it('sends a held decision exactly once, with keepalive, when the tab closes or is hidden', async () => {
    const api = new FakeApi();
    await openInbox({ api });
    press('a');
    act(() => {
      window.dispatchEvent(new Event('pagehide'));
      window.dispatchEvent(new Event('pagehide'));
    });
    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    act(() => {
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await elapse(UNDO_MS * 2);
    expect(decisions(api)).toEqual([
      {
        method: 'decideReview',
        args: ['listings-daily', 'dl_4b2e', { decision: 'approve', version: 1 }, { keepalive: true }],
      },
    ]);
    // The undo window is over: nothing is held any more.
    expect(screen.queryByRole('button', { name: /^Undo/ })).not.toBeInTheDocument();
  });

  it('says in the shell a decision refused after the inbox was left', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api });
    press('j');
    press('j');
    press('a');
    await user.click(screen.getByRole('link', { name: 'Nodes' }));
    await elapse(0);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Your decision on T4 · 91 m² · Montreuil wasn’t applied after you left the inbox: review of delivery dl_7c1d was already decided by claire at 06:29',
    );
    expect(screen.getByRole('heading', { level: 1, name: 'Nodes' })).toBeInTheDocument();
  });

  it('undoes into the queue of the filters the card belongs to', async () => {
    const { user } = await openInbox();
    press('a');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Pipeline' }), 'photo-tagging');
    expect(await screen.findByText('Inbox zero')).toBeInTheDocument();
    press('u');
    // Not a photo-tagging review: the empty queue stays empty.
    expect(screen.getByText('Inbox zero')).toBeInTheDocument();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Pipeline' }), '');
    await screen.findByRole('region', { name: 'Item under review' });
    expect(screen.getByRole('button', { name: /T3 · 68 m² · Paris 19e/ })).toBeInTheDocument();
    expect(screen.getByText('8 to review')).toBeInTheDocument();
  });

  it('reads the queue again every 15 s, silently, keeping a decided card hidden', async () => {
    const api = new FakeApi();
    const listInbox = vi.spyOn(api, 'listInbox');
    await openInbox({ api });
    press('a');
    const reads = listInbox.mock.calls.length;
    await elapse(INBOX_REFRESH_MS);
    expect(listInbox.mock.calls.length).toBe(reads + 1);
    expect(screen.queryByTestId('loading')).not.toBeInTheDocument();
    // The decision is sent after 10 s; the server no longer lists the card, and it was never shown again.
    expect(screen.queryByRole('button', { name: /T3 · 68 m² · Paris 19e/ })).not.toBeInTheDocument();
    expect(screen.getByText('7 to review')).toBeInTheDocument();
  });

  it('filters by pipeline and inbox through the URL', async () => {
    const { user, location } = await openInbox();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Pipeline' }), 'photo-tagging');
    expect(location()).toBe('/inbox?pipeline=photo-tagging');
    expect(await screen.findByText('Inbox zero')).toBeInTheDocument();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Pipeline' }), '');
    await screen.findByRole('region', { name: 'Item under review' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Inbox' }), 'analysts');
    expect(location()).toBe('/inbox?inbox=analysts');
  });

  it('shows loading, then an error with retry', async () => {
    let fail = true;
    const api = new FakeApi();
    const real = api.listInbox;
    api.listInbox = (query) =>
      fail ? Promise.reject(new ApiError(503, 'unavailable', 'engine switching over')) : real(query);
    const { user } = renderApp({ route: '/inbox', api });
    expect(screen.getByTestId('loading')).toBeInTheDocument();
    expect(await screen.findByRole('alert')).toHaveTextContent('This could not be loaded. engine switching over');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    await screen.findByRole('region', { name: 'Item under review' });
    expect(title()).toHaveTextContent('T3');
  });

  it('pages through a long queue', async () => {
    const api = new FakeApi();
    const real = api.listInbox;
    api.listInbox = async ({ cursor, ...query } = {}) => {
      const all = (await real(query)).items;
      const page: ReviewPage = cursor === undefined ? { items: all.slice(0, 3), next: 'c3' } : { items: all.slice(3) };
      return page;
    };
    const { user } = await openInbox({ api });
    const list = () => within(screen.getByRole('list', { name: 'Items to review' })).getAllByRole('button');
    expect(list()).toHaveLength(4);
    await user.click(screen.getByRole('button', { name: 'Load more' }));
    expect(await screen.findByRole('button', { name: /Maison/ })).toBeInTheDocument();
    expect(list()).toHaveLength(9);
    expect(screen.queryByRole('button', { name: 'Load more' })).not.toBeInTheDocument();
  });

  it('works on a phone: list button, previous and next', async () => {
    const { user } = await openInbox();
    const toggle = screen.getByRole('button', { name: 'List of items' });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await user.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('button', { name: 'Previous item' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Next item' }));
    expect(title()).toHaveTextContent('T2 · 44 m² · Pantin');
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    await user.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByLabelText(/marginPct/)).toHaveFocus();
  });

  it('speaks French, with decimal commas', async () => {
    const api = new FakeApi();
    const { user } = await openInbox({ api, language: 'fr' });
    expect(screen.getByRole('heading', { level: 1, name: 'Revue' })).toBeInTheDocument();
    expect(screen.getByText('À revoir : 8')).toBeInTheDocument();
    const margin = screen.getByLabelText(/marginPct/);
    expect(margin).toHaveValue('17,8');
    await user.clear(margin);
    await user.type(margin, '16,5');
    expect(screen.getByRole('button', { name: /Approuver avec modifications \(1\)/ })).toBeInTheDocument();
    press('Escape', margin);
    press('a');
    expect(screen.getByText('Approuvé avec modifications. Reste : 8. Appuyez sur u pour annuler.')).toBeInTheDocument();
    await elapse(UNDO_MS);
    expect(decisions(api)[0]!.args[2]).toMatchObject({ patch: [{ value: 16.5 }] });
  });

  it('turns the single keys off with the preference', async () => {
    window.localStorage.setItem('millstream.singleKeys', 'off');
    const api = new FakeApi();
    await openInbox({ api });
    press('a');
    expect(title()).toHaveTextContent('T3 · 68 m² · Paris 19e');
  });

  it('celebrates inbox zero', async () => {
    renderApp({ route: '/inbox', api: new FakeApi({ ...demoFixtures, inbox: [], samples: [] }) });
    expect(await screen.findByText('Inbox zero')).toBeInTheDocument();
  });
});
