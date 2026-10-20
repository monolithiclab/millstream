import { screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { mockMatchMedia } from '../../test/flow';
import { renderApp } from '../../test/render';

const route = (id: string) => `/p/listings-daily/improve/${id}`;
const section = (name: string) => screen.getByRole('region', { name });

describe('ProposalScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  async function open(id: string, api = new FakeApi(), language: 'en' | 'fr' = 'en') {
    const view = renderApp({ route: route(id), api, language });
    await screen.findByRole('region', { name: language === 'en' ? 'What and why' : 'Quoi et pourquoi' });
    return view;
  }

  it('shows a ready proposal: why, its base, the patch, the validation, the evidence and its history', async () => {
    await open('p-0151');
    expect(screen.getByRole('heading', { level: 1, name: 'Proposal p-0151' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Improve · listings-daily' })).toHaveAttribute(
      'href',
      '/p/listings-daily/improve',
    );
    const why = section('What and why');
    expect(why).toHaveTextContent('Reviewers lowered marginPct on 6 of 18 approvals');
    await vi.waitFor(() => expect(why).toHaveTextContent('sha256:9f2c41d07a r14, the active revision'));
    expect(why).toHaveTextContent('Stepsscore');
    expect(why).toHaveTextContent('Fileslessons/score.yaml');
    expect(within(why).getByRole('button', { name: 'Accept…' })).toBeInTheDocument();
    expect(within(why).getByRole('button', { name: 'Reject…' })).toBeInTheDocument();
    expect(within(why).queryByRole('button', { name: 'Revalidate' })).not.toBeInTheDocument();

    const validation = section('Validation');
    expect(validation).toHaveTextContent('Check, evals and backtest passed');
    expect(validation).toHaveTextContent(/Judged by the base revision’s evals/);
    const evals = within(validation).getByRole('table', { name: /^score · agent\.score/ });
    const flat = within(evals).getByRole('row', { name: /renovation-flat/ });
    expect(flat).toHaveTextContent('renovation-flatPassed · 0.92Passed · 0.95');
    const backtest = within(validation).getByRole('table', { name: 'Backtest over the whole sample' });
    expect(within(backtest).getByRole('row', { name: /Agreement with history/ })).toHaveTextContent(
      'Agreement with history80%84%+4 pts',
    );
    expect(within(backtest).getByRole('row', { name: /Agreement with reviewers/ })).toHaveTextContent('70%82%+12 pts');
    expect(within(backtest).getByRole('row', { name: /Failure rate/ })).toHaveTextContent('2%1.5%−0.5 pts');
    expect(within(backtest).getByRole('row', { name: /Past approvals dropped/ })).toHaveTextContent(
      'Past approvals dropped000',
    );
    expect(within(backtest).getByRole('row', { name: /Cost/ })).toHaveTextContent('$3.80$3.95+$0.15');
    expect(within(backtest).getByRole('row', { name: /Mean latency/ })).toHaveTextContent('2.4 s2.4 s+50 ms');
    expect(validation).toHaveTextContent('200 deliveries from imported evidence, 188 runs · done 96, approved 58');
    expect(within(validation).getByText('By changed step')).toBeInTheDocument();

    const evidence = section('Evidence');
    expect(within(evidence).getByRole('link', { name: 'Review pattern at analyst-review' })).toHaveAttribute(
      'href',
      '/p/listings-daily/insights#in_review_analyst',
    );
    expect(within(evidence).getByRole('link', { name: 'Drift at score' })).toBeInTheDocument();
    expect(within(evidence).getAllByRole('link', { name: /^it_/ })[0]).toHaveAttribute(
      'href',
      '/p/listings-daily/items/it_4c11ab90',
    );
    const history = within(section('History')).getAllByRole('listitem');
    expect(history.map((li) => li.textContent)).toEqual([
      expect.stringMatching(/^Created by improve, /) as unknown,
      'Validated: passed',
      expect.stringMatching(/^Ready since /) as unknown,
    ]);
  });

  it('shows the patch side by side from 1024 px, with its files and syntax', async () => {
    await open('p-0150');
    const patch = section('Patch');
    const files = within(patch).getByRole('navigation', { name: 'Files changed' });
    expect(
      within(files)
        .getAllByRole('listitem')
        .map((li) => li.textContent),
    ).toEqual([
      'pipeline.yamlChanged+2 −1',
      'actions/score-code/action.yamlNew+3 −0',
      'actions/score-code/main.goNew+14 −0',
    ]);
    const before = within(patch).getByRole('region', { name: 'pipeline.yaml, before' });
    const after = within(patch).getByRole('region', { name: 'pipeline.yaml, after' });
    expect(before).toHaveTextContent(/31−Removed: action: agent\.score/);
    expect(after).toHaveTextContent(/31\+Added: action: score-code/);
    expect(within(after).getByText('fallback')).toHaveClass('text-accent');
    const go = within(patch).getByRole('region', { name: 'actions/score-code/main.go, after' });
    expect(within(go).getByText('package')).toHaveClass('text-accent');
    expect(within(go).getByText('"github.com/monolithiclab/millstream/sdk"')).toHaveClass('text-human');
  });

  it('shows the patch unified below 1024 px', async () => {
    mockMatchMedia(() => false);
    await open('p-0152');
    const prompt = within(section('Patch')).getByRole('region', { name: 'actions/score/prompt.md' });
    expect(prompt).toHaveTextContent('Removed: Use `get_comparables` for the price per m² of the area.');
    expect(within(prompt).getByText('## Output')).toHaveClass('text-accent');
  });

  it('accepts a ready proposal naming the patch read, saying what follows', async () => {
    const api = new FakeApi();
    const { user } = await open('p-0151', api);
    await user.click(screen.getByRole('button', { name: 'Accept…' }));
    const dialog = screen.getByRole('dialog', { name: 'Accept proposal p-0151?' });
    expect(dialog).toHaveTextContent('a development revision (apply), a patch file (patch-file) or a pull request');
    await vi.waitFor(() => expect(dialog).toHaveTextContent('only while r14 is the active revision'));
    expect(dialog).toHaveTextContent('become stale');
    await user.click(within(dialog).getByRole('button', { name: 'Accept' }));
    const { patchSha256 } = demoFixtures.proposals.find((p) => p.id === 'p-0151')!;
    expect(api.calls).toEqual([{ method: 'acceptProposal', args: ['p-0151', { patchSha256 }] }]);
    expect(await screen.findByText('Proposal p-0151 accepted')).toBeInTheDocument();
    await vi.waitFor(() => expect(screen.getAllByText('Accepted')[0]).toBeInTheDocument());
    expect(screen.queryByRole('button', { name: 'Accept…' })).not.toBeInTheDocument();
    expect(section('History')).toHaveTextContent('Accepted by demo');
  });

  it('keeps the server’s refusal on the page when the base moved, the proposal now stale', async () => {
    const revisions = demoFixtures.revisions.map((r) => ({ ...r, active: r.id === 13 }));
    const { user } = await open('p-0151', new FakeApi({ ...demoFixtures, revisions }));
    await vi.waitFor(() => expect(section('What and why')).toHaveTextContent('r14, no longer active'));
    await user.click(screen.getByRole('button', { name: 'Accept…' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Accept' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(
      "Not done the active revision of listings-daily is no longer proposal p-0151's base: it is stale. The proposal was read again.",
    );
    expect(await screen.findByRole('button', { name: 'Revalidate' })).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('rejects with a reason, which is required', async () => {
    const api = new FakeApi();
    const { user } = await open('p-0152', api);
    await user.click(screen.getByRole('button', { name: 'Reject…' }));
    const dialog = screen.getByRole('dialog', { name: 'Reject proposal p-0152?' });
    const reason = within(dialog).getByRole('textbox', { name: 'Why? (required)' });
    expect(reason).toHaveFocus();
    expect(reason).toHaveAccessibleDescription(/given to later drafts/);
    const confirm = within(dialog).getByRole('button', { name: 'Reject' });
    expect(confirm).toBeDisabled();
    await user.type(reason, '  Comparables older than a year are still useful.  ');
    await user.click(confirm);
    expect(api.calls).toEqual([
      { method: 'rejectProposal', args: ['p-0152', { reason: 'Comparables older than a year are still useful.' }] },
    ]);
    expect(await screen.findByText('Proposal p-0152 rejected')).toBeInTheDocument();
    await vi.waitFor(() =>
      expect(section('History')).toHaveTextContent(/Rejected by demo, .*: Comparables older than a year/),
    );
    expect(screen.queryByRole('button', { name: 'Reject…' })).not.toBeInTheDocument();
  });

  it('revalidates a stale proposal, rebased onto the active revision', async () => {
    const api = new FakeApi();
    const { user } = await open('p-0149', api);
    expect(screen.getByText(/Its base is no longer the active revision/)).toBeInTheDocument();
    await vi.waitFor(() => expect(section('What and why')).toHaveTextContent('r13, no longer active'));
    expect(screen.queryByRole('button', { name: 'Accept…' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Revalidate' }));
    expect(api.calls).toEqual([{ method: 'revalidateProposal', args: ['p-0149'] }]);
    expect(await screen.findByText('Validated again: Ready')).toBeInTheDocument();
    await vi.waitFor(() => expect(section('What and why')).toHaveTextContent('r14, the active revision'));
  });

  it('says what failed: an eval that regressed, the new cases apart, and a backtest over the limit', async () => {
    await open('p-0152');
    const validation = section('Validation');
    expect(validation).toHaveTextContent('Failed');
    const evals = within(validation).getAllByRole('table', { name: /^score · agent\.score/ });
    expect(within(evals[0]!).getByRole('row', { name: /renovation-flat/ })).toHaveTextContent(
      'Passed · 0.92Failed · 0.41',
    );
    expect(within(validation).getByText('New and changed cases')).toBeInTheDocument();
    expect(within(evals[1]!).getByRole('row', { name: /recent-sales-only/ })).toHaveTextContent('Passed · 0.9');
    const backtest = within(validation).getByRole('table', { name: 'Backtest over the whole sample' });
    expect(within(backtest).getByRole('row', { name: /Agreement with history/ })).toHaveTextContent(
      '−5 pts, over the limit',
    );
    expect(within(backtest).getByRole('row', { name: /Agreement with reviewers/ })).toHaveTextContent(
      '−4 pts, over the limit',
    );
  });

  it('shows check warnings, divergence and the engine’s warnings', async () => {
    await open('p-0150');
    const validation = section('Validation');
    expect(validation).toHaveTextContent('Warningpipeline.yamlstep score: lessons given to a step');
    const note = within(validation).getByRole('note');
    expect(note).toHaveTextContent('Recorded on a revision other than the base: sha256:c7e19a05f2 (40 deliveries)');
    expect(within(note).getByText(/has diverged from the evidence bundle/)).toHaveAttribute('lang', 'en');
  });

  it('shows an empty sample as nothing to compare, and a rejected proposal without actions', async () => {
    await open('p-0147');
    const backtest = within(section('Validation')).getByRole('table', { name: 'Backtest over the whole sample' });
    expect(within(backtest).getByRole('row', { name: /Agreement with history/ })).toHaveTextContent(
      'Agreement with history——nothing to compare',
    );
    expect(section('Validation')).toHaveTextContent('the sample is empty');
    expect(screen.queryByRole('button', { name: /Accept|Reject|Revalidate/ })).not.toBeInTheDocument();
    expect(section('History')).toHaveTextContent(
      /Rejected by claire, .*: Analysts want to see every listing above €500k/,
    );
  });

  it('shows a proposal being validated, and an accepted one', async () => {
    await open('p-0153');
    expect(section('Validation')).toHaveTextContent('Validating now: check, evals and backtest.');
    expect(screen.getByText(/The engine is validating it/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Accept…' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reject…' })).toBeInTheDocument();
  });

  it('shows an accepted proposal with who accepted it, and an insight no longer listed', async () => {
    const api = new FakeApi({ ...demoFixtures, insights: [] });
    await open('p-0142', api);
    expect(section('Validation')).toHaveTextContent('Not validated yet.');
    expect(section('History')).toHaveTextContent(/Accepted by nicolas, /);
    expect(
      await within(section('Evidence')).findByRole('link', { name: 'Insight in_fail_score, no longer listed' }),
    ).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Accept|Reject|Revalidate/ })).not.toBeInTheDocument();
  });

  it('says when the proposal cannot be read', async () => {
    renderApp({ route: route('nope') });
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Proposal nope could not be loaded. proposal nope not found',
    );
    expect(screen.getByRole('heading', { level: 1, name: 'Proposal nope' })).toBeInTheDocument();
  });

  it('speaks French', async () => {
    await open('p-0151', new FakeApi(), 'fr');
    expect(screen.getByRole('heading', { level: 1, name: 'Proposition p-0151' })).toBeInTheDocument();
    expect(section('Validation')).toHaveTextContent('Vérification, évaluations et rejeu réussis');
    expect(screen.getByRole('button', { name: 'Accepter…' })).toBeInTheDocument();
  });
});
