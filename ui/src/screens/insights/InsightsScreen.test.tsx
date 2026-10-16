import { screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW } from '../../api/demo';
import { demoFixtures, FakeApi } from '../../api/fake';
import type { Insight } from '../../api/schema';
import { renderApp } from '../../test/render';

const ROUTE = '/p/listings-daily/insights';
const card = (name: RegExp) => screen.getByRole('article', { name });

describe('InsightsScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  async function open(route = ROUTE, api = new FakeApi(), language: 'en' | 'fr' = 'en') {
    const view = renderApp({ route, api, language });
    await screen.findAllByRole('article');
    return view;
  }

  it('lists what needs attention, by kind, each with its evidence and a way in', async () => {
    await open();
    expect(screen.getByRole('button', { name: 'Needs attention 3' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'Resolved 1' })).toBeInTheDocument();
    expect(screen.getAllByRole('article')).toHaveLength(3);

    const slow = card(/enrich is slowing runs down/);
    expect(slow).toHaveTextContent('OpenSlow stepenrichraised 2 days ago');
    expect(slow).toHaveTextContent('It holds 34% of the critical path across the last runs (3).');
    expect(slow).toHaveTextContent('p95 latency4.1 sbaseline 1.3 s');
    expect(within(slow).getByRole('link', { name: 'Open in metrics' })).toHaveAttribute(
      'href',
      '/metrics?m=delivery_latency_seconds&p=listings-daily&range=7d',
    );
    expect(within(slow).getByRole('link', { name: 'enrich' })).toHaveAttribute(
      'href',
      '/p/listings-daily/graph?step=enrich',
    );
    expect(within(slow).getAllByRole('link', { name: /^it_/ })[0]).toHaveAttribute(
      'href',
      '/p/listings-daily/items/it_88a1c3f0',
    );

    const failures = card(/2 failures with the same error at score/);
    expect(failures).toHaveTextContent('schema: output schema: "score" missing');
    expect(failures).toHaveTextContent('Recovered on retry0%');
    expect(within(failures).getByRole('link', { name: 'Open dead letters' })).toHaveAttribute(
      'href',
      '/p/listings-daily/dead-letters',
    );

    const review = card(/How reviewers decide at analyst-review/);
    expect(review).toHaveTextContent('Acknowledged');
    expect(review).toHaveTextContent('Reviewers edit these fields most: /marginPct.');
    expect(review).toHaveTextContent('Intervention rate38%');
    expect(within(review).queryByRole('button', { name: 'Acknowledge' })).not.toBeInTheDocument();
  });

  it('filters by kind and step', async () => {
    const { user } = await open();
    await user.click(screen.getByRole('checkbox', { name: /Slow step/ }));
    expect(screen.getAllByRole('article')).toHaveLength(2);
    await user.selectOptions(screen.getByRole('combobox', { name: 'Step' }), 'score');
    expect(screen.getAllByRole('article')).toHaveLength(1);
    await user.click(screen.getByRole('checkbox', { name: /Failure cluster/ }));
    expect(screen.getByText('Nothing needs attention')).toBeInTheDocument();
  });

  it('drops a step filter that does not apply to the tab shown', async () => {
    const { user } = await open();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Step' }), 'score');
    expect(screen.getAllByRole('article')).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: 'Muted 1' }));
    expect(card(/No node can run render/)).toBeInTheDocument();
  });

  it('switches to resolved and muted insights through the URL', async () => {
    const { user, location } = await open();
    await user.click(screen.getByRole('button', { name: 'Resolved 1' }));
    expect(location()).toBe('/p/listings-daily/insights?state=resolved');
    const cost = card(/score cost \$7\.62 over the window/);
    expect(cost).toHaveTextContent('Cost per output$0.02baseline $0.02');
    expect(within(cost).queryByRole('button')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Muted 1' }));
    expect(card(/No node can run render/)).toHaveTextContent('unsupported: no node offers runtime docker');
    expect(within(card(/No node can run render/)).getByRole('link', { name: 'Open nodes' })).toHaveAttribute(
      'href',
      '/nodes',
    );
  });

  it('acknowledges and mutes, then reads the insights again', async () => {
    const api = new FakeApi();
    const { user } = await open(ROUTE, api);
    await user.click(within(card(/enrich is slowing/)).getByRole('button', { name: 'Acknowledge' }));
    expect(api.calls).toEqual([{ method: 'acknowledgeInsight', args: ['listings-daily', 'in_slow_enrich'] }]);
    await vi.waitFor(() => expect(within(card(/enrich is slowing/)).getByText('Acknowledged')).toBeInTheDocument());
    await user.click(within(card(/2 failures/)).getByRole('button', { name: 'Mute' }));
    await vi.waitFor(() => expect(screen.getAllByRole('article')).toHaveLength(2));
    expect(screen.getByRole('button', { name: 'Muted 2' })).toBeInTheDocument();
  });

  it('says when an action fails', async () => {
    const api = new FakeApi();
    api.muteInsight = () => Promise.reject(new ApiError(404, 'notFound', 'insight in_x not found'));
    const { user } = await open(ROUTE, api);
    await user.click(within(card(/enrich is slowing/)).getByRole('button', { name: 'Mute' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The insight could not be changed. insight in_x not found');
    await user.click(within(alert).getByRole('button', { name: 'Dismiss' }));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('shows an unknown kind with the engine summary, and missing figures as dashes', async () => {
    const odd: Insight = {
      ...demoFixtures.insights![0]!,
      id: 'in_drift',
      kind: 'drift',
      summary: 'output drifted',
      evidence: {},
    };
    const bare: Insight = { ...demoFixtures.insights![1]!, id: 'in_bare', evidence: {} };
    await open(ROUTE, new FakeApi({ ...demoFixtures, insights: [odd, bare] }));
    expect(screen.getByText('output drifted')).toHaveAttribute('lang', 'en');
    expect(card(/0 failures/)).toHaveTextContent('Share of attempts—');
  });

  it('teaches when nothing needs attention, and retries a failed read', async () => {
    let fail = true;
    const api = new FakeApi({ ...demoFixtures, insights: [] });
    const real = api.listInsights;
    api.listInsights = (q) => (fail ? Promise.reject(new ApiError(500, 'internal', 'boom')) : real(q));
    const { user } = renderApp({ route: ROUTE, api });
    expect(await screen.findByRole('alert')).toHaveTextContent('boom');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByText('Nothing needs attention')).toBeInTheDocument();
    expect(screen.getByText('No insight here.')).toBeInTheDocument();
  });

  it('speaks French', async () => {
    await open(ROUTE, new FakeApi(), 'fr');
    expect(screen.getByRole('heading', { level: 1, name: 'Constats' })).toBeInTheDocument();
    expect(card(/enrich ralentit les runs/)).toHaveTextContent('Elle occupe 34 % du chemin critique');
  });
});
