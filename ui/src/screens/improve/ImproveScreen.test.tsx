import { screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures, demoImprovePipeline } from '../../api/demo';
import { FakeApi, type FakeOptions } from '../../api/fake';
import { renderApp } from '../../test/render';

const ROUTE = '/p/listings-daily/improve';
const row = (name: RegExp) => screen.getByRole('link', { name }).closest('li')!;

// A deployment where improve is on: the improve pipeline is loaded.
const withImprove = (options: FakeOptions = {}) =>
  new FakeApi({ ...demoFixtures, pipelines: [...demoFixtures.pipelines, demoImprovePipeline] }, options);

describe('ImproveScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  async function open(route = ROUTE, api = new FakeApi(), language: 'en' | 'fr' = 'en') {
    const view = renderApp({ route, api, language });
    await screen.findByRole('heading', { level: 1, name: language === 'en' ? 'Improve' : 'Améliorer' });
    return view;
  }

  it('lists the proposals to decide, newest first, each with its validation in a line', async () => {
    await open();
    const list = await screen.findByRole('list', { name: 'Proposals, newest first' });
    expect(within(list).getAllByRole('listitem')).toHaveLength(4);
    expect(screen.getByRole('button', { name: 'To decide 4' })).toHaveAttribute('aria-pressed', 'true');
    for (const tab of ['Stale 1', 'Accepted 1', 'Rejected 1']) {
      expect(screen.getByRole('button', { name: tab })).toBeInTheDocument();
    }
    const ready = row(/^p-0151 · Reviewers lowered marginPct/);
    expect(within(ready).getByRole('link', { name: /^p-0151/ })).toHaveAttribute(
      'href',
      '/p/listings-daily/improve/p-0151',
    );
    expect(ready).toHaveTextContent(/^ReadyLessonsscoreby improve 10 hours ago/);
    expect(ready).toHaveTextContent('Validation passed');
    expect(ready).toHaveTextContent('lessons/score.yaml');
    expect(row(/^p-0152/)).toHaveTextContent('DraftingLessons');
    expect(row(/^p-0152/)).toHaveTextContent('Validation failed: evals, backtest');
    expect(row(/^p-0153/)).toHaveTextContent('ValidatingConfig');
    expect(row(/^p-0153/)).toHaveTextContent('Not validated yet');
  });

  it('filters by state and kind through the URL', async () => {
    const { user, location } = await open();
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Kind' }), 'lessons');
    expect(location()).toBe(`${ROUTE}?kind=lessons`);
    const list = screen.getByRole('list', { name: 'Proposals, newest first' });
    expect(within(list).getAllByRole('listitem')).toHaveLength(2);
    await user.click(screen.getByRole('button', { name: 'Rejected 1' }));
    expect(location()).toBe(`${ROUTE}?kind=lessons&state=rejected`);
    // The kind chosen is not among the rejected ones: it does not filter them.
    expect(screen.queryByRole('combobox', { name: 'Kind' })).not.toBeInTheDocument();
    expect(row(/^p-0147/)).toHaveTextContent(
      'Rejected: Analysts want to see every listing above €500k, whatever its score.',
    );
    await user.click(screen.getByRole('button', { name: 'To decide 4' }));
    expect(location()).toBe(`${ROUTE}?kind=lessons`);
  });

  it('teaches when no proposal waits, and says when a state is empty', async () => {
    const { user } = await open(ROUTE, new FakeApi({ ...demoFixtures, proposals: [] }));
    expect(await screen.findByText('No proposal to decide')).toBeInTheDocument();
    expect(screen.getByText(/Proposals come from the improve pipeline/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Stale 0' }));
    expect(screen.getByText('No proposal in this state')).toBeInTheDocument();
  });

  it('holds Propose a fix back, saying why, while improve is off', async () => {
    await open();
    const propose = screen.getByRole('button', { name: 'Propose a fix' });
    await vi.waitFor(() => expect(propose).toHaveAccessibleDescription(/^Improve is off in this deployment/));
    expect(propose).toBeDisabled();
    // The reason is on screen, in the title's row beside the button, not only in the button's description.
    const reason = screen.getByText(/^Improve is off in this deployment/);
    expect(reason).toBeVisible();
    expect(reason.closest('section')!.querySelector('h1')!.parentElement!.parentElement).toContainElement(reason);
  });

  it('holds Propose a fix back in production, whatever improve says', async () => {
    await open(ROUTE, withImprove({ environment: 'production' }));
    await vi.waitFor(() =>
      expect(screen.getByRole('button', { name: 'Propose a fix' })).toHaveAccessibleDescription(
        /^Production never runs the improve pipeline/,
      ),
    );
  });

  it('says when the improve pipeline has no manual trigger', async () => {
    const auto = { ...demoImprovePipeline, triggers: demoImprovePipeline.triggers.filter((x) => x.kind !== 'manual') };
    await open(ROUTE, new FakeApi({ ...demoFixtures, pipelines: [...demoFixtures.pipelines, auto] }));
    await vi.waitFor(() =>
      expect(screen.getByRole('button', { name: 'Propose a fix' })).toHaveAccessibleDescription(
        'The improve pipeline has no manual trigger.',
      ),
    );
  });

  it('runs the improve pipeline for the pipeline', async () => {
    const api = withImprove();
    const { user } = await open(ROUTE, api);
    const propose = screen.getByRole('button', { name: 'Propose a fix' });
    await vi.waitFor(() => expect(propose).toBeEnabled());
    await user.click(propose);
    expect(await screen.findByText('Improve run run_1 started: its proposals appear here.')).toBeInTheDocument();
    expect(api.calls).toEqual([
      {
        method: 'triggerRun',
        args: ['improve', { payload: { pipeline: 'listings-daily' }, requestId: expect.any(String) as unknown }],
      },
    ]);
  });

  it('has no insights tab: the board lives on the Insights screen', async () => {
    await open();
    await screen.findByRole('list', { name: 'Proposals, newest first' });
    expect(screen.queryByRole('group', { name: 'Improve view' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Insights/ })).not.toBeInTheDocument();
  });

  it('says when the improve pipeline does not start', async () => {
    const api = withImprove();
    api.triggerRun = () => Promise.reject(new ApiError(409, 'conflict', 'pipeline improve is disabled'));
    const { user } = await open(ROUTE, api);
    const propose = screen.getByRole('button', { name: 'Propose a fix' });
    await vi.waitFor(() => expect(propose).toBeEnabled());
    await user.click(propose);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'The improve pipeline did not start: pipeline improve is disabled',
    );
  });

  it('speaks French', async () => {
    await open(ROUTE, new FakeApi(), 'fr');
    expect(await screen.findByRole('button', { name: 'À décider 4' })).toBeInTheDocument();
    expect(row(/^p-0151/)).toHaveTextContent('PrêteLeçons');
    expect(screen.getByRole('button', { name: 'Proposer une correction' })).toBeDisabled();
  });
});
