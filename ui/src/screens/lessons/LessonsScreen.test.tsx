import { screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';

describe('LessonsScreen', () => {
  it('shows each step’s rules and examples with the source they cite', async () => {
    renderApp({ route: '/p/listings-daily/lessons' });
    const score = await screen.findByRole('region', { name: 'Step score' });
    const rules = within(score).getAllByRole('listitem');
    expect(rules).toHaveLength(3);
    expect(rules[0]).toHaveTextContent('Listings without a surface area are not scored');
    expect(rules[0]).toHaveTextContent('from proposal p-0142added 2026-10-03');
    expect(within(rules[0]!).getByRole('link', { name: 'from proposal p-0142' })).toHaveAttribute(
      'href',
      '/p/listings-daily/improve/p-0142',
    );
    expect(within(rules[1]!).getByRole('link', { name: 'from insight in_review_analyst' })).toHaveAttribute(
      'href',
      '/p/listings-daily/insights#in_review_analyst',
    );
    expect(within(score).getByRole('button', { name: 'from review dl_4b2e' })).toBeInTheDocument();
    expect(score).toHaveTextContent('"reason": "groundFloorMainRoad"');
    expect(score).toHaveTextContent('Reviewer lowered the score');
    expect(within(score).getByRole('link', { name: 'Open in the graph' })).toHaveAttribute(
      'href',
      '/p/listings-daily/graph?step=score',
    );
    const enrich = screen.getByRole('region', { name: 'Step enrich' });
    expect(enrich).toHaveTextContent('No example.');
  });

  it('opens a cited review as a readable panel: the decision, who, when, and the item', async () => {
    const api = new FakeApi();
    const review = await api.getReview('listings-daily', 'dl_4b2e');
    api.getReview = () =>
      Promise.resolve({
        ...review,
        state: 'decided',
        decision: 'approve',
        reviewer: 'claire',
        decidedAt: '2026-10-15T06:20:00Z',
        patch: [{ op: 'replace', path: '/marginPct', value: 12 }],
      });
    const { user, location } = renderApp({ route: '/p/listings-daily/lessons', api });
    const score = await screen.findByRole('region', { name: 'Step score' });
    await user.click(within(score).getByRole('button', { name: 'from review dl_4b2e' }));
    const dialog = await screen.findByRole('dialog', { name: 'Review dl_4b2e' });
    expect(await within(dialog).findByText('Approved with changes')).toBeInTheDocument();
    expect(dialog).toHaveTextContent('Byclaire');
    expect(dialog).toHaveTextContent('replace /marginPct → 12');
    expect(dialog).not.toHaveTextContent('"deliveryId"');
    await user.click(within(dialog).getByRole('link', { name: 'T3 · 68 m² · Paris 19e' }));
    expect(location()).toBe(`/p/listings-daily/items/${review.item.id}`);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('names an unknown source as it is, and a step without rules', async () => {
    const lessons = {
      'listings-daily': [{ step: 'tag', rules: [], examples: [{ input: 1, output: 2, source: 'chat/42' }] }],
    };
    renderApp({ route: '/p/listings-daily/lessons', api: new FakeApi({ ...demoFixtures, lessons }) });
    const tag = await screen.findByRole('region', { name: 'Step tag' });
    expect(tag).toHaveTextContent('No rule.');
    expect(tag).toHaveTextContent('chat/42');
  });

  it('teaches where lessons come from', async () => {
    renderApp({ route: '/p/photo-tagging/lessons' });
    expect(await screen.findByText('No lessons yet')).toBeInTheDocument();
    expect(screen.getByText('lessons/score.yaml')).toBeInTheDocument();
  });

  it('retries a failed read, in French', async () => {
    let fail = true;
    const api = new FakeApi();
    const real = api.listLessons;
    api.listLessons = (p) => (fail ? Promise.reject(new ApiError(500, 'internal', 'boom')) : real(p));
    const { user } = renderApp({ route: '/p/listings-daily/lessons', api, language: 'fr' });
    expect(await screen.findByRole('alert')).toHaveTextContent('Le chargement a échoué. boom');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Réessayer' }));
    expect(await screen.findByRole('region', { name: 'Étape score' })).toHaveTextContent(
      'd’après la proposition p-0142',
    );
  });
});
