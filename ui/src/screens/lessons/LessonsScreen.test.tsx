import { screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures, FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';

describe('LessonsScreen', () => {
  it('shows each step’s rules and examples with the source they cite', async () => {
    renderApp({ route: '/p/listings-daily/lessons' });
    const score = await screen.findByRole('region', { name: 'score' });
    const rules = within(score).getAllByRole('listitem');
    expect(rules).toHaveLength(3);
    expect(rules[0]).toHaveTextContent('Listings without a surface area are not scored');
    expect(rules[0]).toHaveTextContent('from proposal p-0142added 2026-10-03');
    expect(within(rules[1]!).getByRole('link', { name: 'from insight in_review_analyst' })).toHaveAttribute(
      'href',
      '/p/listings-daily/insights#in_review_analyst',
    );
    expect(within(score).getByRole('link', { name: 'from review dl_4b2e' })).toHaveAttribute(
      'href',
      '/api/v1/pipelines/listings-daily/reviews/dl_4b2e',
    );
    expect(score).toHaveTextContent('"reason": "groundFloorMainRoad"');
    expect(score).toHaveTextContent('Reviewer lowered the score');
    expect(within(score).getByRole('link', { name: 'Open in the graph' })).toHaveAttribute(
      'href',
      '/p/listings-daily/graph?step=score',
    );
    const enrich = screen.getByRole('region', { name: 'enrich' });
    expect(enrich).toHaveTextContent('No example.');
  });

  it('names an unknown source as it is, and a step without rules', async () => {
    const lessons = {
      'listings-daily': [{ step: 'tag', rules: [], examples: [{ input: 1, output: 2, source: 'chat/42' }] }],
    };
    renderApp({ route: '/p/listings-daily/lessons', api: new FakeApi({ ...demoFixtures, lessons }) });
    const tag = await screen.findByRole('region', { name: 'tag' });
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
    expect(await screen.findByRole('region', { name: 'score' })).toHaveTextContent('d’après la proposition p-0142');
  });
});
