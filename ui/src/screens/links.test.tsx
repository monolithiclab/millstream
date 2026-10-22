import { screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';
import { mockMatchMedia } from '../test/flow';
import { renderApp } from '../test/render';

// The links between screens land on what they name: the demo dataset holds an item for every inbox card, timeline
// bar and insight sample, and each screen reads the URL the other builds.
describe('links between screens', () => {
  beforeEach(() => mockMatchMedia(() => false));

  it('opens the item of an insight sample, and the inbox card the item waits in', async () => {
    const { user, location } = renderApp({ route: '/p/listings-daily/insights' });
    const [sample] = await screen.findAllByRole('link', { name: 'it_88a1c3f0' });
    await user.click(sample!);
    expect(await screen.findByRole('heading', { level: 1, name: 'Studio · 19 m² · Paris 20e' })).toBeInTheDocument();
    await user.click(screen.getByRole('link', { name: 'Review now' }));
    expect(location()).toBe('/inbox?pipeline=listings-daily&delivery=dl_88f2');
    const panel = await screen.findByRole('region', { name: 'Item under review' });
    expect(within(panel).getByRole('heading', { level: 2 })).toHaveTextContent('Studio · 19 m² · Paris 20e');
  });

  it('opens the step an insight names in the graph, with its panel', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/insights' });
    await user.click(await screen.findByRole('link', { name: 'enrich' }));
    expect(await screen.findByRole('complementary', { name: 'enrich' })).toBeInTheDocument();
  });

  it('opens the current run from the graph', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/graph' });
    await user.click(await screen.findByRole('link', { name: 'Run 06:00 so far' }));
    expect(await screen.findByRole('heading', { level: 1, name: 'Run 06:00 · Thu, Oct 15' })).toBeInTheDocument();
  });

  it('opens the step a lesson belongs to in the graph', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/lessons' });
    const score = await screen.findByRole('region', { name: 'Step score' });
    await user.click(within(score).getByRole('link', { name: 'Open in the graph' }));
    expect(await screen.findByRole('complementary', { name: 'score' })).toBeInTheDocument();
  });

  it('opens an item from a bar of the density strip, and its run from the item', async () => {
    const { user, location } = renderApp({ route: '/p/listings-daily/runs/run_0600' });
    await user.click(await screen.findByRole('button', { name: 'score' }));
    await user.click(screen.getAllByRole('button', { name: /deliveries$/ })[0]!);
    const items = screen.getByRole('list', { name: 'Items in this bar' });
    await user.click(within(items).getAllByRole('link')[0]!);
    expect(await screen.findByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(location()).toMatch(/^\/p\/listings-daily\/items\/it_/);
    await user.click(await screen.findByRole('link', { name: 'run_0600' }));
    expect(await screen.findByRole('heading', { level: 1, name: 'Run 06:00 · Thu, Oct 15' })).toBeInTheDocument();
  });

  it('opens the dead letters of a run report’s failures, and the item of a dead letter', async () => {
    const { user } = renderApp({ route: '/p/listings-daily/runs/run_0600' });
    const report = await screen.findByRole('complementary', { name: 'Run report' });
    await user.click(within(report).getByRole('link', { name: /^×2score · schema/ }));
    const group = await screen.findByRole('region', { name: /^Group at score/ });
    await user.click(within(group).getAllByRole('link', { name: /^it_dl_/ })[0]!);
    expect(await screen.findByRole('heading', { level: 1 })).toBeInTheDocument();
  });
});
