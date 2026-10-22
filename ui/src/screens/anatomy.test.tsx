import { screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';
import { pipelinePath, screens, screenPath } from '../routes';
import { mockMatchMedia } from '../test/flow';
import { renderApp } from '../test/render';

const PIPELINE = 'listings-daily';

// Every screen, the three detail screens included, with whether it sits in a pipeline and has help (the spec's
// screen table).
const withHelp = new Set(['pipelines', 'graph', 'insights', 'improve', 'lessons']);
const pages = [
  ...screens.map((s) => ({
    name: s.id,
    route: screenPath(s, PIPELINE),
    pipeline: s.scope === 'pipeline',
    help: withHelp.has(s.id),
  })),
  { name: 'run', route: pipelinePath(PIPELINE, 'runs/run_0600'), pipeline: true, help: false },
  { name: 'item', route: pipelinePath(PIPELINE, 'items/it_7f3a9c21'), pipeline: true, help: false },
  { name: 'proposal', route: pipelinePath(PIPELINE, 'improve/p-0151'), pipeline: true, help: false },
];

// The page anatomy (ScreenPage): one h1, the context line of a pipeline screen leading with the pipeline, help only
// in the toggle, and no explanation left in the old right-aligned spot.
describe('page anatomy', () => {
  beforeEach(() => mockMatchMedia(() => false));

  it.each(pages)('$name', async ({ route, pipeline, help }) => {
    const { container } = renderApp({ route });
    await waitFor(() => expect(screen.queryByTestId('loading')).not.toBeInTheDocument());
    const [h1, ...more] = screen.getAllByRole('heading', { level: 1 });
    expect(more).toHaveLength(0);
    const page = h1!.closest('section')!;
    expect(page).toHaveAttribute('aria-labelledby', h1!.id);

    const context = within(page).queryByRole('navigation', { name: 'Breadcrumb' });
    if (pipeline) {
      expect(context).not.toBeNull();
      // The context line comes before the title.
      expect(context!.compareDocumentPosition(h1!) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
      // On the Graph screen the pipeline crumb is the page itself: text marked current, not a link to here.
      const first = within(context!).queryAllByRole('link')[0] ?? context!.querySelector('[aria-current="page"]');
      if (first!.tagName === 'A') expect(first).toHaveAttribute('href', pipelinePath(PIPELINE));
      else expect(route).toBe(pipelinePath(PIPELINE));
      expect(first).toHaveTextContent(PIPELINE);
    } else {
      expect(context).toBeNull();
    }

    const toggle = within(page).queryByRole('button', { name: 'About this page' });
    if (help) {
      expect(toggle).not.toBeNull();
      // Right after the title, in its row.
      expect(toggle!.closest('div')).toBe(h1!.parentElement);
    } else {
      expect(toggle).toBeNull();
    }

    expect(container.querySelectorAll('.text-ink-3.md\\:ml-auto')).toHaveLength(0);
  });
});
