import { render, screen } from '@testing-library/react';
import type { ReactElement } from 'react';
import { I18nextProvider } from 'react-i18next';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { Breadcrumb } from '../common/Breadcrumb';
import { createI18n } from '../i18n';
import { ScreenPage } from './ScreenPage';

function renderPage(page: ReactElement) {
  return render(
    <I18nextProvider i18n={createI18n('en')}>
      <MemoryRouter>{page}</MemoryRouter>
    </I18nextProvider>,
  );
}

// The slots' order in the document: data-slot markers, the h1 and the help button.
function order(root: HTMLElement): string[] {
  return [...root.querySelectorAll('[data-slot], h1, button[aria-expanded]')].map((el) =>
    el.tagName === 'H1' ? 'h1' : el.tagName === 'BUTTON' ? 'help' : el.getAttribute('data-slot')!,
  );
}

describe('ScreenPage', () => {
  it('lays out every slot in the anatomy order', () => {
    renderPage(
      <ScreenPage
        title="Runs"
        context={<span data-slot="context">m2-fixture</span>}
        help="Runs explained"
        status={<span data-slot="status">running</span>}
        actions={<span data-slot="actions">Trigger</span>}
        summary={<span data-slot="summary">3 runs</span>}
        toolbar={<span data-slot="toolbar">tabs</span>}
      >
        <p data-slot="children">content</p>
      </ScreenPage>,
    );
    const page = screen.getByRole('region', { name: 'Runs' });
    expect(order(page)).toEqual(['context', 'h1', 'help', 'status', 'actions', 'summary', 'toolbar', 'children']);
    expect(document.title).toBe('Runs · Millstream');
  });

  it('renders only the h1 and the children when no slot is given', () => {
    renderPage(
      <ScreenPage title="Nodes">
        <p>content</p>
      </ScreenPage>,
    );
    const page = screen.getByRole('region', { name: 'Nodes' });
    expect(page.children).toHaveLength(2);
    expect(page.children[0]).toHaveTextContent(/^Nodes$/);
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('keeps the h1, and the focus on it, when a status and actions arrive', () => {
    const page = (ready: boolean) => (
      <ScreenPage title="Run" status={ready ? 'running' : undefined} actions={ready ? 'Cancel' : undefined}>
        <p>content</p>
      </ScreenPage>
    );
    const { rerender } = renderPage(page(false));
    const h1 = screen.getByRole('heading', { level: 1 });
    h1.focus();
    rerender(
      <I18nextProvider i18n={createI18n('en')}>
        <MemoryRouter>{page(true)}</MemoryRouter>
      </I18nextProvider>,
    );
    expect(screen.getByRole('heading', { level: 1 })).toBe(h1);
    expect(h1).toHaveFocus();
  });

  it('draws the breadcrumb as links separated by slashes, pipeline names mono', () => {
    renderPage(
      <Breadcrumb
        items={[
          { label: 'm2-fixture', to: '/pipelines/m2-fixture', mono: true },
          { label: 'Runs', to: '/r' },
          { label: 'Here' },
        ]}
      />,
    );
    const nav = screen.getByRole('navigation', { name: 'Breadcrumb' });
    expect(nav).toHaveTextContent('m2-fixture/Runs/Here');
    expect(screen.getByRole('link', { name: 'm2-fixture' })).toHaveClass('font-mono');
    expect(screen.getByRole('link', { name: 'Runs' })).not.toHaveClass('font-mono');
    expect(screen.getAllByRole('link')).toHaveLength(2);
  });
});
