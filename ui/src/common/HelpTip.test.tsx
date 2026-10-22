import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { I18nextProvider } from 'react-i18next';
import { describe, expect, it } from 'vitest';
import { createI18n } from '../i18n';
import { HelpTip } from './HelpTip';

function renderTip() {
  return render(
    <I18nextProvider i18n={createI18n('en')}>
      <HelpTip>
        Pipelines are files: edit, then <code>millstream apply</code>.
      </HelpTip>
      <p>Outside</p>
    </I18nextProvider>,
  );
}

describe('HelpTip', () => {
  it('opens on Enter as a disclosure and closes on Escape, focus back on the button', async () => {
    const user = userEvent.setup();
    renderTip();
    const button = screen.getByRole('button', { name: 'About this page' });
    expect(button).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText(/Pipelines are files/)).not.toBeInTheDocument();
    await user.tab();
    await user.keyboard('{Enter}');
    expect(button).toHaveAttribute('aria-expanded', 'true');
    const popover = screen.getByText(/Pipelines are files/);
    expect(popover).toBeVisible();
    expect(button.getAttribute('aria-controls')).toBe(popover.id);
    await user.keyboard('{Escape}');
    expect(button).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText(/Pipelines are files/)).not.toBeInTheDocument();
    expect(button).toHaveFocus();
  });

  it('closes on a second click and on a click outside', async () => {
    const user = userEvent.setup();
    renderTip();
    const button = screen.getByRole('button', { name: 'About this page' });
    await user.click(button);
    await user.click(screen.getByText(/Pipelines are files/));
    expect(button).toHaveAttribute('aria-expanded', 'true');
    await user.click(button);
    expect(button).toHaveAttribute('aria-expanded', 'false');
    await user.click(button);
    await user.click(screen.getByText('Outside'));
    expect(button).toHaveAttribute('aria-expanded', 'false');
  });

  it('takes a label for field help', () => {
    render(
      <I18nextProvider i18n={createI18n('en')}>
        <HelpTip label="About Where">CEL</HelpTip>
      </I18nextProvider>,
    );
    expect(screen.getByRole('button', { name: 'About Where' })).toBeInTheDocument();
  });
});
