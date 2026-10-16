import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { Dialog } from './Dialog';

describe('Dialog', () => {
  it('traps Tab inside and closes on the backdrop', async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <Dialog labelledBy="t" onClose={onClose}>
        <h2 id="t">Title</h2>
        <button type="button">One</button>
        <button type="button">Two</button>
      </Dialog>,
    );
    expect(screen.getByRole('dialog', { name: 'Title' })).toHaveAttribute('aria-modal', 'true');
    expect(screen.getByRole('button', { name: 'One' })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole('button', { name: 'Two' })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole('button', { name: 'One' })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole('button', { name: 'Two' })).toHaveFocus();
    await user.keyboard('{a}');
    await user.click(screen.getByTestId('backdrop'));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('focuses itself when it holds no control', async () => {
    const user = userEvent.setup();
    render(
      <Dialog labelledBy="t" onClose={() => undefined}>
        <h2 id="t">Empty</h2>
      </Dialog>,
    );
    expect(screen.getByRole('dialog')).toHaveFocus();
    await user.tab();
  });
});
