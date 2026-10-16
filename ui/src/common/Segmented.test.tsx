import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { describe, expect, it } from 'vitest';
import { Segmented } from './Segmented';

function Range() {
  const [value, setValue] = useState<'1h' | '24h' | '7d'>('1h');
  return (
    <Segmented
      label="Range"
      value={value}
      onChange={setValue}
      options={[
        { value: '1h', label: '1 h' },
        { value: '24h', label: '24 h' },
        { value: '7d', label: '7 d' },
      ]}
    />
  );
}

describe('Segmented', () => {
  it('moves the choice with the arrow keys, Home and End, as a tab list, with one Tab stop', async () => {
    const user = userEvent.setup();
    render(<Range />);
    await user.tab();
    expect(screen.getByRole('button', { name: '1 h' })).toHaveFocus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('button', { name: '24 h' })).toHaveFocus();
    expect(screen.getByRole('button', { name: '24 h' })).toHaveAttribute('aria-pressed', 'true');
    await user.keyboard('{End}');
    expect(screen.getByRole('button', { name: '7 d', pressed: true })).toHaveFocus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('button', { name: '1 h', pressed: true })).toHaveFocus();
    await user.keyboard('{ArrowLeft}{Home}');
    expect(screen.getByRole('button', { name: '1 h', pressed: true })).toHaveFocus();
    await user.keyboard('{Enter}');
    expect(screen.getAllByRole('button').filter((b) => b.tabIndex === 0)).toHaveLength(1);
  });
});
