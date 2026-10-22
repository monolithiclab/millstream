import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { Select } from './Select';

describe('Select', () => {
  it('is a native select with a decorative chevron, the width on its wrapper', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    const { container } = render(
      <>
        <label htmlFor="theme">Theme</label>
        <Select id="theme" value="light" onChange={(e) => onChange(e.target.value)} className="w-40">
          <option value="light">Light</option>
          <option value="dark">Dark</option>
        </Select>
      </>,
    );
    const select = screen.getByRole('combobox', { name: 'Theme' });
    expect(screen.getAllByRole('option').map((o) => o.textContent)).toEqual(['Light', 'Dark']);
    await user.selectOptions(select, 'dark');
    expect(onChange).toHaveBeenCalledWith('dark');
    expect(container.querySelector('svg')).toHaveAttribute('aria-hidden', 'true');
    expect(select.parentElement).toHaveClass('w-40');
    expect(select).toHaveClass('appearance-none', 'min-h-[34px]');
  });
});
