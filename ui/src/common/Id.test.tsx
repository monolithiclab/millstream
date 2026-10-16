import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Id, shortId } from './Id';

describe('Id', () => {
  it('shows a short id whole, without a title', () => {
    render(<Id value="run_0600" className="text-ink" />);
    const id = screen.getByText('run_0600');
    expect(id).not.toHaveAttribute('title');
    expect(id).toHaveClass('font-mono', 'text-ink');
  });

  it('truncates a long id in the middle, with the full value on hover and for screen readers', () => {
    const { container } = render(<Id value="it_7f3a9c21e5b04d9c21" />);
    const id = container.firstElementChild!;
    expect(id).toHaveAttribute('title', 'it_7f3a9c21e5b04d9c21');
    expect(screen.getByText('it_7f3a…9c21')).toHaveAttribute('aria-hidden', 'true');
    expect(screen.getByText('it_7f3a9c21e5b04d9c21')).toHaveClass('sr-only');
    expect(shortId('12345678901234')).toBe('12345678901234');
  });
});
