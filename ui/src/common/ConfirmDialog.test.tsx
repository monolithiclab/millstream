import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import { App } from '../App';
import { FakeApi } from '../api/fake';
import { fakeEventSource } from '../live/fakeEventSource';
import { ConfirmDialog } from './ConfirmDialog';

describe('ConfirmDialog', () => {
  it('says it is busy until the action settles', async () => {
    const user = userEvent.setup();
    let settle: () => void = () => undefined;
    const onConfirm = vi.fn(() => new Promise<void>((resolve) => (settle = resolve)));
    render(
      <MemoryRouter>
        <App api={new FakeApi()} eventSource={fakeEventSource} language="en">
          <ConfirmDialog title="Replay 2 items?" confirmLabel="Replay 2 items" onConfirm={onConfirm} onCancel={vi.fn()}>
            <p>They go back into the queue.</p>
          </ConfirmDialog>
        </App>
      </MemoryRouter>,
    );
    const status = screen.getByRole('status');
    expect(status).toBeEmptyDOMElement();
    await user.click(screen.getByRole('button', { name: 'Replay 2 items' }));
    expect(status).toHaveTextContent('Replay 2 items: in progress…');
    expect(status.parentElement).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled();
    await act(async () => settle());
    expect(status).toBeEmptyDOMElement();
    expect(status.parentElement).toHaveAttribute('aria-busy', 'false');
  });
});
