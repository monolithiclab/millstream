import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';
import { App } from '../../App';
import { FakeApi } from '../../api/fake';
import type { Delivery } from '../../api/schema';
import { fakeEventSource } from '../../live/fakeEventSource';
import { Journey } from './Journey';

const base: Delivery = {
  id: 'dl_x',
  pipeline: 'listings-daily',
  runId: 'run_0600',
  step: 'score',
  itemId: 'it_x',
  generation: 1,
  state: 'done',
  attempt: 1,
  costUsd: 0,
  createdAt: '2026-10-15T06:00:00Z',
  waitMs: 0,
  workMs: 1200,
};

const delivery = (id: string, change: Partial<Delivery>): Delivery => ({ ...base, id, step: id, ...change });

function renderJourney(deliveries: Delivery[]) {
  render(
    <App api={new FakeApi()} eventSource={fakeEventSource} language="en">
      <Journey pipeline="listings-daily" deliveries={deliveries} now={Date.parse('2026-10-15T06:30:00Z')} />
    </App>,
  );
}

const row = (step: string) => screen.getByRole('button', { name: new RegExp(`^${step}`) });

describe('Journey', () => {
  it('says what each delivery state did, with and without its detail', () => {
    renderJourney([
      delivery('filter-why', { state: 'filtered', filterReason: 'price over budget' }),
      delivery('filter-bare', { state: 'filtered' }),
      delivery('lease-owner', { state: 'leased', leaseOwner: 'worker-2' }),
      delivery('lease-bare', { state: 'leased' }),
      delivery('pend-retry', { state: 'pending', nextAttemptAt: '2026-10-15T06:42:00Z' }),
      delivery('pend-bare', { state: 'pending' }),
      delivery('cancel', { state: 'cancelled' }),
      delivery('retried', { state: 'done', attempt: 3, costUsd: 0.02 }),
    ]);
    expect(row('filter-why')).toHaveTextContent('filtered: price over budget');
    expect(row('filter-bare')).toHaveTextContent('held back by the filter');
    expect(row('lease-owner')).toHaveTextContent('running on worker-2');
    expect(row('lease-bare')).toHaveTextContent(/Running · running/);
    expect(row('pend-retry')).toHaveTextContent(/retry at \d{2}:\d{2}/);
    expect(row('pend-bare')).toHaveTextContent('waiting for a slot');
    expect(row('cancel')).toHaveTextContent('its run was cancelled');
    expect(row('retried')).toHaveTextContent('3 attempts');
    // Nothing is open until a row is pressed, as no delivery is dead.
    expect(screen.queryAllByRole('button', { expanded: true })).toEqual([]);
  });

  it('opens a row on the error of attempts that could not be read', async () => {
    renderJourney([delivery('missing', { state: 'pending' })]);
    const user = userEvent.setup();
    await user.click(row('missing'));
    expect(row('missing')).toHaveAttribute('aria-expanded', 'true');
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('The attempts could not be loaded.');
    expect(within(alert).getByRole('button', { name: 'Retry' })).toBeInTheDocument();
    await user.click(row('missing'));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('says so when no step has read the item', () => {
    renderJourney([]);
    expect(screen.getByText('No step has read this item yet.')).toBeInTheDocument();
  });
});
