import { act, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';

// jsdom has no canvas: uPlot is replaced by a recorder of what the chart asked it to draw.
const plots = vi.hoisted(() => [] as { opts: Record<string, unknown>; data: unknown[]; destroyed: boolean }[]);
vi.mock('uplot', () => ({
  default: class {
    ctx = {
      save: vi.fn(),
      restore: vi.fn(),
      setLineDash: vi.fn(),
      beginPath: vi.fn(),
      moveTo: vi.fn(),
      lineTo: vi.fn(),
      stroke: vi.fn(),
      fillText: vi.fn(),
    };
    bbox = { left: 0, top: 0, width: 1000, height: 300 };
    cursor = { idx: 2 as number | null };
    entry: { opts: Record<string, unknown>; data: unknown[]; destroyed: boolean };
    constructor(opts: Record<string, unknown>, data: unknown[]) {
      this.entry = { opts, data, destroyed: false };
      plots.push(this.entry);
    }
    valToPos() {
      return 500;
    }
    setSize = vi.fn();
    setData = vi.fn((data: unknown[]) => {
      this.entry.data = data;
    });
    redraw = vi.fn();
    destroy() {
      this.entry.destroyed = true;
    }
  },
}));

const latest = () => plots.at(-1)!;
interface Hooks {
  setCursor: ((u: unknown) => void)[];
  draw: ((u: unknown) => void)[];
}

describe('MetricsScreen', () => {
  beforeEach(() => {
    plots.length = 0;
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  async function open(route = '/metrics', api = new FakeApi(), language: 'en' | 'fr' = 'en') {
    const view = renderApp({ route, api, language });
    await screen.findByRole('region', { name: language === 'fr' ? 'Graphique' : 'Chart' });
    await vi.waitFor(() => expect(plots.length).toBeGreaterThan(0));
    return view;
  }

  it('opens on delivery latency by step over 24 hours, with the series table', async () => {
    await open();
    expect(screen.getByRole('combobox', { name: 'Metric' })).toHaveValue('delivery_latency_seconds');
    expect(screen.getByRole('button', { name: 'Step' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: '24 h' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'Mean' })).toHaveAttribute('aria-pressed', 'true');
    expect(
      screen.getByRole('img', { name: /^delivery_latency_seconds, Mean over 24 h: 3 of 3 series shown/ }),
    ).toBeInTheDocument();
    const rows = within(screen.getByRole('region', { name: 'Series' })).getAllByRole('row');
    expect(rows.map((r) => r.textContent)).toEqual([
      'stepLastMedianMax',
      expect.stringMatching(/^enrich/) as unknown as string,
      expect.stringMatching(/^score\d/) as unknown as string,
      expect.stringMatching(/^dedupe\d/) as unknown as string,
    ]);
    expect(latest().data).toHaveLength(4);
    const series = latest().opts.series as { label?: string; dash?: number[] }[];
    expect(series.slice(1).map((s) => [s.label, s.dash])).toEqual([
      ['enrich', []],
      ['score', [6, 3]],
      ['dedupe', [1.5, 3]],
    ]);
  });

  it('keeps the view in the URL: metric, grouping, range, statistic and hidden series', async () => {
    const { user, location } = await open();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Metric' }), 'items_in');
    expect(location()).toBe('/metrics?m=items_in');
    expect(screen.getByRole('button', { name: 'Sum' })).toHaveAttribute('aria-pressed', 'true');
    await user.click(screen.getByRole('button', { name: '7 days' }));
    expect(location()).toBe('/metrics?m=items_in&range=7d');
    await user.click(screen.getByRole('button', { name: 'Max' }));
    expect(location()).toBe('/metrics?m=items_in&range=7d&stat=max');
    await screen.findByRole('region', { name: 'Chart' });
    await user.click(within(screen.getByRole('region', { name: 'Series' })).getByRole('checkbox', { name: /score/ }));
    expect(location()).toBe('/metrics?m=items_in&range=7d&stat=max&hide=score');
    expect(screen.getByRole('img', { name: /2 of 3 series shown/ })).toBeInTheDocument();
    await user.click(within(screen.getByRole('region', { name: 'Series' })).getByRole('checkbox', { name: /score/ }));
    expect(location()).toBe('/metrics?m=items_in&range=7d&stat=max');
    await user.click(screen.getByRole('button', { name: 'Nothing' }));
    expect(location()).toBe('/metrics?m=items_in&by=&range=7d&stat=max');
    await screen.findByRole('region', { name: 'Chart' });
    await user.click(screen.getByRole('button', { name: 'Label' }));
    expect(location()).toBe('/metrics?m=items_in&by=stream&range=7d&stat=max');
    const labelBox = screen.getByRole('combobox', { name: 'Label to group by' });
    await user.clear(labelBox);
    await user.type(labelBox, 'reason');
    expect(location()).toBe('/metrics?m=items_in&by=stream&range=7d&stat=max');
    await user.keyboard('{Enter}');
    expect(location()).toBe('/metrics?m=items_in&by=reason&range=7d&stat=max');
  });

  it('reopens a shared link exactly, with revision marks for the pipeline', async () => {
    await open('/metrics?m=delivery_cost_usd&p=listings-daily&range=30d&stat=sum&hide=dedupe');
    expect(screen.getByRole('combobox', { name: 'Metric' })).toHaveValue('delivery_cost_usd');
    expect(screen.getByRole('combobox', { name: 'Pipeline' })).toHaveValue('listings-daily');
    expect(screen.getByRole('button', { name: '30 days' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('checkbox', { name: 'Mark revisions' })).toBeChecked();
    expect(
      within(screen.getByRole('region', { name: 'Series' })).getByRole('checkbox', { name: /dedupe/ }),
    ).not.toBeChecked();
    await vi.waitFor(() => expect(plots.some((p) => p.data.length === 3)).toBe(true));
    const hooks = latest().opts.hooks as Hooks;
    const u = new (await import('uplot')).default({} as never, [] as never) as unknown as {
      ctx: { fillText: ReturnType<typeof vi.fn> };
    };
    hooks.draw[0]!(u);
    expect(u.ctx.fillText.mock.calls.map((c) => c[0])).toContain('r14 applied');
  });

  it('formats axes by unit and reports the hovered bucket in text', async () => {
    await open('/metrics?m=delivery_cost_usd');
    const opts = latest().opts as { axes: { values?: (u: unknown, s: number[]) => string[] }[]; hooks: Hooks };
    expect(opts.axes[1]!.values!(null, [0.25])).toEqual(['$0.25']);
    const chart = screen.getByRole('region', { name: 'Chart' });
    act(() => opts.hooks.setCursor[0]!({ cursor: { idx: 3 } }));
    expect(chart).toHaveTextContent(/enrich \$\d+\.\d+ · score \$/);
    act(() => opts.hooks.setCursor[0]!({ cursor: { idx: null } }));
    expect(chart).not.toHaveTextContent(/enrich \$/);
  });

  it('formats seconds as durations and counts as numbers', async () => {
    await open('/metrics?m=delivery_latency_seconds&stat=count');
    const axes = latest().opts.axes as { values: (u: unknown, s: number[]) => string[] }[];
    expect(axes[1]!.values(null, [1240])).toEqual(['1,240']);
  });

  it('picks a custom range from the current window', async () => {
    const { user, location } = await open('/metrics?m=items_in');
    await user.click(screen.getByRole('button', { name: 'Custom' }));
    expect(location()).toMatch(/range=custom&from=2026-10-14T06%3A31%3A00.000Z&to=2026-10-15T06%3A31%3A00.000Z/);
    const from = screen.getByLabelText('From');
    expect(from).toHaveAttribute('type', 'datetime-local');
    await user.clear(from);
    await user.type(from, '2026-10-14T12:00');
    expect(location()).toMatch(/from=2026-10-14T/);
  });

  it('offers the table alternative and copies the link', async () => {
    const { user } = await open('/metrics?m=items_in&range=1h');
    // After user-event's setup, which installs a clipboard of its own.
    const writeText = vi.fn(() => Promise.resolve());
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    const details = screen.getByText('Show the data as a table');
    await user.click(details);
    const table = screen.getByRole('table', { name: /items_in, Sum over 1 h/ });
    expect(within(table).getAllByRole('row').length).toBeGreaterThan(50);
    await user.click(screen.getByRole('button', { name: 'Copy link' }));
    expect(writeText).toHaveBeenCalledWith('http://localhost:3000/metrics?m=items_in&range=1h');
    expect(await screen.findByRole('button', { name: 'Link copied' })).toBeInTheDocument();
  });

  it('teaches when there are no metrics, and says when a range is empty', async () => {
    renderApp({ route: '/metrics', api: new FakeApi({ ...demoFixtures, metricNames: [] }) });
    expect(await screen.findByText('No metrics yet')).toBeInTheDocument();
  });

  it('says when a range holds no data', async () => {
    renderApp({ route: '/metrics?m=items_in', api: new FakeApi({ ...demoFixtures, metrics: () => [] }) });
    expect(await screen.findByText('No data in this range')).toBeInTheDocument();
  });

  it('retries a failed read', async () => {
    let fail = true;
    const api = new FakeApi();
    const real = api.getMetrics;
    api.getMetrics = (q) => (fail ? Promise.reject(new ApiError(400, 'invalid', 'to is before from')) : real(q));
    const { user } = renderApp({ route: '/metrics?m=items_in', api });
    expect(await screen.findByRole('alert')).toHaveTextContent('to is before from');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('region', { name: 'Chart' })).toBeInTheDocument();
  });

  it('reads a fixed range again each minute, updating the chart in place', async () => {
    const api = new FakeApi();
    const getMetrics = vi.spyOn(api, 'getMetrics');
    await open('/metrics?m=items_in', api);
    const created = plots.length;
    expect(getMetrics).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(60_000));
    expect(getMetrics).toHaveBeenCalledTimes(2);
    expect(plots).toHaveLength(created);
  });

  it('destroys the chart when it leaves', async () => {
    const { unmount } = await open();
    unmount();
    expect(plots.every((p) => p.destroyed)).toBe(true);
  });

  it('speaks French', async () => {
    await open('/metrics', new FakeApi(), 'fr');
    expect(screen.getByRole('heading', { level: 1, name: 'Métriques' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '7 jours' })).toBeInTheDocument();
    expect(screen.getByRole('img', { name: /3 séries affichées sur 3/ })).toBeInTheDocument();
  });
});
