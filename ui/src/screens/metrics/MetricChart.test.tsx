import { render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { PreferencesProvider } from '../../preferences';
import { MetricChart } from './MetricChart';

// jsdom has no canvas: uPlot is replaced by a recorder of the options it was built with.
const built = vi.hoisted(() => [] as { series: Record<string, unknown>[]; data: unknown }[]);
const redraw = vi.hoisted(() => vi.fn());
vi.mock('uplot', () => ({
  default: class {
    constructor(opts: { series: Record<string, unknown>[] }, data: unknown) {
      built.push({ ...opts, data });
      queueMicrotask(() => (this.status = 1));
    }
    // uPlot is ready after its first commit, a microtask after construction.
    status = 0;
    setData = vi.fn();
    setSize = vi.fn();
    redraw = redraw;
    destroy = vi.fn();
  },
}));

type Filter = (u: { data: (number | null)[][] }, si: number, show: boolean) => number[] | null;

describe('MetricChart', () => {
  it('draws a point where a value has no neighbour to join, so a lone bucket is not invisible', () => {
    const values = [null, 4, null, null, 5, 6, null, 7];
    render(
      <PreferencesProvider>
        <MetricChart
          times={values.map((_, i) => i * 60_000)}
          series={[{ group: 'crawl', tone: '--ms-accent', dash: [], values }]}
          marks={[]}
          format={String}
          label="chart"
          onHover={() => undefined}
        />
      </PreferencesProvider>,
    );
    const points = built.at(-1)!.series[1]!.points as { show: boolean; filter: Filter };
    expect(points.show).toBe(false);
    const u = { data: [values.map((_, i) => i), values] };
    // Lines join 5 and 6; 4 and 7 (the last bucket) stand alone.
    expect(points.filter(u, 1, false)).toEqual([1, 7]);
    expect(
      points.filter(
        {
          data: [
            [0, 1],
            [1, 2],
          ],
        },
        1,
        false,
      ),
    ).toBeNull();
  });

  it('hands the plot its series at construction and redraws only once uPlot is ready', async () => {
    redraw.mockClear();
    const chart = (marks: { t: number; label: string }[], format: (value: number) => string) => (
      <PreferencesProvider>
        <MetricChart
          times={[0, 60_000]}
          series={[{ group: 'crawl', tone: '--ms-accent', dash: [], values: [null, 19] }]}
          marks={marks}
          format={format}
          label="chart"
          onHover={() => undefined}
        />
      </PreferencesProvider>
    );
    const { rerender } = render(chart([], String));
    // The series are in the constructor's data: the chart shows without a toggle.
    expect(built.at(-1)!.data).toEqual([
      [0, 60],
      [null, 19],
    ]);
    // A redraw ahead of uPlot's first commit left the chart empty.
    expect(redraw).not.toHaveBeenCalled();
    rerender(chart([{ t: 30_000, label: 'r2' }], String));
    expect(redraw).not.toHaveBeenCalled();

    await Promise.resolve();
    rerender(chart([{ t: 30_000, label: 'r3' }], String));
    expect(redraw).toHaveBeenCalledTimes(1);
    rerender(chart([{ t: 30_000, label: 'r3' }], (v) => `${v}%`));
    expect(redraw).toHaveBeenCalledTimes(2);
  });
});
