import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { useMediaQuery } from '../../common/motion';
import { usePreferences } from '../../preferences';
import { useLatest } from '../../useLatest';

export interface ChartSeries {
  group: string;
  // A colour token (--ms-accent), resolved for the current theme when the chart is drawn.
  tone: string;
  dash: number[];
  values: (number | null)[];
}

export interface Mark {
  t: number;
  label: string;
}

// A token's colour as the browser computes it for the current theme (tokens are light-dark() values, which a canvas
// cannot read).
function resolve(host: HTMLElement, token: string): string {
  const probe = document.createElement('span');
  probe.style.color = `var(${token})`;
  host.appendChild(probe);
  const color = getComputedStyle(probe).color;
  probe.remove();
  return color;
}

const HEIGHT = 320;

// The buckets a line cannot show: a value between two empty buckets (or an edge) is a path of zero length, so with
// points hidden a series that has data only there draws nothing. uPlot draws the points this returns.
const lonePoints = (u: uPlot, si: number, show: boolean): number[] | null => {
  if (show) return null;
  const values = u.data[si] ?? [];
  const lone: number[] = [];
  for (let i = 0; i < values.length; i++)
    if (values[i] != null && values[i - 1] == null && values[i + 1] == null) lone.push(i);
  return lone.length > 0 ? lone : null;
};

// The uPlot line chart of the explorer: one line per visible series, dashes as well as colours, revision marks as
// dashed rules, and the hovered bucket reported to the caller (the readout and the data table carry the values for
// people who cannot see the canvas).
export function MetricChart({
  times,
  series,
  marks,
  format,
  label,
  onHover,
}: {
  times: number[];
  series: ChartSeries[];
  marks: Mark[];
  format: (value: number) => string;
  label: string;
  onHover: (index: number | null) => void;
}) {
  const host = useRef<HTMLDivElement>(null);
  const { theme } = usePreferences();
  // Redraws when the system colour scheme changes under a "follow the system" theme.
  const scheme = useMediaQuery('(prefers-color-scheme: dark)', false);
  const hover = useLatest(onHover);
  // Read by the plot's hooks, so a new format or new marks only need a redraw.
  const latest = useLatest({ marks, format, series });
  const plotRef = useRef<uPlot | null>(null);
  // The plot is rebuilt only when its lines change (count, colour, dash) or the theme does; data is set in place.
  const shape = series.map((s) => `${s.group}|${s.tone}|${s.dash.join(',')}`).join(';');
  const data = useLatest<uPlot.AlignedData>([times.map((t) => t / 1000), ...series.map((s) => s.values)]);

  useEffect(() => {
    const el = host.current;
    if (el === null) return;
    const ink3 = resolve(el, '--ms-ink-3');
    const ink = resolve(el, '--ms-ink');
    const line = resolve(el, '--ms-line');
    const axis = {
      stroke: ink3,
      grid: { stroke: line, width: 1 },
      ticks: { show: false },
      font: '11px Atkinson Hyperlegible Mono',
    };
    const plot = new uPlot(
      {
        width: el.clientWidth || 640,
        height: HEIGHT,
        legend: { show: false },
        cursor: { y: false, points: { show: false } },
        scales: { x: { time: true } },
        axes: [axis, { ...axis, size: 64, values: (_u, splits) => splits.map((v) => latest.current.format(v)) }],
        series: [
          {},
          ...latest.current.series.map((s) => ({
            label: s.group,
            stroke: resolve(el, s.tone),
            dash: s.dash,
            width: 2,
            points: { show: false, filter: lonePoints },
          })),
        ],
        hooks: {
          setCursor: [(u) => hover.current(u.cursor.idx ?? null)],
          draw: [
            (u) => {
              const ctx = u.ctx;
              const ratio = window.devicePixelRatio || 1;
              for (const mark of latest.current.marks) {
                const x = u.valToPos(mark.t / 1000, 'x', true);
                if (x < u.bbox.left || x > u.bbox.left + u.bbox.width) continue;
                ctx.save();
                ctx.strokeStyle = ink3;
                ctx.setLineDash([3 * ratio, 3 * ratio]);
                ctx.beginPath();
                ctx.moveTo(x, u.bbox.top);
                ctx.lineTo(x, u.bbox.top + u.bbox.height);
                ctx.stroke();
                ctx.fillStyle = ink;
                ctx.font = `${11 * ratio}px Atkinson Hyperlegible Mono`;
                ctx.fillText(mark.label, x + 4 * ratio, u.bbox.top + 12 * ratio);
                ctx.restore();
              }
            },
          ],
        },
      },
      data.current,
      el,
    );
    plotRef.current = plot;
    const observer =
      typeof ResizeObserver === 'undefined'
        ? null
        : new ResizeObserver(() => plot.setSize({ width: el.clientWidth, height: HEIGHT }));
    observer?.observe(el);
    return () => {
      observer?.disconnect();
      plot.destroy();
      plotRef.current = null;
    };
  }, [shape, theme, scheme, hover, latest, data]);

  // (On mount this repeats the data the constructor just took: harmless, uPlot's own init does the same.)
  useEffect(() => {
    plotRef.current?.setData([times.map((t) => t / 1000), ...series.map((s) => s.values)]);
  }, [times, series]);

  // Not before the plot is ready: a redraw ahead of uPlot's first commit (on mount, in the same effects flush) sets
  // the x scale from its still-null range, over the auto range the commit is about to apply, and leaves the plot
  // empty until it is rebuilt. A plot built meanwhile already reads the latest marks and format on its first draw.
  useEffect(() => {
    if (plotRef.current?.status === 1) plotRef.current.redraw();
  }, [marks, format]);

  return <div ref={host} role="img" aria-label={label} className="min-h-[320px] w-full" />;
}
