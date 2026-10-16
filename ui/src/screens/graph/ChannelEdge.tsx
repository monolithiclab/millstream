import { getBezierPath, type Edge, type EdgeProps } from '@xyflow/react';
import type { CSSProperties } from 'react';
import { useFormat } from '../../common/useFormat';
import type { ChannelModel } from './model';

interface ChannelData extends Record<string, unknown> {
  channel: ChannelModel;
  // moving drifts the grains; paused (a hidden tab) holds them where they are; still (reduced motion) never animates.
  motion: 'moving' | 'paused' | 'still';
  tone: 'accent' | 'bad' | 'warn';
}

export type ChannelEdgeType = Edge<ChannelData, 'channel'>;

// Grains drift at a constant 24 px/s (interaction notes).
const GRAIN_SPEED = 24;
// At most this many grains pack against the head of a step, then the number says the rest.
const QUEUE_GRAINS = 12;

// A channel's width follows its stream's volume on a log scale: idle 3 px, a thousand items 12 px.
function channelWidth(volume: number): number {
  return 3 + Math.min(9, Math.round(Math.log10(volume + 1) * 3));
}

// Grain spacing follows the pace into the next step: one grain per 14 px at the busiest, 48 px when slow; no pace,
// no grains (an idle channel).
function grainSpacing(pace: number): number | null {
  if (pace <= 0) return null;
  return Math.round(Math.min(48, Math.max(14, 48 - Math.log10(pace + 1) * 17)));
}

const grainClass = {
  moving: 'ms-grains-moving',
  paused: 'ms-grains-moving ms-grains-paused',
  still: undefined,
} as const;

const toneVar = { accent: 'var(--ms-accent)', bad: 'var(--ms-bad)', warn: 'var(--ms-warn)' } as const;

// The signature channel (canvas "channels and pebbles"): a bank and a channel whose width is the stream's volume,
// grains drifting along it as items flow, and the queue packed against the next step's head with its count.
export function ChannelEdge({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps<ChannelEdgeType>) {
  const f = useFormat();
  if (data === undefined) return null;
  const { channel, motion: mode, tone } = data;
  const [path] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition });
  const width = channelWidth(channel.volume);
  const spacing = grainSpacing(channel.pace);
  const grain = Math.max(2, Math.round(width / 2));
  const packed = Math.min(QUEUE_GRAINS, channel.queued);
  const queueLength = packed * 5.5;
  const color = toneVar[tone];
  const motion: CSSProperties | undefined =
    spacing === null
      ? undefined
      : ({
          '--ms-grain-shift': `${-spacing}px`,
          '--ms-grain-duration': `${spacing / GRAIN_SPEED}s`,
        } as CSSProperties);
  return (
    <g data-testid={`channel-${channel.stream}`}>
      <path d={path} fill="none" stroke="var(--ms-channel-bank)" strokeWidth={width + 2} />
      <path d={path} fill="none" stroke="var(--ms-channel)" strokeWidth={width} />
      {spacing !== null && (
        <path
          d={path}
          fill="none"
          stroke={color}
          strokeWidth={grain}
          strokeLinecap="round"
          strokeDasharray={`4 ${spacing - 4}`}
          className={grainClass[mode]}
          data-motion={mode}
          style={motion}
        />
      )}
      {packed > 0 && (
        <>
          <path
            d={`M${targetX - 2 - queueLength} ${targetY}H${targetX - 2}`}
            stroke={color}
            strokeWidth={grain + 1}
            strokeLinecap="round"
            strokeDasharray="4 1.5"
          />
          <text
            x={targetX - 2 - queueLength}
            y={targetY - width / 2 - 6}
            fill={color}
            className="font-mono text-[11px] font-bold"
          >
            {f.num(channel.queued)}
          </text>
        </>
      )}
    </g>
  );
}
