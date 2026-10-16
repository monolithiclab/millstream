import { Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import type { KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { StatusGlyph, useStatusLabel } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import type { Trigger } from '../../api/schema';
import type { StepKind, StepModel } from './model';

export type Overlay = 'flow' | 'errors' | 'latency' | 'queues';

// Step kind icons, from the canvas (code, human, filter); dedupe is a builtin code step.
const kindIcon: Record<StepKind, string> = {
  action: 'M5.5 4L2 8L5.5 12M10.5 4L14 8L10.5 12',
  dedupe: 'M5.5 4L2 8L5.5 12M10.5 4L14 8L10.5 12',
  review: 'M8 7.5a2.75 2.75 0 1 0 0-5.5a2.75 2.75 0 1 0 0 5.5ZM2.5 14.5c.6-3 2.8-4.6 5.5-4.6s4.9 1.6 5.5 4.6',
  filter: 'M1.5 2.5H14.5L9.5 8.5V13.5L6.5 12V8.5Z',
};

export function KindIcon({ kind, size = 14 }: { kind: StepKind; size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" aria-hidden="true" className="flex-none">
      <path
        d={kindIcon[kind]}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}

export function useKindLabel() {
  const { t } = useTranslation();
  const labels: Record<StepKind, string> = {
    action: t('graph.kind.action'),
    dedupe: t('graph.kind.dedupe'),
    review: t('graph.kind.review'),
    filter: t('graph.kind.filter'),
  };
  return (kind: StepKind) => labels[kind];
}

interface StepNodeData extends Record<string, unknown> {
  model: StepModel;
  overlay: Overlay;
  selected: boolean;
  onOpen: (name: string) => void;
  onMove: (name: string, key: string) => boolean;
  register: (name: string, el: HTMLButtonElement | null) => void;
  onFocus: (name: string) => void;
}

export type StepNodeType = Node<StepNodeData, 'step' | 'filter'>;
export type TriggerNodeType = Node<{ trigger: Trigger }, 'trigger'>;

// The main figure of a step under the chosen overlay, and its unit: the overlay changes words and colours, never the
// layout (interaction notes).
function useFigure() {
  const { t } = useTranslation();
  const f = useFormat();
  return (model: StepModel, overlay: Overlay): { value: string; label: string; tone: string } => {
    const fig = model.figures;
    if (fig === null) return { value: '—', label: t('graph.figure.waiting'), tone: 'text-ink-3' };
    switch (overlay) {
      case 'errors':
        return {
          value: f.percent(fig.errorRate),
          label: t('graph.figure.errorRate'),
          tone: fig.errorRate > 0 ? 'text-bad' : 'text-ink',
        };
      case 'latency':
        return { value: f.duration(fig.p95LatencyMs), label: t('graph.figure.p95'), tone: 'text-ink' };
      case 'queues':
        return {
          value: f.num(fig.queued),
          label: t('graph.figure.queued'),
          tone: fig.queued > 0 ? 'text-accent' : 'text-ink',
        };
      default:
        if (model.kind === 'review')
          return { value: f.num(fig.awaitingReview), label: t('graph.figure.toReview'), tone: 'text-human' };
        if (model.kind === 'filter' && model.passRate !== undefined)
          return { value: f.percent(model.passRate), label: t('graph.figure.passRate'), tone: 'text-ink' };
        return { value: f.num(fig.inFlight), label: t('graph.figure.inFlight'), tone: 'text-ink' };
    }
  };
}

// Queue dots: ten, filled on a log scale (14 waiting fills four, 186 eight).
function queueDots(queued: number): number {
  return queued <= 0 ? 0 : Math.min(10, Math.ceil(Math.log2(queued + 1)));
}

function useBadge(model: StepModel) {
  const { t } = useTranslation();
  const f = useFormat();
  if (model.openDeadLetters > 0) {
    const n = f.num(model.openDeadLetters);
    return {
      text: t(f.plural(model.openDeadLetters, 'graph.badge.deadOne', 'graph.badge.deadOther'), { n }),
      bad: true,
    };
  }
  const insight = model.insight;
  if (insight === undefined) return null;
  const words: Record<string, string> = {
    slowStep: t('graph.badge.slow'),
    failureCluster: t('graph.badge.cluster'),
    cost: t('graph.badge.cost'),
    reviewPattern: t('graph.badge.review'),
    unschedulable: t('graph.badge.unschedulable'),
  };
  return { text: words[insight.kind] ?? t('graph.badge.insight'), bad: insight.severity === 'error' };
}

const handleStyle = { opacity: 0, pointerEvents: 'none' } as const;

function Handles() {
  return (
    <>
      <Handle type="target" position={Position.Left} isConnectable={false} style={handleStyle} />
      <Handle type="source" position={Position.Right} isConnectable={false} style={handleStyle} />
    </>
  );
}

// The step's button: Enter (a click) opens the side panel, arrow keys follow channels. Its name is one visually
// hidden sentence (name, kind, state, figure) and its drawing is hidden from screen readers, so the name never
// drifts from what is shown (WCAG 2.5.3) and the small figures stay in the table alternative.
function useStepButton(data: StepNodeData) {
  const { t } = useTranslation();
  const kindLabel = useKindLabel();
  const statusLabel = useStatusLabel();
  const figure = useFigure()(data.model, data.overlay);
  const { model } = data;
  return {
    figure,
    label: t('graph.stepLabel', {
      name: model.name,
      kind: kindLabel(model.kind),
      state: statusLabel(model.state),
      value: figure.value,
      label: figure.label,
    }),
    props: {
      type: 'button' as const,
      ref: (el: HTMLButtonElement | null) => data.register(model.name, el),
      'data-step': model.name,
      'aria-expanded': data.selected,
      'aria-controls': data.selected ? 'step-panel' : undefined,
      onClick: () => data.onOpen(model.name),
      onFocus: () => data.onFocus(model.name),
      onKeyDown: (event: KeyboardEvent) => {
        if (event.key.startsWith('Arrow') && data.onMove(model.name, event.key)) event.preventDefault();
      },
    },
  };
}

const kindTone: Record<StepKind, string> = {
  action: 'text-ink-2',
  dedupe: 'text-ink-2',
  review: 'text-human',
  filter: 'text-ink-2',
};

export function StepNode({ data }: NodeProps<StepNodeType>) {
  const { t } = useTranslation();
  const f = useFormat();
  const kindLabel = useKindLabel();
  const { model, selected } = data;
  const { figure, label, props } = useStepButton(data);
  const fig = model.figures;
  const failing = model.state === 'failing';
  const review = model.kind === 'review';
  const paused = model.state === 'paused';
  const border = selected
    ? 'border-2 border-accent shadow-[0_0_0_5px_var(--ms-accent-soft)]'
    : failing
      ? 'border-2 border-bad shadow-1'
      : review
        ? 'border-[1.5px] border-dashed border-human shadow-1'
        : paused
          ? 'border-[1.5px] border-dashed border-line-strong'
          : 'border border-control-line shadow-1';
  const badge = useBadge(model);
  const dots = queueDots(fig?.queued ?? 0);
  return (
    <div className="relative">
      <Handles />
      <button
        {...props}
        className={`nodrag nopan flex h-[158px] w-[136px] flex-col gap-1.5 rounded-2 px-[9px] pt-[9px] pb-2 text-left text-ink ${border} ${paused ? 'bg-surface-2' : 'bg-surface'}`}
      >
        <span className="sr-only">{label}</span>
        <span aria-hidden="true" className="contents">
          <span className={`flex w-full items-center gap-1.5 text-[11px] leading-4 font-bold ${kindTone[model.kind]}`}>
            <KindIcon kind={model.kind} />
            <span className="min-w-0">{kindLabel(model.kind)}</span>
            <span className="ml-auto">
              <StatusGlyph status={model.state} />
            </span>
          </span>
          <span className="text-small leading-[15px] font-bold break-words [overflow-wrap:anywhere]">{model.name}</span>
          <span className="flex flex-wrap items-baseline gap-x-1.5" data-live>
            <span className={`font-display text-[28px] leading-6 ${figure.tone}`}>{figure.value}</span>
            <span className="text-[11px] leading-4 text-ink-3">{figure.label}</span>
          </span>
          <span className="flex items-center gap-0.5" data-live>
            {Array.from({ length: 10 }, (_, i) => (
              <span key={i} className={`size-1.5 rounded-pill ${i < dots ? 'bg-accent' : 'bg-line'}`} />
            ))}
            <span className="ml-1 font-mono text-[11px] text-ink-2">{fig === null ? '—' : f.num(fig.queued)}</span>
          </span>
          <span
            className="mt-auto grid w-full grid-cols-2 gap-x-1.5 gap-y-px font-mono text-[10.5px] leading-[14px] text-ink-2"
            data-live
          >
            {fig === null ? (
              <span className="col-span-2">{t('graph.figure.noSnapshot')}</span>
            ) : (
              <>
                <span>{t('graph.node.pace', { n: f.num(fig.throughputPerMin, { maximumFractionDigits: 1 }) })}</span>
                <span>{t('graph.node.p95', { d: f.duration(fig.p95LatencyMs) })}</span>
                <span className={fig.errorRate > 0 ? 'text-bad' : ''}>
                  {t('graph.node.errors', { p: f.percent(fig.errorRate) })}
                </span>
                <span>
                  {t(f.plural(model.step.concurrency, 'graph.node.slotsOne', 'graph.node.slotsOther'), {
                    n: f.num(model.step.concurrency),
                  })}
                </span>
              </>
            )}
          </span>
        </span>
      </button>
      {badge !== null && (
        <span
          className={`pointer-events-none absolute -top-[11px] right-2 inline-flex items-center gap-1 rounded-1 border px-1.5 text-[11px] leading-4 font-bold ${badge.bad ? 'border-bad bg-bad-soft text-bad' : 'border-warn bg-warn-soft text-warn'}`}
        >
          <svg width="10" height="10" viewBox="0 0 12 12" aria-hidden="true">
            <path d="M6 1.3L11 10.4H1Z" fill="currentColor" />
          </svg>
          {badge.text}
        </span>
      )}
    </div>
  );
}

// A filter is a funnel (canvas): its name above, the pass rate inside, what it held back below.
export function FilterNode({ data }: NodeProps<StepNodeType>) {
  const { t } = useTranslation();
  const f = useFormat();
  const { model, selected } = data;
  const { figure, label, props } = useStepButton(data);
  return (
    <div className="relative flex h-[158px] w-[112px] flex-col items-center">
      <Handles />
      <span
        aria-hidden="true"
        className="flex max-w-full items-center gap-1 self-start font-mono text-[11.5px] leading-4 font-bold break-all text-ink"
      >
        <KindIcon kind="filter" size={12} />
        {model.name}
      </span>
      <button {...props} className="nodrag nopan relative mt-1 h-[130px] w-[100px] text-ink">
        <span className="sr-only">{label}</span>
        <svg width="100" height="130" viewBox="0 0 100 130" aria-hidden="true" className="absolute inset-0">
          <path
            d="M0.75 0.75H99.25L90 129.25H10Z"
            fill="var(--ms-surface)"
            stroke={
              selected ? 'var(--ms-accent)' : model.state === 'failing' ? 'var(--ms-bad)' : 'var(--ms-channel-bank)'
            }
            strokeWidth={selected || model.state === 'failing' ? 2.5 : 1.5}
          />
        </svg>
        <span aria-hidden="true" className="relative flex flex-col items-center gap-1.5 px-3 pt-4" data-live>
          <span className="text-[18px] leading-5 font-extrabold">{figure.value}</span>
          <span className="h-px w-14 bg-line" />
          <span className="text-center text-[11px] leading-4 text-ink-3">{figure.label}</span>
          {model.filtered !== undefined && (
            <span className="text-center font-mono text-[11px] leading-4 text-ink-3">
              {t('graph.node.filtered', { n: f.num(model.filtered) })}
            </span>
          )}
        </span>
      </button>
    </div>
  );
}

// A trigger: where items enter, not a step (no panel, not in the Tab order).
export function TriggerNode({ data }: NodeProps<TriggerNodeType>) {
  const { t } = useTranslation();
  const { trigger } = data;
  const detail =
    trigger.kind === 'cron'
      ? t('graph.trigger.cron', { schedule: trigger.schedule ?? '' })
      : trigger.kind === 'webhook'
        ? t('graph.trigger.webhook')
        : t('graph.trigger.manual');
  return (
    <div className="flex min-h-[52px] w-[120px] flex-col justify-center gap-0.5 rounded-1 border border-dashed border-line-strong bg-surface-2 px-2.5">
      <Handle type="source" position={Position.Right} isConnectable={false} style={handleStyle} />
      <span className="font-mono text-tiny font-bold break-all text-ink">{trigger.name}</span>
      <span className="text-[11px] leading-4 text-ink-2">{detail}</span>
    </div>
  );
}
