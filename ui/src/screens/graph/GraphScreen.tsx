import {
  MiniMap,
  Panel,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
  type NodeTypes,
  type EdgeTypes,
} from '@xyflow/react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { liveUrls } from '../../api/api';
import type { Pipeline, PipelineSnapshot, Run } from '../../api/schema';
import { useApi } from '../../api/context';
import { useResource } from '../../common/useResource';
import { card } from '../../common/styles';
import { useDocumentVisible, useReducedMotion } from '../../common/motion';
import { NoticeRegion, runAction, useNotice } from '../../common/Notice';
import { ScrollRegion } from '../../common/ScrollRegion';
import { Segmented } from '../../common/Segmented';
import { EmptyState, ErrorState, Loading } from '../../common/States';
import { StatusBadge, isStatus, useStatusLabel } from '../../common/Status';
import { useFormat } from '../../common/useFormat';
import { useCoalescedRefresh } from '../../live/useCoalescedRefresh';
import { useThrottle } from '../../common/useThrottle';
import { parseData, useLiveStream } from '../../live/useLiveStream';
import { pipelinePath, runPath } from '../../routes';
import { ScreenPage } from '../ScreenPage';
import { useDeadLetterActions } from '../deadLetters/useDeadLetterActions';
import { ChannelEdge, type ChannelEdgeType } from './ChannelEdge';
import { buildModel, layout, sizeOf, stepId, type GraphModel, type Layout, type StepModel } from './model';
import {
  FilterNode,
  KindIcon,
  StepNode,
  TriggerNode,
  useKindLabel,
  type Overlay,
  type StepNodeType,
  type TriggerNodeType,
} from './nodes';
import { StepPanel } from './StepPanel';
import './graph.css';

const nodeTypes: NodeTypes = { step: StepNode, filter: FilterNode, trigger: TriggerNode };
const edgeTypes: EdgeTypes = { channel: ChannelEdge };

// Live snapshots arrive about four times a second; the graph paints at most once per PAINT_MS (interaction notes).
export const PAINT_MS = 250;
// The minimap appears above this many steps.
const MINIMAP_STEPS = 12;

const fitViewOptions = { padding: 0.12 };
const proOptions = { hideAttribution: true };

function orderedSteps(model: GraphModel, order: string[]): StepModel[] {
  const byName = new Map(model.steps.map((s) => [s.name, s]));
  return order.flatMap((name) => byName.get(name) ?? []);
}

// The latest snapshot of the pipeline's live stream, painted at most every PAINT_MS: the first at once, then the
// newest of each window. A reconnection reloads the REST data around it.
function useSnapshot(pipeline: string, onReconnect: () => void): PipelineSnapshot | null {
  const [state, setState] = useState<{ pipeline: string; snapshot: PipelineSnapshot } | null>(null);
  // The newest snapshot not painted yet, with its pipeline.
  const pending = useRef<{ pipeline: string; snapshot: PipelineSnapshot } | null>(null);
  // Paint the newest snapshot now, then once per window while more arrive.
  const paint = useThrottle(
    () => {
      if (pending.current !== null) setState(pending.current);
      pending.current = null;
    },
    PAINT_MS,
    { leading: true },
  );

  useLiveStream(liveUrls.pipeline(pipeline), {
    events: ['snapshot'],
    onReconnect,
    onMessage: (message) => {
      const snapshot = message.type === 'snapshot' ? parseData<PipelineSnapshot>(message) : null;
      if (snapshot === null) return;
      pending.current = { pipeline, snapshot };
      paint();
    },
  });
  return state?.pipeline === pipeline ? state.snapshot : null;
}

// Snapshots carry the live figures; the rest of the graph comes from REST reads (the current run, dead letters,
// insight badges). A snapshot that reflects events the last one did not, such as a run fired elsewhere, refetches
// them, coalesced.
function useRefreshOnEvents(pipeline: string, snapshot: PipelineSnapshot | null, refresh: () => void) {
  const schedule = useCoalescedRefresh(refresh);
  const seen = useRef<{ pipeline: string; seq: number } | null>(null);
  useEffect(() => {
    if (snapshot === null) return;
    const last = seen.current?.pipeline === pipeline ? seen.current.seq : null;
    seen.current = { pipeline, seq: snapshot.seq };
    // The first snapshot follows the REST reads just made.
    if (last !== null && last !== snapshot.seq) schedule();
  }, [pipeline, snapshot, schedule]);
}

function GraphCanvas({
  model,
  positions,
  overlay,
  selected,
  motion,
  onOpen,
}: {
  model: GraphModel;
  positions: Layout;
  overlay: Overlay;
  selected: string | null;
  motion: 'moving' | 'paused' | 'still';
  onOpen: (name: string) => void;
}) {
  const { t } = useTranslation();
  const f = useFormat();
  const flow = useReactFlow();
  const container = useRef<HTMLDivElement>(null);
  const stepButtons = useRef(new Map<string, HTMLButtonElement>());

  const register = useCallback((name: string, el: HTMLButtonElement | null) => {
    if (el === null) stepButtons.current.delete(name);
    else stepButtons.current.set(name, el);
  }, []);

  // Arrow keys follow channels (Right, Left) and move within a column (Up, Down).
  const onMove = useCallback(
    (name: string, key: string) => {
      const to = positions.moves.get(name)?.[key as 'ArrowRight'];
      if (to === undefined) return false;
      stepButtons.current.get(to)?.focus();
      return true;
    },
    [positions],
  );

  // A step focused outside the view (Tab, arrows) brings the view to it.
  const onFocus = useCallback(
    (name: string) => {
      const box = container.current;
      const p = positions.positions.get(stepId(name));
      const step = model.steps.find((s) => s.name === name);
      if (box === null || p === undefined || step === undefined) return;
      const { width, height } = sizeOf(step.kind);
      const { x, y, zoom } = flow.getViewport();
      const left = p.x * zoom + x;
      const top = p.y * zoom + y;
      if (left < 0 || top < 0 || left + width * zoom > box.clientWidth || top + height * zoom > box.clientHeight) {
        void flow.setCenter(p.x + width / 2, p.y + height / 2, { zoom, duration: 0 });
      }
    },
    [flow, positions, model],
  );

  const byId = useMemo(() => new Map(model.steps.map((s) => [s.id, s])), [model]);
  const names = useMemo(
    () =>
      new Map<string, string>([
        ...model.steps.map((s) => [s.id, s.name] as const),
        ...model.triggers.map((tr) => [tr.id, tr.name] as const),
      ]),
    [model],
  );

  // A click anywhere on a step opens it; with a node handler, React Flow also lets clicks reach the nodes.
  const onNodeClick = useCallback(
    (_event: unknown, node: Node) => {
      const name = names.get(node.id);
      if (node.type !== 'trigger' && name !== undefined) onOpen(name);
    },
    [names, onOpen],
  );

  const nodes = useMemo<Node[]>(() => {
    const steps = orderedSteps(model, positions.order).map((s): StepNodeType => {
      const position = positions.positions.get(s.id) ?? { x: 0, y: 0 };
      return {
        id: s.id,
        type: s.kind === 'filter' ? 'filter' : 'step',
        position,
        ...sizeOf(s.kind),
        data: { model: s, overlay, selected: s.name === selected, onOpen, onMove, register, onFocus },
        ariaRole: 'group',
        domAttributes: { 'aria-roledescription': t('graph.stepRole') },
      };
    });
    const triggers = model.triggers.map((tr): TriggerNodeType => ({
      id: tr.id,
      type: 'trigger',
      position: positions.positions.get(tr.id) ?? { x: 0, y: 0 },
      ...sizeOf('trigger'),
      data: 'imported' in tr ? { imported: tr.imported } : { trigger: tr.trigger },
      ariaRole: 'group',
      ariaLabel:
        'imported' in tr
          ? t('graph.importLabel', { name: tr.name, pipeline: tr.imported.pipeline })
          : t('graph.triggerLabel', { name: tr.name }),
      domAttributes: { 'aria-roledescription': 'imported' in tr ? t('graph.importRole') : t('graph.triggerRole') },
    }));
    return [...steps, ...triggers];
  }, [model, positions, overlay, selected, onOpen, onMove, register, onFocus, t]);

  const edges = useMemo<Edge[]>(
    () =>
      model.channels.map((channel): ChannelEdgeType => {
        const target = byId.get(channel.target);
        const tone =
          overlay === 'errors' && (target?.figures?.errorRate ?? 0) > 0
            ? 'bad'
            : overlay === 'latency' && target?.state === 'slow'
              ? 'warn'
              : 'accent';
        return {
          id: channel.id,
          source: channel.source,
          target: channel.target,
          type: 'channel',
          data: { channel, motion, tone },
          ariaRole: 'img',
          ariaLabel: t('graph.channelLabel', {
            stream: channel.stream,
            from: names.get(channel.source) ?? channel.source,
            to: names.get(channel.target) ?? channel.target,
            n: f.num(channel.volume),
            queued: f.num(channel.queued),
          }),
          domAttributes: { 'aria-roledescription': t('graph.channelRole') },
        };
      }),
    [model, byId, names, overlay, motion, t, f],
  );

  return (
    <div
      ref={container}
      className={`${card} relative h-[clamp(380px,62vh,680px)] overflow-hidden`}
      data-testid="graph-canvas"
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        aria-label={t('graph.canvas')}
        fitView
        fitViewOptions={fitViewOptions}
        minZoom={0.3}
        maxZoom={1.6}
        nodesDraggable={false}
        nodesConnectable={false}
        nodesFocusable={false}
        edgesFocusable={false}
        elementsSelectable={false}
        disableKeyboardA11y
        zoomOnScroll={false}
        preventScrolling={false}
        zoomOnDoubleClick={false}
        proOptions={proOptions}
        onNodeClick={onNodeClick}
      >
        <Panel position="bottom-left">
          <div className="flex overflow-hidden rounded-1 border border-control-line bg-surface">
            <button
              type="button"
              aria-label={t('graph.zoomOut')}
              onClick={() => void flow.zoomOut()}
              className="size-8 border-r border-line text-base text-ink"
            >
              −
            </button>
            <button
              type="button"
              onClick={() => void flow.fitView(fitViewOptions)}
              className="h-8 border-r border-line px-2.5 text-tiny font-bold text-ink"
            >
              {t('graph.fit')}
            </button>
            <button
              type="button"
              aria-label={t('graph.zoomIn')}
              onClick={() => void flow.zoomIn()}
              className="size-8 text-base text-ink"
            >
              +
            </button>
          </div>
        </Panel>
        {model.steps.length > MINIMAP_STEPS && <MiniMap pannable zoomable ariaLabel={t('graph.minimap')} />}
      </ReactFlow>
    </div>
  );
}

// The steps with their figures, for screen readers and anyone who prefers a table (the graph's text alternative).
function StepTable({ model, order }: { model: GraphModel; order: string[] }) {
  const { t } = useTranslation();
  const f = useFormat();
  const kindLabel = useKindLabel();
  const statusLabel = useStatusLabel();
  const steps = orderedSteps(model, order);
  const cell = 'px-2 py-1.5 text-right font-mono';
  return (
    <details className={`${card} px-4 py-3`}>
      <summary className="cursor-pointer text-small font-bold">{t('graph.table.summary')}</summary>
      <ScrollRegion className="mt-3 overflow-x-auto" label={t('graph.table.caption')}>
        <table className="w-full border-collapse text-small">
          <caption className="sr-only">{t('graph.table.caption')}</caption>
          <thead>
            <tr className="text-left text-tiny text-ink-3">
              <th scope="col" className="px-2 py-1.5">
                {t('graph.table.step')}
              </th>
              <th scope="col" className="px-2 py-1.5">
                {t('graph.table.state')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.figure.inFlight')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.figure.queued')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.figure.toReview')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.table.pace')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.figure.errorRate')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.figure.p95')}
              </th>
              <th scope="col" className="px-2 py-1.5 text-right">
                {t('graph.table.deadLetters')}
              </th>
            </tr>
          </thead>
          <tbody>
            {steps.map((s) => {
              const fig = s.figures;
              const v = (value: string) => (fig === null ? '—' : value);
              return (
                <tr key={s.name} className="border-t border-line">
                  <th scope="row" className="px-2 py-1.5 text-left font-normal">
                    <span className="font-mono font-bold">{s.name}</span>{' '}
                    <span className="text-ink-2">{kindLabel(s.kind)}</span>
                  </th>
                  <td className="px-2 py-1.5">{statusLabel(s.state)}</td>
                  <td className={cell}>{v(f.num(fig?.inFlight ?? 0))}</td>
                  <td className={cell}>{v(f.num(fig?.queued ?? 0))}</td>
                  <td className={cell}>{v(f.num(fig?.awaitingReview ?? 0))}</td>
                  <td className={cell}>
                    {v(
                      t('graph.panel.perMinute', {
                        n: f.num(fig?.throughputPerMin ?? 0, { maximumFractionDigits: 1 }),
                      }),
                    )}
                  </td>
                  <td className={cell}>{v(f.percent(fig?.errorRate ?? 0))}</td>
                  <td className={cell}>{v(f.duration(fig?.p95LatencyMs ?? 0))}</td>
                  <td className={cell}>{f.num(s.openDeadLetters)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </ScrollRegion>
    </details>
  );
}

// The current or last run so far (canvas footer): items per stream, each a door to them, and its dead letters.
function RunStrip({ pipeline, run }: { pipeline: Pipeline; run: Run | null }) {
  const { t } = useTranslation();
  const f = useFormat();
  const last = pipeline.lastRun;
  if (last === undefined || run === null) return null;
  const streams = pipeline.streams.filter((s) => !s.endsWith('.dlq') && (run.items?.[s] ?? 0) > 0);
  return (
    <section
      aria-label={t('graph.run.label', { time: f.clock(last.startedAt) })}
      className={`${card} flex flex-wrap items-center gap-x-[22px] gap-y-2 px-5 py-3.5 text-small`}
    >
      <Link to={runPath(pipeline.name, last.id)} className="font-display text-[20px] text-ink-2 italic">
        {t('graph.run.title', { time: f.clock(last.startedAt) })}
      </Link>
      {isStatus(run.state) && <StatusBadge status={run.state} />}
      {streams.map((s) => (
        <Link
          key={s}
          to={`${pipelinePath(pipeline.name, 'items')}?stream=${encodeURIComponent(s)}&run=${encodeURIComponent(last.id)}`}
          className="inline-flex items-baseline gap-1.5 text-ink-2 no-underline"
          data-live
        >
          <strong className="font-display text-[24px] font-normal text-ink underline decoration-line-strong decoration-1 underline-offset-4">
            {f.num(run.items?.[s] ?? 0)}
          </strong>{' '}
          <span className="font-mono">{s}</span>
        </Link>
      ))}
      <Link
        to={pipelinePath(pipeline.name, 'dead-letters')}
        className="inline-flex items-baseline gap-1.5 text-ink-2 no-underline"
      >
        <strong
          className={`font-display text-[24px] font-normal underline decoration-line-strong decoration-1 underline-offset-4 ${(run.openDeadLetters ?? 0) > 0 ? 'text-bad' : 'text-ink'}`}
        >
          {f.num(run.openDeadLetters ?? 0)}
        </strong>{' '}
        {t('graph.run.deadLetters')}
      </Link>
      <span className="inline-flex items-baseline gap-1.5 text-ink-2">
        <strong className="font-display text-[24px] font-normal text-ink">{f.money(run.costUsd)}</strong>{' '}
        {t('graph.run.cost')}
      </span>
    </section>
  );
}

function Legend() {
  const kindLabel = useKindLabel();
  return (
    <ul className="m-0 flex list-none flex-wrap items-center gap-x-4 gap-y-1 p-0 text-tiny text-ink-2">
      {(['action', 'filter', 'review', 'dedupe'] as const).map((kind) => (
        <li key={kind} className={`inline-flex items-center gap-1.5 ${kind === 'review' ? 'text-human' : ''}`}>
          <KindIcon kind={kind} />
          {kindLabel(kind)}
        </li>
      ))}
    </ul>
  );
}

// The pipeline graph (canvas "Pipeline graph"): steps laid out left to right, channels whose width is their
// stream's volume and whose grains are items flowing, live figures from the pipeline's snapshots, failing steps
// outlined, insight badges, and the side panel of the step named by ?step=.
export function GraphScreen() {
  const { pipeline = '' } = useParams();
  const { t } = useTranslation();
  const api = useApi();
  const [params, setParams] = useSearchParams();
  const selected = params.get('step');
  const reduced = useReducedMotion();
  const visible = useDocumentVisible();
  const [overlay, setOverlay] = useState<Overlay>('flow');
  const [busy, setBusy] = useState(false);
  const { notice, setNotice, dismiss } = useNotice();

  const data = useResource(`graph:${pipeline}`, async (a) => {
    // The run so far is read with the pipeline that names it; the strip is left out when it cannot be read.
    const withRun = async (p: Pipeline) =>
      [p, p.lastRun ? await a.getRun(pipeline, p.lastRun.id).catch(() => null) : null] as const;
    const [[p, run], streams, deadLetters, insights] = await Promise.all([
      a.getPipeline(pipeline).then(withRun),
      a.listStreams(pipeline),
      a.listDeadLetters(pipeline),
      a.listInsights({ pipeline, state: 'open' }),
    ]);
    return { pipeline: p, run, streams, deadLetters, insights };
  });
  const { reload, refresh } = data;
  const snapshot = useSnapshot(pipeline, refresh);
  useRefreshOnEvents(pipeline, snapshot, refresh);
  const ready = data.status === 'ready' ? data.data : null;

  const model = useMemo(() => (ready === null ? null : buildModel({ ...ready, snapshot })), [ready, snapshot]);
  // Layout follows the REST reads only: live figures never move a node.
  const positions = useMemo(() => (ready === null ? null : layout(buildModel({ ...ready, snapshot: null }))), [ready]);

  const actions = useDeadLetterActions({ pipeline, revision: ready?.pipeline.revision, onDone: refresh, setNotice });

  const open = useCallback(
    (name: string) =>
      setParams(
        (current) => {
          const next = new URLSearchParams(current);
          next.set('step', name);
          return next;
        },
        { replace: true },
      ),
    [setParams],
  );
  const close = () => {
    const name = selected;
    setParams(
      (current) => {
        const next = new URLSearchParams(current);
        next.delete('step');
        return next;
      },
      { replace: true },
    );
    // Focus returns to the step whose panel closed.
    if (name !== null)
      [...document.querySelectorAll<HTMLElement>('[data-step]')].find((el) => el.dataset.step === name)?.focus();
  };

  const setPaused = async ({ name: step }: StepModel, paused: boolean) => {
    setBusy(true);
    await runAction(setNotice, {
      call: async () => {
        await (paused ? api.pause({ pipeline, step }) : api.resume({ pipeline, step }));
        return paused ? t('graph.paused', { step }) : t('graph.resumed', { step });
      },
      failed: (message) =>
        paused ? t('graph.pauseFailed', { step, message }) : t('graph.resumeFailed', { step, message }),
      settled: refresh,
    });
    setBusy(false);
  };

  const motion = reduced ? 'still' : visible ? 'moving' : 'paused';
  const selectedModel = model?.steps.find((s) => s.name === selected);

  return (
    <ScreenPage
      title={t('nav.graph')}
      eyebrow={<p className="m-0 font-mono text-small text-ink-2">{t('screen.inPipeline', { pipeline })}</p>}
    >
      <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
        <Segmented
          label={t('graph.overlay')}
          value={overlay}
          onChange={setOverlay}
          options={[
            { value: 'flow', label: t('graph.overlays.flow') },
            { value: 'errors', label: t('graph.overlays.errors') },
            { value: 'latency', label: t('graph.overlays.latency') },
            { value: 'queues', label: t('graph.overlays.queues') },
          ]}
        />
        <Legend />
        <p className="m-0 text-tiny text-ink-3 md:ml-auto">{t('graph.hint')}</p>
      </div>

      {data.status === 'loading' && <Loading rows={1} className="h-[420px]" />}
      {data.status === 'error' && <ErrorState title={t('graph.error')} message={data.error.message} onRetry={reload} />}
      {model !== null && positions !== null && ready !== null && (
        <>
          {model.steps.length === 0 ? (
            <EmptyState title={t('graph.empty.title')}>
              <p className="m-0">{t('graph.empty.body')}</p>
            </EmptyState>
          ) : (
            <div className="flex flex-col gap-4 md:flex-row md:items-start">
              <div className="flex min-w-0 flex-1 flex-col gap-4">
                <ReactFlowProvider>
                  <GraphCanvas
                    model={model}
                    positions={positions}
                    overlay={overlay}
                    selected={selected}
                    motion={motion}
                    onOpen={open}
                  />
                </ReactFlowProvider>
                {snapshot === null && (
                  <p className="m-0 text-small text-ink-2" role="status">
                    {t('graph.figure.noSnapshot')}
                  </p>
                )}
                <RunStrip pipeline={ready.pipeline} run={ready.run} />
                <StepTable model={model} order={positions.order} />
              </div>
              {selectedModel !== undefined && (
                <StepPanel
                  pipeline={pipeline}
                  revision={ready.pipeline.revision}
                  model={selectedModel}
                  deadLetters={ready.deadLetters.filter((g) => g.step === selectedModel.name)}
                  busy={busy}
                  onClose={close}
                  onPause={(paused) => void setPaused(selectedModel, paused)}
                  onDeadLetters={actions.request}
                />
              )}
            </div>
          )}
        </>
      )}
      {actions.dialog}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}
