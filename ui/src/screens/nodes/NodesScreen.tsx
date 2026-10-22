import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useApi } from '../../api/context';
import type { DeploymentEngine, Node, Unschedulable } from '../../api/schema';
import { ConfirmDialog, Consequences } from '../../common/ConfirmDialog';
import { NoticeRegion, runAction, useNotice } from '../../common/Notice';
import { ScreenPage } from '../ScreenPage';
import { ScrollRegion } from '../../common/ScrollRegion';
import { EmptyState, QueryView } from '../../common/States';
import { StatusBadge } from '../../common/Status';
import { buttons, card, cardTitle } from '../../common/styles';
import { useFormat, type Format } from '../../common/useFormat';
import { useNow } from '../../common/useNow';
import { useResource } from '../../common/useResource';

// The API has no node stream: the screen reads again this often.
export const NODES_REFRESH_MS = 10_000;

interface Confirm {
  action: 'drain' | 'revoke';
  node: Node;
}

// The engines and their nodes: labels, version, slots used, leases, heartbeat, health and state, with Drain and Revoke
// for a remote node, then the steps whose pending deliveries no node can take. A standby serves it read-only.
export function NodesScreen() {
  const { t } = useTranslation();
  const f = useFormat();
  const api = useApi();
  const now = useNow(NODES_REFRESH_MS);
  const { notice, setNotice, dismiss } = useNotice();
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const close = () => setConfirm(null);
  const nodes = useResource('nodes', (a) => a.listNodes());
  const refresh = nodes.refresh;
  useEffect(() => {
    const timer = setInterval(refresh, NODES_REFRESH_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const data = nodes.status === 'ready' ? nodes.data : null;
  const serving = data?.nodes.filter((n) => n.state !== 'revoked') ?? [];
  const slots = serving.reduce((n, node) => n + node.slots, 0);
  const leases = serving.reduce((n, node) => n + node.leases, 0);
  // Only an active node takes new deliveries: a draining one finishes what it holds.
  const free = serving.filter((n) => n.state === 'active').reduce((n, node) => n + node.slots - node.leases, 0);

  const drain = ({ id }: Node) =>
    runAction(setNotice, {
      call: async () => {
        await api.drainNode(id);
        return t('nodes.drained', { node: id });
      },
      failed: (message) => t('nodes.drainFailed', { node: id, message }),
      settled: refresh,
    });

  const revoke = ({ id }: Node) =>
    runAction(setNotice, {
      call: async () => {
        await api.revokeNode(id);
        return t('nodes.revoked', { node: id });
      },
      failed: (message) => t('nodes.revokeFailed', { node: id, message }),
      settled: refresh,
    });

  return (
    <ScreenPage
      title={t('nav.nodes')}
      summary={
        data ? (
          <span data-live>
            <strong className="text-ink">{t('nodes.busy', { leases: f.num(leases), slots: f.num(slots) })}</strong>
            {' · '}
            {t('nodes.connected', { count: data.nodes.length })}
          </span>
        ) : undefined
      }
    >
      <QueryView query={nodes} rows={2}>
        {({ engine, engines, nodes: list, unschedulable }) => {
          const standby = engine.state === 'standby';
          return (
            <div className="flex flex-col gap-5">
              {standby && (
                <p
                  role="status"
                  className="m-0 rounded-2 border border-line bg-surface-2 px-3.5 py-2.5 text-small text-ink"
                >
                  {t('nodes.readOnly')}
                </p>
              )}
              <div className="grid grid-cols-[repeat(auto-fit,minmax(min(300px,100%),1fr))] gap-4">
                {(engines.length > 0 ? engines : [engine]).map((e) => (
                  <EngineCard key={e.id} engine={e} serving={e.id === engine.id} f={f} />
                ))}
                <section aria-label={t('nodes.capacity')} className={`${card} flex flex-col gap-2 p-4`}>
                  <span className="font-display text-[18px] text-ink-3 italic">{t('nodes.capacity')}</span>
                  <p className="m-0 font-display text-[44px] leading-[44px]">{f.num(free)}</p>
                  <p className="m-0 text-small text-ink-2">{t('nodes.freeSlots')}</p>
                </section>
              </div>
              {list.length === 0 ? (
                <EmptyState title={t('nodes.none')}>{t('nodes.noneBody')}</EmptyState>
              ) : (
                <ScrollRegion label={t('nodes.table')} className={`${card} overflow-x-auto`}>
                  <table className="w-full min-w-[1040px] border-collapse text-small">
                    <caption className="sr-only">{t('nodes.table')}</caption>
                    <thead>
                      <tr className="bg-surface-2 text-left text-tiny text-ink-3">
                        <th scope="col" className="py-2.5 pr-2 pl-4 font-bold">
                          {t('nodes.col.node')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.state')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.health')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.heartbeat')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.labels')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.version')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 font-bold">
                          {t('nodes.col.slots')}
                        </th>
                        <th scope="col" className="px-2 py-2.5 text-right font-bold">
                          {t('nodes.col.leases')}
                        </th>
                        <th scope="col" className={`py-2.5 pl-2 font-bold ${standby ? 'pr-4' : 'pr-2'}`}>
                          {t('nodes.col.runtimes')}
                        </th>
                        {!standby && (
                          <th scope="col" className="py-2.5 pr-4 pl-2 text-right font-bold">
                            {t('nodes.col.actions')}
                          </th>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {list.map((node) => (
                        <NodeRow
                          key={node.id}
                          node={node}
                          now={now}
                          f={f}
                          actions={!standby}
                          onConfirm={(action) => setConfirm({ action, node })}
                        />
                      ))}
                    </tbody>
                  </table>
                </ScrollRegion>
              )}
              {unschedulable !== undefined ? (
                <UnschedulableSection list={unschedulable} f={f} />
              ) : (
                standby && (
                  <section aria-labelledby="nodes-unschedulable" className="flex flex-col gap-2">
                    <h2 id="nodes-unschedulable" className={cardTitle}>
                      {t('nodes.unschedulable')}
                    </h2>
                    <p className="m-0 text-small text-ink-2">{t('nodes.unschedulableStandby')}</p>
                  </section>
                )
              )}
            </div>
          );
        }}
      </QueryView>
      {confirm && (
        <NodeDialog
          confirm={confirm}
          f={f}
          onConfirm={() => (confirm.action === 'drain' ? drain(confirm.node) : revoke(confirm.node)).finally(close)}
          onCancel={close}
        />
      )}
      <NoticeRegion notice={notice} onDismiss={dismiss} />
    </ScreenPage>
  );
}

function EngineCard({ engine, serving, f }: { engine: DeploymentEngine; serving: boolean; f: Format }) {
  const { t } = useTranslation();
  const active = engine.state === 'active';
  const since = engine.since ? f.dateTime(engine.since) : undefined;
  const { version } = engine;
  let line = t('nodes.version', { version });
  if (since)
    line = active ? t('nodes.leaderSince', { version, since }) : t('nodes.standingBySince', { version, since });
  if (engine.epoch !== undefined) line = `${line} · ${t('nodes.epoch', { epoch: engine.epoch })}`;
  return (
    <section
      aria-label={active ? t('nodes.activeEngine') : t('nodes.standbyEngine')}
      className={`${card} flex flex-col gap-2 p-4`}
    >
      <div className="flex items-center gap-2">
        <span className="font-display text-[18px] text-ink-3 italic">{t('nodes.engine')}</span>
        {active ? <StatusBadge status="active" /> : <StatusBadge status="pending" label={t('nodes.standby')} />}
        {serving && <span className="ml-auto text-tiny text-ink-3">{t('nodes.serving')}</span>}
      </div>
      <p className="m-0 font-display text-[34px] leading-9">{engine.id}</p>
      <p className="m-0 text-small text-ink-2">{line}</p>
      <p className="m-0 text-small text-ink-2">{active ? t('nodes.activeNote') : t('nodes.standbyNote')}</p>
    </section>
  );
}

function NodeRow({
  node,
  now,
  f,
  actions,
  onConfirm,
}: {
  node: Node;
  now: number;
  f: Format;
  actions: boolean;
  onConfirm: (action: Confirm['action']) => void;
}) {
  const { t } = useTranslation();
  const labels = Object.entries(node.labels);
  const full = node.leases >= node.slots && node.slots > 0;
  const used = f.num(node.leases);
  const total = f.num(node.slots);
  return (
    <tr className="border-t border-line">
      <td className="py-3 pr-2 pl-4">
        <div className="font-mono font-bold">{node.id}</div>
        <div className="mt-[3px] text-tiny text-ink-3">{node.embedded ? t('nodes.embedded') : t('nodes.remote')}</div>
      </td>
      <td className="px-2 py-3">
        <StatusBadge status={node.state} />
      </td>
      <td className="px-2 py-3">
        <StatusBadge status={node.health} />
      </td>
      <td className="px-2 py-3 text-ink-2">
        {node.lastHeartbeat === undefined ? (
          <span title={t('nodes.noHeartbeat')}>
            <span aria-hidden="true">—</span>
            <span className="sr-only">{t('nodes.noHeartbeat')}</span>
          </span>
        ) : (
          <time dateTime={node.lastHeartbeat} title={f.dateTime(node.lastHeartbeat)}>
            {f.relative(node.lastHeartbeat, new Date(now))}
          </time>
        )}
      </td>
      <td className="px-2 py-3">
        {labels.length === 0 ? (
          <span className="text-ink-3">{t('nodes.noLabels')}</span>
        ) : (
          <span className="flex flex-wrap gap-1">
            {labels.map(([k, v]) => (
              <span key={k} className="rounded-1 border border-line px-1.5 font-mono text-[11.5px] text-ink-2">
                {k}={v}
              </span>
            ))}
          </span>
        )}
      </td>
      <td className={`px-2 py-3 font-mono ${node.behind ? 'text-warn' : ''}`}>
        {node.behind ? t('nodes.behind', { version: node.version }) : node.version}
      </td>
      <td className="px-2 py-3">
        <span className="flex items-center gap-2">
          <span
            role="meter"
            aria-label={t('nodes.slotsOf', { node: node.id })}
            aria-valuemin={0}
            aria-valuemax={node.slots}
            aria-valuenow={node.leases}
            aria-valuetext={t('nodes.busy', { leases: used, slots: total })}
            className="flex max-w-[180px] flex-wrap gap-0.5"
          >
            {Array.from({ length: Math.min(node.slots, 32) }, (_, i) => (
              <span
                key={i}
                className={`h-3.5 w-[9px] rounded-[4px] border ${i < node.leases ? 'border-accent bg-accent' : 'border-control-line'}`}
              />
            ))}
          </span>
          <span aria-hidden="true" className="font-mono text-tiny text-ink-2">
            {t(full ? 'nodes.slotsFull' : 'nodes.slots', { used, total })}
          </span>
        </span>
      </td>
      <td className="px-2 py-3 text-right font-mono">{used}</td>
      <td className={`py-3 pl-2 font-mono text-tiny text-ink-2 ${actions ? 'pr-2' : 'pr-4'}`}>
        {node.runtimes.join(', ')}
      </td>
      {actions && (
        <td className="py-3 pr-4 pl-2">
          {!node.embedded && node.state !== 'revoked' && (
            <div className="flex flex-wrap justify-end gap-1.5">
              {node.state === 'active' && (
                <button
                  type="button"
                  className={buttons.smallSecondary}
                  aria-label={t('nodes.drainName', { node: node.id })}
                  onClick={() => onConfirm('drain')}
                >
                  {t('nodes.drain')}
                </button>
              )}
              <button
                type="button"
                className={buttons.smallSecondary}
                aria-label={t('nodes.revokeName', { node: node.id })}
                onClick={() => onConfirm('revoke')}
              >
                {t('nodes.revoke')}
              </button>
            </div>
          )}
        </td>
      )}
    </tr>
  );
}

// Drain or Revoke a remote node, saying what becomes of the deliveries it holds.
function NodeDialog({
  confirm: { action, node },
  f,
  onConfirm,
  onCancel,
}: {
  confirm: Confirm;
  f: Format;
  onConfirm: () => Promise<unknown>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const vars = { node: node.id, n: f.num(node.leases) };
  const held = node.leases;
  if (action === 'drain') {
    return (
      <ConfirmDialog
        title={t('nodes.drainTitle', vars)}
        confirmLabel={t('nodes.drainName', vars)}
        onConfirm={onConfirm}
        onCancel={onCancel}
      >
        <Consequences>
          <li>{t('nodes.drainStops', vars)}</li>
          <li>
            {held === 0
              ? t('nodes.drainIdle')
              : t(f.plural(held, 'nodes.drainFinishOne', 'nodes.drainFinishOther'), vars)}
          </li>
          <li>{t('nodes.drainRestart')}</li>
        </Consequences>
      </ConfirmDialog>
    );
  }
  return (
    <ConfirmDialog
      title={t('nodes.revokeTitle', vars)}
      confirmLabel={t('nodes.revokeName', vars)}
      destructive
      onConfirm={onConfirm}
      onCancel={onCancel}
    >
      <Consequences>
        <li>{t('nodes.revokeRefused', vars)}</li>
        {held > 0 && <li>{t(f.plural(held, 'nodes.revokeLeasesOne', 'nodes.revokeLeasesOther'), vars)}</li>}
        <li>{t('nodes.revokeUndo')}</li>
      </Consequences>
    </ConfirmDialog>
  );
}

function UnschedulableSection({ list, f }: { list: Unschedulable[]; f: Format }) {
  const { t } = useTranslation();
  const pending = list.reduce((n, u) => n + u.pending, 0);
  return (
    <section aria-labelledby="nodes-unschedulable" className="flex flex-col gap-3">
      <h2 id="nodes-unschedulable" className={`${cardTitle} flex flex-wrap items-baseline gap-x-3`}>
        {t('nodes.unschedulable')}
        {list.length > 0 && (
          <span className="font-sans text-small font-bold text-bad">
            {t(f.plural(pending, 'nodes.pendingOne', 'nodes.pendingOther'), { n: f.num(pending) })}
          </span>
        )}
      </h2>
      {list.length === 0 ? (
        <EmptyState title={t('nodes.unschedulableNone')} done />
      ) : (
        <ScrollRegion label={t('nodes.unschedulableTable')} className={`${card} overflow-x-auto`}>
          <table className="w-full min-w-[560px] border-collapse text-small">
            <caption className="sr-only">{t('nodes.unschedulableTable')}</caption>
            <thead>
              <tr className="bg-surface-2 text-left text-tiny text-ink-3">
                <th scope="col" className="py-2.5 pr-2 pl-4 font-bold">
                  {t('nodes.col.pipeline')}
                </th>
                <th scope="col" className="px-2 py-2.5 font-bold">
                  {t('nodes.col.step')}
                </th>
                <th scope="col" className="px-2 py-2.5 text-right font-bold">
                  {t('nodes.col.pending')}
                </th>
                <th scope="col" className="py-2.5 pr-4 pl-2 font-bold">
                  {t('nodes.col.needs')}
                </th>
              </tr>
            </thead>
            <tbody>
              {list.map((u) => (
                <tr key={`${u.pipeline}/${u.step}`} className="border-t border-line">
                  <td className="py-3 pr-2 pl-4 font-mono">{u.pipeline}</td>
                  <td className="px-2 py-3 font-mono font-bold">{u.step}</td>
                  <td className="px-2 py-3 text-right font-mono">{f.num(u.pending)}</td>
                  <td className="py-3 pr-4 pl-2 font-mono text-tiny text-ink-2">{u.needs}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </ScrollRegion>
      )}
    </section>
  );
}
