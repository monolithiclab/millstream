import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import type { Node } from '../../api/schema';
import { formatNumber } from '../../i18n/format';
import { usePreferences } from '../../preferences';
import { ScreenPage } from '../ScreenPage';
import { EmptyState, QueryView } from '../parts/States';
import { StatusBadge } from '../parts/Status';
import { card } from '../parts/styles';
import { useResource } from '../parts/useResource';

// The API has no node stream: the screen reads again this often.
export const NODES_REFRESH_MS = 10_000;

// The engine and its nodes: labels, version, slots used and free, leases held, runtimes. M3 runs one node, embedded
// in the engine; remote nodes, heartbeats, Drain and Revoke arrive with M4.
export function NodesScreen() {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const nodes = useResource('nodes', (api) => api.listNodes());
  const refresh = nodes.refresh;
  useEffect(() => {
    const timer = setInterval(refresh, NODES_REFRESH_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const data = nodes.state.status === 'ready' ? nodes.state.data : null;
  const slots = data?.nodes.reduce((n, node) => n + node.slots, 0) ?? 0;
  const leases = data?.nodes.reduce((n, node) => n + node.leases, 0) ?? 0;

  return (
    <ScreenPage
      title={t('nav.nodes')}
      header={
        data && (
          <p className="m-0 text-small text-ink-2" data-live>
            <strong className="text-ink">
              {t('nodes.busy', { leases: formatNumber(leases, language), slots: formatNumber(slots, language) })}
            </strong>
            {' · '}
            {t('nodes.connected', { count: data.nodes.length })}
          </p>
        )
      }
    >
      <QueryView state={nodes.state} onRetry={nodes.reload} lines={2}>
        {({ engine, nodes: list }) => (
          <div className="flex flex-col gap-5">
            <div className="grid grid-cols-[repeat(auto-fit,minmax(min(300px,100%),1fr))] gap-4">
              <section aria-label={t('nodes.engine')} className={`${card} flex flex-col gap-2 p-4`}>
                <div className="flex items-center gap-2">
                  <span className="font-display text-[18px] text-ink-3 italic">{t('nodes.engine')}</span>
                  {engine.state === 'active' ? (
                    <StatusBadge name="ok">{t('nodes.state.active')}</StatusBadge>
                  ) : (
                    <StatusBadge name="pend">{t('nodes.state.standby')}</StatusBadge>
                  )}
                </div>
                <p className="m-0 font-display text-[34px] leading-9">
                  {t('nodes.version', { version: engine.version })}
                </p>
                <p className="m-0 text-small text-ink-2">{t('nodes.engineNote')}</p>
              </section>
              <section aria-label={t('nodes.capacity')} className={`${card} flex flex-col gap-2 p-4`}>
                <span className="font-display text-[18px] text-ink-3 italic">{t('nodes.capacity')}</span>
                <p className="m-0 font-display text-[44px] leading-[44px]">{formatNumber(slots - leases, language)}</p>
                <p className="m-0 text-small text-ink-2">{t('nodes.freeSlots')}</p>
              </section>
            </div>
            {list.length === 0 ? (
              <EmptyState title={t('nodes.none')}>{t('nodes.noneBody')}</EmptyState>
            ) : (
              // A region that scrolls sideways on a phone takes focus, so the keyboard can scroll it.
              // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
              <div role="region" aria-label={t('nodes.table')} tabIndex={0} className={`${card} overflow-x-auto`}>
                <table className="w-full min-w-[760px] border-collapse text-small">
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
                      <th scope="col" className="py-2.5 pr-4 pl-2 font-bold">
                        {t('nodes.col.runtimes')}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((node) => (
                      <NodeRow key={node.id} node={node} engineVersion={engine.version} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </QueryView>
    </ScreenPage>
  );
}

function NodeRow({ node, engineVersion }: { node: Node; engineVersion: string }) {
  const { t } = useTranslation();
  const { language } = usePreferences();
  const labels = Object.entries(node.labels);
  const full = node.leases >= node.slots && node.slots > 0;
  return (
    <tr className="border-t border-line">
      <td className="py-3 pr-2 pl-4 font-mono font-bold">{node.id}</td>
      <td className="px-2 py-3">
        <StatusBadge name="ok">{t('nodes.state.active')}</StatusBadge>
        <div className="mt-[3px] text-tiny text-ink-3">{node.embedded ? t('nodes.embedded') : t('nodes.remote')}</div>
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
      <td className={`px-2 py-3 font-mono ${node.version === engineVersion ? '' : 'text-warn'}`}>
        {node.version === engineVersion ? node.version : t('nodes.behind', { version: node.version })}
      </td>
      <td className="px-2 py-3">
        <span className="flex items-center gap-2">
          <span aria-hidden="true" className="flex max-w-[180px] flex-wrap gap-0.5">
            {Array.from({ length: Math.min(node.slots, 32) }, (_, i) => (
              <span
                key={i}
                className={`h-3.5 w-[9px] rounded-[4px] border ${i < node.leases ? 'border-accent bg-accent' : 'border-control-line'}`}
              />
            ))}
          </span>
          <span className="font-mono text-tiny text-ink-2">
            {t(full ? 'nodes.slotsFull' : 'nodes.slots', {
              used: formatNumber(node.leases, language),
              total: formatNumber(node.slots, language),
            })}
          </span>
        </span>
      </td>
      <td className="px-2 py-3 text-right font-mono">{formatNumber(node.leases, language)}</td>
      <td className="py-3 pr-4 pl-2 font-mono text-tiny text-ink-2">{node.runtimes.join(', ')}</td>
    </tr>
  );
}
