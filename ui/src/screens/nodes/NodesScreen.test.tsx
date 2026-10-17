import { act, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import type { Node, Nodes } from '../../api/schema';
import { renderApp } from '../../test/render';
import { formatDateTime } from '../../i18n/format';
import { NODES_REFRESH_MS } from './NodesScreen';

const NODES_TABLE = 'Nodes, their labels, slots and leases';

// The row of a node in the nodes table, by id.
async function nodeRow(id: string) {
  const table = await screen.findByRole('table', { name: NODES_TABLE });
  const row = within(table)
    .getAllByRole('row')
    .find((r) => within(r).queryByText(id, { selector: 'div' }) !== null);
  if (!row) throw new Error(`no row for ${id}`);
  return row;
}

function remote(overrides: Partial<Node> = {}): Node {
  return {
    id: 'edge-7',
    embedded: false,
    version: '0.0.0',
    behind: false,
    slots: 2,
    leases: 1,
    labels: {},
    runtimes: ['exec'],
    state: 'active',
    health: 'healthy',
    lastHeartbeat: DEMO_NOW,
    ...overrides,
  };
}

// The demo nodes with some fields replaced; without unschedulable, as a standby answers, when it is dropped.
function withNodes(overrides: Partial<Nodes>, { dropUnschedulable = false } = {}) {
  const nodes: Nodes = { ...demoFixtures.nodes, ...overrides };
  if (dropUnschedulable) delete nodes.unschedulable;
  return new FakeApi({ ...demoFixtures, nodes });
}

const standbyEngine = { id: 'mill-02', state: 'standby' as const, version: '0.0.0' };
const standbyNodes = {
  engine: { ...standbyEngine, environment: 'production' as const, notificationsFailed: 0 },
  engines: [{ id: 'mill-01', state: 'active' as const, version: '0.0.0' }, standbyEngine],
};

describe('NodesScreen', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true, now: new Date(DEMO_NOW) });
  });

  it('shows the engine, the free capacity and the embedded node', async () => {
    renderApp({ route: '/nodes' });
    const row = await nodeRow('mill-01');
    expect(screen.getByText('6 of 16 slots busy')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Active engine' })).toHaveTextContent('mill-01');
    // The draining node's free slots take nothing new.
    expect(screen.getByRole('region', { name: 'Free capacity' })).toHaveTextContent('7');
    expect(row).toHaveTextContent('embedded in the engine');
    expect(row).toHaveTextContent('Active');
    expect(row).toHaveTextContent('Healthy');
    expect(within(row).getByText('No heartbeat')).toHaveClass('sr-only');
    expect(row).toHaveTextContent('exec, agent:api');
    expect(within(row).queryByRole('button')).not.toBeInTheDocument();
  });

  it('shows each remote node: labels, health, heartbeat age, version and slots', async () => {
    renderApp({ route: '/nodes' });
    const gpu = await nodeRow('gpu-01');
    expect(gpu).toHaveTextContent('remote');
    expect(gpu).toHaveTextContent('gpu=true');
    expect(gpu).toHaveTextContent('zone=eu-west');
    expect(gpu).toHaveTextContent('Healthy');
    expect(gpu).toHaveTextContent('12 seconds ago');
    expect(gpu).not.toHaveTextContent('behind');
    const meter = within(gpu).getByRole('meter', { name: 'Slots in use on gpu-01' });
    expect(meter).toHaveAttribute('aria-valuenow', '3');
    expect(meter).toHaveAttribute('aria-valuemax', '4');
    expect(meter).toHaveAttribute('aria-valuetext', '3 of 4 slots busy');
    expect(within(gpu).getByRole('button', { name: 'Drain gpu-01' })).toBeInTheDocument();
    expect(within(gpu).getByRole('button', { name: 'Revoke gpu-01' })).toBeInTheDocument();

    const worker = await nodeRow('worker-03');
    expect(worker).toHaveTextContent('0.9.8 · behind');
    expect(worker).toHaveTextContent('Late');
    expect(worker).toHaveTextContent('Draining');
    expect(worker).toHaveTextContent('2 minutes ago');
    expect(within(worker).queryByRole('button', { name: 'Drain worker-03' })).not.toBeInTheDocument();
    expect(within(worker).getByRole('button', { name: 'Revoke worker-03' })).toBeInTheDocument();
  });

  it('marks a full node', async () => {
    renderApp({ route: '/nodes', api: withNodes({ nodes: [remote({ leases: 2 })] }) });
    expect(await nodeRow('edge-7')).toHaveTextContent('2 / 2 full');
  });

  it('drains a node after a confirmation that states the consequences', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ route: '/nodes', api });
    await user.click(within(await nodeRow('gpu-01')).getByRole('button', { name: 'Drain gpu-01' }));
    const dialog = screen.getByRole('dialog', { name: 'Drain gpu-01?' });
    expect(dialog).toHaveTextContent('The engine stops leasing deliveries to gpu-01.');
    expect(dialog).toHaveTextContent('It finishes the 3 deliveries it holds, then stays idle.');
    expect(dialog).toHaveTextContent('It becomes active again when it is restarted.');
    await user.click(within(dialog).getByRole('button', { name: 'Drain gpu-01' }));
    expect(await screen.findByText(/^gpu-01 is draining/)).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(api.calls).toEqual([{ method: 'drainNode', args: ['gpu-01'] }]);
    const row = await nodeRow('gpu-01');
    expect(row).toHaveTextContent('Draining');
    expect(within(row).queryByRole('button', { name: 'Drain gpu-01' })).not.toBeInTheDocument();
  });

  it('revokes a node: its leases end and its actions go', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ route: '/nodes', api });
    await user.click(within(await nodeRow('gpu-01')).getByRole('button', { name: 'Revoke gpu-01' }));
    const dialog = screen.getByRole('dialog', { name: 'Revoke gpu-01?' });
    expect(dialog).toHaveTextContent('gpu-01 is refused from now on and cannot rejoin under this id.');
    expect(dialog).toHaveTextContent(
      'The 3 deliveries it holds end at once, each counting as a failed attempt, and go to other nodes.',
    );
    await user.click(within(dialog).getByRole('button', { name: 'Revoke gpu-01' }));
    expect(await screen.findByText('gpu-01 revoked: it can no longer rejoin under this id.')).toBeInTheDocument();
    expect(api.calls).toEqual([{ method: 'revokeNode', args: ['gpu-01'] }]);
    const row = await nodeRow('gpu-01');
    expect(row).toHaveTextContent('Revoked');
    expect(within(row).getByRole('meter')).toHaveAttribute('aria-valuenow', '0');
    expect(within(row).queryByRole('button')).not.toBeInTheDocument();
  });

  it('words the dialogs for one delivery held, and for none', async () => {
    const api = withNodes({ nodes: [remote({ leases: 1 }), remote({ id: 'edge-8', leases: 0 })] });
    const { user } = renderApp({ route: '/nodes', api });
    await user.click(await screen.findByRole('button', { name: 'Revoke edge-7' }));
    expect(screen.getByRole('dialog')).toHaveTextContent(
      'The delivery it holds ends at once, counting as a failed attempt, and goes to another node.',
    );
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByRole('button', { name: 'Drain edge-7' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('It finishes the delivery it holds, then stays idle.');
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByRole('button', { name: 'Drain edge-8' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('It holds no delivery, so it goes idle at once.');
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByRole('button', { name: 'Revoke edge-8' }));
    expect(screen.getByRole('dialog')).not.toHaveTextContent('it holds end');
  });

  it('changes nothing when the dialog is cancelled', async () => {
    const api = new FakeApi();
    const { user } = renderApp({ route: '/nodes', api });
    await user.click(within(await nodeRow('gpu-01')).getByRole('button', { name: 'Revoke gpu-01' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(api.calls).toEqual([]);
    expect(await nodeRow('gpu-01')).toHaveTextContent('Active');
  });

  it('reports a refused action, with Retry', async () => {
    const api = new FakeApi();
    api.drainNode = () => Promise.reject(new ApiError(409, 'conflict', 'node gpu-01 is embedded.'));
    const { user } = renderApp({ route: '/nodes', api });
    await user.click(within(await nodeRow('gpu-01')).getByRole('button', { name: 'Drain gpu-01' }));
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Drain gpu-01' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Couldn’t drain gpu-01: node gpu-01 is embedded. Nothing changed.');
    expect(within(alert).getByRole('button', { name: 'Retry' })).toBeInTheDocument();
  });

  it('lists the steps no node can take, with the pending total', async () => {
    renderApp({ route: '/nodes' });
    const heading = await screen.findByRole('heading', { level: 2, name: /^Unschedulable deliveries/ });
    expect(heading).toHaveTextContent('12 pending deliveries');
    const table = screen.getByRole('table', { name: 'Steps whose pending deliveries no node can take' });
    expect(within(table).getAllByRole('row')[1]).toHaveTextContent(
      'photo-taggingresize12runtime container, labels arch=arm64',
    );
  });

  it('says every pending delivery has a node, and leaves the section out when the server does', async () => {
    const { unmount } = renderApp({ route: '/nodes', api: withNodes({ unschedulable: [] }) });
    expect(await screen.findByText('Every pending delivery has a node that can take it')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Unschedulable deliveries' })).toBeInTheDocument();
    unmount();
    renderApp({ route: '/nodes', api: withNodes({}, { dropUnschedulable: true }) });
    await screen.findByRole('table', { name: NODES_TABLE });
    expect(screen.queryByRole('heading', { level: 2, name: /^Unschedulable/ })).not.toBeInTheDocument();
  });

  it('marks labels, an old version and a standby engine, without actions', async () => {
    const nodes = {
      engine: {
        state: 'standby' as const,
        id: 'mill-02',
        version: '0.9.2',
        environment: 'production' as const,
        notificationsFailed: 0,
      },
      engines: [{ id: 'mill-02', state: 'standby' as const, version: '0.9.2' }],
      nodes: [remote({ id: 'gpu-box', version: '0.9.1', behind: true, labels: { gpu: 'true' }, health: 'lost' })],
    };
    renderApp({ route: '/nodes', api: new FakeApi({ ...demoFixtures, nodes }) });
    const row = await nodeRow('gpu-box');
    expect(row).toHaveTextContent('gpu=true');
    expect(row).toHaveTextContent('0.9.1 · behind');
    expect(row).toHaveTextContent('Lost');
    expect(screen.getByRole('region', { name: 'Standby engine' })).toHaveTextContent('Standby');
    expect(screen.queryByRole('button', { name: /^(Drain|Revoke)/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'Actions' })).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Unschedulable deliveries' })).toBeInTheDocument();
    expect(screen.getByText('Known to the active engine only.')).toBeInTheDocument();
  });

  const since = formatDateTime(new Date(Date.parse(DEMO_NOW) - 2000 * 60_000), 'en');

  it('shows a card per engine, with its id, version and since', async () => {
    renderApp({ route: '/nodes' });
    const active = await screen.findByRole('region', { name: 'Active engine' });
    expect(active).toHaveTextContent('mill-01');
    expect(active).toHaveTextContent(`Millstream 0.0.0 · leader since ${since} · epoch 3`);
    expect(active).toHaveTextContent('Runs the pipelines; a standby engine takes over if it fails.');
    const standby = screen.getByRole('region', { name: 'Standby engine' });
    expect(standby).toHaveTextContent('mill-02');
    expect(standby).toHaveTextContent(`Millstream 0.0.0 · standing by since ${since}`);
    expect(standby).not.toHaveTextContent('epoch');
  });

  it('tells the standby is ready to take over, and which card serves the page', async () => {
    renderApp({ route: '/nodes' });
    const standby = await screen.findByRole('region', { name: 'Standby engine' });
    expect(standby).toHaveTextContent(
      'Ready to take over within about 30 seconds. The UI goes read-only during a handover.',
    );
    expect(standby).not.toHaveTextContent('serving this page');
    expect(screen.getByRole('region', { name: 'Active engine' })).toHaveTextContent('serving this page');
    expect(screen.queryByText(/^Read-only/)).not.toBeInTheDocument();
  });

  it('says the page is read-only when a standby serves it', async () => {
    renderApp({ route: '/nodes', api: withNodes(standbyNodes, { dropUnschedulable: true }) });
    expect(await screen.findByRole('status')).toHaveTextContent(
      'Read-only: this engine is the standby. Actions go to the active engine.',
    );
    expect(screen.getByRole('region', { name: 'Standby engine' })).toHaveTextContent('serving this page');
    expect(screen.getByRole('region', { name: 'Active engine' })).not.toHaveTextContent('serving this page');
    expect(screen.queryByRole('button', { name: /^(Drain|Revoke)/ })).not.toBeInTheDocument();
  });

  it('falls back to the serving engine when the list is empty', async () => {
    renderApp({ route: '/nodes', api: withNodes({ engines: [] }) });
    expect(await screen.findByRole('region', { name: 'Active engine' })).toHaveTextContent('mill-01');
    expect(screen.queryByRole('region', { name: 'Standby engine' })).not.toBeInTheDocument();
  });

  it('renders an engine card and a dialog in French', async () => {
    const { user } = renderApp({ route: '/nodes', language: 'fr' });
    const standby = await screen.findByRole('region', { name: 'Moteur de secours' });
    expect(standby).toHaveTextContent('mill-02');
    expect(standby).toHaveTextContent('en attente depuis le');
    expect(standby).toHaveTextContent('Prêt à prendre le relais en environ 30 secondes.');
    expect(await screen.findByText('Signal en retard')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Drainer gpu-01' }));
    expect(screen.getByRole('dialog', { name: 'Drainer gpu-01 ?' })).toHaveTextContent(
      'Il termine les 3 livraisons qu’il détient, puis reste inactif.',
    );
  });

  it('reads the nodes again every ten seconds', async () => {
    const api = new FakeApi();
    const listNodes = vi.spyOn(api, 'listNodes');
    renderApp({ route: '/nodes', api });
    await screen.findByRole('table', { name: NODES_TABLE });
    expect(listNodes).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(NODES_REFRESH_MS));
    expect(listNodes).toHaveBeenCalledTimes(2);
  });

  it('says when no node is connected, and retries a failed read', async () => {
    let fail = true;
    const api = new FakeApi({
      ...demoFixtures,
      nodes: {
        engine: { state: 'active', id: 'mill-01', version: '1', environment: 'development', notificationsFailed: 0 },
        engines: [],
        nodes: [],
      },
    });
    const real = api.listNodes;
    api.listNodes = () => (fail ? Promise.reject(new ApiError(503, 'unavailable', 'engine switching over')) : real());
    const { user } = renderApp({ route: '/nodes', api, language: 'fr' });
    expect(await screen.findByRole('alert')).toHaveTextContent('engine switching over');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Réessayer' }));
    expect(await screen.findByText('Aucun nœud connecté')).toBeInTheDocument();
  });
});
