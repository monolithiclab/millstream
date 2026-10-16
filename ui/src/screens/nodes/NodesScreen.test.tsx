import { act, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { DEMO_NOW, demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';
import { formatDateTime } from '../../i18n/format';
import { NODES_REFRESH_MS } from './NodesScreen';

describe('NodesScreen', () => {
  it('shows the engine, the free capacity and each node', async () => {
    renderApp({ route: '/nodes' });
    const table = await screen.findByRole('table', { name: 'Nodes, their labels, slots and leases' });
    expect(screen.getByText('2 of 8 slots busy')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Active engine' })).toHaveTextContent('mill-01');
    expect(screen.getByRole('region', { name: 'Free capacity' })).toHaveTextContent('6');
    const row = within(table).getAllByRole('row')[1]!;
    expect(row).toHaveTextContent('mill-01Activeembedded in the enginenone0.0.02 / 82process');
  });

  it('marks labels, an old version, a full node and a standby engine', async () => {
    const nodes = {
      engine: { state: 'standby' as const, id: 'mill-02', version: '0.9.2', notificationsFailed: 0 },
      engines: [{ id: 'mill-02', state: 'standby' as const, version: '0.9.2' }],
      nodes: [
        {
          id: 'gpu-box',
          embedded: false,
          version: '0.9.1',
          slots: 4,
          leases: 4,
          labels: { gpu: 'true' },
          runtimes: ['docker'],
        },
      ],
    };
    renderApp({ route: '/nodes', api: new FakeApi({ ...demoFixtures, nodes }) });
    const row = (await screen.findAllByRole('row'))[1]!;
    expect(row).toHaveTextContent('gpu-boxActiveremotegpu=true0.9.1 · behind4 / 4 full');
    expect(screen.getByRole('region', { name: 'Standby engine' })).toHaveTextContent('Standby');
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
    const standby = { id: 'mill-02', state: 'standby' as const, version: '0.0.0' };
    const nodes = {
      ...demoFixtures.nodes,
      engine: { ...standby, notificationsFailed: 0 },
      engines: [{ id: 'mill-01', state: 'active' as const, version: '0.0.0' }, standby],
    };
    renderApp({ route: '/nodes', api: new FakeApi({ ...demoFixtures, nodes }) });
    expect(await screen.findByRole('status')).toHaveTextContent(
      'Read-only: this engine is the standby. Actions go to the active engine.',
    );
    expect(screen.getByRole('region', { name: 'Standby engine' })).toHaveTextContent('serving this page');
    expect(screen.getByRole('region', { name: 'Active engine' })).not.toHaveTextContent('serving this page');
  });

  it('falls back to the serving engine when the list is empty', async () => {
    const nodes = { ...demoFixtures.nodes, engines: [] };
    renderApp({ route: '/nodes', api: new FakeApi({ ...demoFixtures, nodes }) });
    expect(await screen.findByRole('region', { name: 'Active engine' })).toHaveTextContent('mill-01');
    expect(screen.queryByRole('region', { name: 'Standby engine' })).not.toBeInTheDocument();
  });

  it('renders an engine card in French', async () => {
    renderApp({ route: '/nodes', language: 'fr' });
    const standby = await screen.findByRole('region', { name: 'Moteur de secours' });
    expect(standby).toHaveTextContent('mill-02');
    expect(standby).toHaveTextContent('en attente depuis le');
    expect(standby).toHaveTextContent('Prêt à prendre le relais en environ 30 secondes.');
  });

  it('reads the nodes again every ten seconds', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const api = new FakeApi();
    const listNodes = vi.spyOn(api, 'listNodes');
    renderApp({ route: '/nodes', api });
    await screen.findByRole('table');
    expect(listNodes).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(NODES_REFRESH_MS));
    expect(listNodes).toHaveBeenCalledTimes(2);
  });

  it('says when no node is connected, and retries a failed read', async () => {
    let fail = true;
    const api = new FakeApi({
      ...demoFixtures,
      nodes: {
        engine: { state: 'active', id: 'mill-01', version: '1', notificationsFailed: 0 },
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
