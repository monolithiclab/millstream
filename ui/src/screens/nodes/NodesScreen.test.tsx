import { act, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/api';
import { demoFixtures } from '../../api/demo';
import { FakeApi } from '../../api/fake';
import { renderApp } from '../../test/render';
import { NODES_REFRESH_MS } from './NodesScreen';

describe('NodesScreen', () => {
  it('shows the engine, the free capacity and each node', async () => {
    renderApp({ route: '/nodes' });
    const table = await screen.findByRole('table', { name: 'Nodes, their labels, slots and leases' });
    expect(screen.getByText('2 of 8 slots busy')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Engine' })).toHaveTextContent('EngineActiveMillstream 0.0.0');
    expect(screen.getByRole('region', { name: 'Free capacity' })).toHaveTextContent('6');
    const row = within(table).getAllByRole('row')[1]!;
    expect(row).toHaveTextContent('mill-01Activeembedded in the enginenone0.0.02 / 82process');
  });

  it('marks labels, an old version, a full node and a standby engine', async () => {
    const nodes = {
      engine: { state: 'standby' as const, version: '0.9.2' },
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
    expect(screen.getByRole('region', { name: 'Engine' })).toHaveTextContent('Standby');
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
    const api = new FakeApi({ ...demoFixtures, nodes: { engine: { state: 'active', version: '1' }, nodes: [] } });
    const real = api.listNodes;
    api.listNodes = () => (fail ? Promise.reject(new ApiError(503, 'unavailable', 'engine switching over')) : real());
    const { user } = renderApp({ route: '/nodes', api, language: 'fr' });
    expect(await screen.findByRole('alert')).toHaveTextContent('engine switching over');
    fail = false;
    await user.click(screen.getByRole('button', { name: 'Réessayer' }));
    expect(await screen.findByText('Aucun nœud connecté')).toBeInTheDocument();
  });
});
