import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { App } from '../App';
import { FakeApi } from '../api/fake';
import { fakeEventSource } from '../live/fakeEventSource';
import { screens } from '../routes';
import { renderApp } from '../test/render';
import { Placeholder } from './Placeholder';

// The items list is the one screen not built yet: its route shows the placeholder.
describe('Placeholder', () => {
  it('stands in for the items screen, under its pipeline', async () => {
    renderApp({ route: '/p/listings-daily/items' });
    expect(await screen.findByRole('heading', { level: 1, name: 'Streams and items' })).toBeInTheDocument();
    expect(screen.getByText('Pipeline listings-daily')).toBeInTheDocument();
  });

  it('names no pipeline for a workspace screen', () => {
    render(
      <MemoryRouter>
        <App api={new FakeApi()} eventSource={fakeEventSource} language="en">
          <Placeholder screen={screens.find((s) => s.id === 'nodes')!} />
        </App>
      </MemoryRouter>,
    );
    expect(screen.getByRole('heading', { level: 1, name: 'Nodes' })).toBeInTheDocument();
    expect(screen.queryByText(/^Pipeline /)).not.toBeInTheDocument();
    expect(screen.getByText('This view is not built yet.')).toBeInTheDocument();
  });
});
