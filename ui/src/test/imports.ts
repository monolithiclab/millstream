import { demoFixtures } from '../api/demo';
import type { Fixtures } from '../api/fake';

// The demo data with a second pipeline, deal-flow, importing listings-daily's scored stream as approved (spec
// section 4.12), and a copy of the first listing on it.
export function withImport(): Fixtures {
  const [producer] = demoFixtures.pipelines;
  const listing = demoFixtures.items.find((i) => i.id === 'it_7f3a9c21');
  if (producer === undefined || listing === undefined) throw new Error('the demo data lost its first listing');
  return {
    ...demoFixtures,
    pipelines: [
      ...demoFixtures.pipelines,
      {
        ...producer,
        name: 'deal-flow',
        triggers: [],
        imports: [{ name: 'approved', pipeline: producer.name, stream: listing.stream }],
        steps: [
          {
            name: 'notify',
            action: './actions/notify',
            builtin: false,
            in: ['approved'],
            out: [],
            dlq: 'notify.dlq',
            concurrency: 1,
            paused: false,
          },
        ],
        streams: ['approved', 'notify.dlq'],
      },
    ],
    items: [
      ...demoFixtures.items,
      {
        ...listing,
        id: 'it_c0a1e5d9',
        pipeline: 'deal-flow',
        runId: 'run_import_5e1f',
        stream: 'approved',
        step: 'approved',
        parents: [listing.id],
        parentPipeline: producer.name,
      },
    ],
  };
}
