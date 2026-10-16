import { expect, test as setup } from '@playwright/test';

// Seeds the e2e server (millstream run over e2e/fixture) through the API: four listings that park in the analysts
// and triage inboxes, one the shortlist filters out, so a run settles, and one whose surface is 0, which dies at
// price-check (a division by zero), so there is a dead letter to replay. The screens tests read the analysts inbox;
// the keyboard flow decides the triage one.
export const listings = [
  {
    title: 'T3 · 68 m² · Paris 19e',
    surfaceM2: 68,
    where: 'Rue de Crimée, 75019 Paris',
    priceEur: 412000,
    marginPct: 17.8,
    notes: '',
  },
  {
    title: 'T2 · 44 m² · Pantin',
    surfaceM2: 44,
    where: 'Avenue Jean Lolive, 93500 Pantin',
    priceEur: 268000,
    marginPct: 16.2,
    notes: '',
  },
  {
    title: 'T4 · 91 m² · Montreuil',
    surfaceM2: 91,
    where: 'Rue de Paris, 93100 Montreuil',
    priceEur: 489000,
    marginPct: 15.4,
    notes: '',
  },
  {
    title: 'Studio · 19 m² · Paris 20e',
    surfaceM2: 19,
    where: 'Rue des Pyrénées, 75020 Paris',
    priceEur: 159000,
    marginPct: 21,
    notes: '',
  },
];

setup('seed runs with parked reviews and a dead letter', async ({ request }) => {
  const payloads = [
    ...listings,
    { title: 'Garage · 12 m² · Bobigny', keep: false },
    { title: 'T3 · 68 m² · Paris 19e', priceEur: 412000, surfaceM2: 0, keep: false },
  ];
  for (const [i, payload] of payloads.entries()) {
    const response = await request.post('/api/v1/pipelines/listings-daily/runs', {
      data: { trigger: 'adhoc', payload, requestId: `e2e-${i}` },
    });
    expect(response.ok(), await response.text()).toBe(true);
  }
  const open = async (inbox: string) =>
    ((await (await request.get(`/api/v1/inbox?inbox=${inbox}&state=open`)).json()) as { items: unknown[] }).items
      .length;
  await expect.poll(() => open('analysts'), { timeout: 30_000 }).toBe(listings.length);
  await expect.poll(() => open('triage'), { timeout: 30_000 }).toBe(listings.length);
  const settled = async () =>
    (
      (await (await request.get('/api/v1/pipelines/listings-daily/runs?state=succeeded')).json()) as {
        items: unknown[];
      }
    ).items.length;
  await expect.poll(settled, { timeout: 30_000 }).toBe(1);
  const deadLetters = async () =>
    ((await (await request.get('/api/v1/pipelines/listings-daily/dead-letters')).json()) as unknown[]).length;
  await expect.poll(deadLetters, { timeout: 30_000 }).toBe(1);
});
