import { expect, test, type APIRequestContext } from '@playwright/test';
import { expectNoViolations } from './checks';

// The review flow from the keyboard, in French, against the real server: approve, undo within the 10 s window
// (nothing reaches the server), approve again and let the window close, then reject with r and a reason digit.
// The triage inbox is this test's alone (global.setup.ts parks four listings in it).
async function open(request: APIRequestContext): Promise<string[]> {
  const response = await request.get('/api/v1/inbox?inbox=triage&state=open');
  const { items } = (await response.json()) as { items: { deliveryId: string }[] };
  return items.map((r) => r.deliveryId);
}

test('decide reviews from the keyboard in French, with an undo', async ({ page, request }) => {
  await page.clock.install();
  await page.addInitScript(() => window.localStorage.setItem('millstream.language', 'fr'));
  await page.goto('/inbox?inbox=triage');
  const panel = page.getByRole('region', { name: 'Item en revue' });
  const title = panel.getByRole('heading', { level: 2 });
  await expect(title).toBeVisible();
  const before = await open(request);
  expect(before).toHaveLength(4);
  const first = await title.textContent();

  // a approves and opens the next item; u brings the card back and nothing is sent.
  await page.keyboard.press('a');
  await expect(page.getByRole('status').filter({ hasText: 'Approuvé. Reste : 3.' })).toBeAttached();
  await expect(title).not.toHaveText(first ?? '');
  await expect(title).toBeFocused();
  await page.keyboard.press('u');
  await expect(title).toHaveText(first ?? '');
  await page.clock.runFor(11_000);
  expect(await open(request)).toEqual(before);

  // a again, and the decision is posted once the 10 s window closes.
  await page.keyboard.press('a');
  await page.clock.runFor(10_000);
  await expect.poll(async () => (await open(request)).length).toBe(3);

  // r then 2 rejects the next one with the second reason code.
  await page.keyboard.press('r');
  const dialog = page.getByRole('dialog', { name: 'Pourquoi rejeter ?' });
  await expect(dialog).toBeVisible();
  await expectNoViolations(page);
  await page.keyboard.press('2');
  await expect(dialog).toHaveCount(0);
  await expect(page.getByRole('status').filter({ hasText: 'Rejeté. Reste : 2.' })).toBeAttached();
  await page.clock.runFor(10_000);
  await expect.poll(async () => (await open(request)).length).toBe(2);

  // The server recorded one approval and one rejection with its reason code, under the reviews no longer open.
  const after = await open(request);
  const decided = before.filter((id) => !after.includes(id));
  const reviews = await Promise.all(
    decided.map(async (id) => {
      const response = await request.get(`/api/v1/pipelines/listings-daily/reviews/${id}`);
      return (await response.json()) as { decision?: string; reason?: string };
    }),
  );
  expect(reviews.map((r) => [r.decision, r.reason ?? null]).sort()).toEqual([
    ['approve', null],
    ['reject', 'stale'],
  ]);
});
