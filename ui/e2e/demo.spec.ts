import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import { expectNoClipping, expectNoViolations } from './checks';

// The M3 exit (spec section 19): the full reference demo from the browser, once in English (light theme) and once in
// French (dark theme), against millstream run on the demo package (playwright.demo.config.ts). Day 1 crawls four
// listings; the score agent replays its recordings, but L002 has none, so its delivery dead-letters. A person fires
// the run and watches the graph move, follows the run to its parked reviews, reads an agent transcript, decides the
// three reviews from the keyboard (approve after an undo, edit then approve, reject with a reason), sees the run
// settle and the cost per approved item, replays the dead letter and finds the run's metrics. Every state visited
// passes axe (WCAG 2.2 AA) and the clipping check.
const pipeline = 'm3-demo';
const api = `/api/v1/pipelines/${pipeline}`;

type Language = 'en' | 'fr';

// Each visited state: no axe violation, no clipped text.
async function check(page: Page) {
  await expectNoViolations(page);
  await expectNoClipping(page);
}

async function openReviews(request: APIRequestContext): Promise<string[]> {
  const response = await request.get(`/api/v1/inbox?inbox=analysts&state=open`);
  const { items } = (await response.json()) as { items: { deliveryId: string }[] };
  return items.map((r) => r.deliveryId);
}

test('the reference demo from the browser', async ({ page, context, request }) => {
  const language = test.info().project.metadata.language as Language;
  const say = <T>(en: T, fr: T): T => (language === 'en' ? en : fr);
  await context.addInitScript((lang) => window.localStorage.setItem('millstream.language', lang), language);
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.emulateMedia({ reducedMotion: 'reduce' });
  // The undo window is 10 s: the test moves the page's clock past it rather than waiting.
  await page.clock.install();

  // 1. The graph of a pipeline that has never run, open in one tab while the run is fired from another: the live
  // overlays move without a reload, up to the three listings waiting at the review step.
  await page.goto(`/p/${pipeline}/graph`);
  const review = page.locator('[data-step="review"]');
  await expect(review).toBeVisible();
  await check(page);

  const runs = await context.newPage();
  await runs.setViewportSize({ width: 1280, height: 900 });
  await runs.goto(`/p/${pipeline}/runs`);
  await expect(runs.getByText(say('No runs yet', 'Aucun run pour l’instant'))).toBeVisible();
  await check(runs);
  await runs.getByRole('button', { name: say('Run adhoc now', 'Lancer adhoc maintenant') }).click();
  await expect(runs.getByText(say(/^Run .+ started$/, /^Run .+ lancé$/))).toBeVisible();
  // The list reloads once the run is fired: the new run is there.
  const list = runs.getByRole('list', { name: say('Runs, newest first', 'Runs, du plus récent au plus ancien') });
  await expect(list.getByRole('link')).toHaveCount(1);
  await runs.close();

  await expect(review).toHaveAccessibleName(say(/, 3 to review$/, /, 3 à revoir$/));
  await expect(page.locator('[data-step="score"]')).toHaveAccessibleName(say(/Failing/, /En échec/));
  await check(page);

  // 2. The run timeline: everything ran but the reviews, which hold the run.
  await page.getByRole('link', { name: say(/^Run .+ so far$/, /^Exécution de .+ jusqu’ici$/) }).click();
  const report = page.getByRole('complementary', { name: say('Run report', 'Rapport du run') });
  await expect(report).toBeVisible();
  await expect(page.getByText(say('Waiting for review', 'En attente de revue')).first()).toBeVisible();
  await expect(report).toContainText(say('In the review inbox: 3', 'Dans la boîte de revue : 3'));
  await check(page);

  // 3. An item under review and its agent transcript, with tokens and cost.
  await page.goto('/inbox?inbox=analysts');
  const panel = page.getByRole('region', { name: say('Item under review', 'Item en revue') });
  const title = panel.getByRole('heading', { level: 2 });
  await expect(title).toBeVisible();
  await check(page);
  await panel.getByRole('link', { name: /^it_/ }).click();
  const journey = page.getByRole('region', { name: say('Journey', 'Parcours') });
  await journey.getByRole('button', { name: /^score/ }).click();
  await expect(
    journey.getByText(say(/^in \d+ tok · out \d+ tok$/, /^\d+ jetons en entrée · \d+ en sortie$/)),
  ).toBeVisible();
  await expect(journey.getByText('fake-scorer-1')).toBeVisible();
  await expect(journey.getByText(/\$/).first()).toBeVisible();
  await check(page);

  // 4. The three reviews from the keyboard. The queue's order is the server's; each listing gets its decision.
  await page.goto('/inbox?inbox=analysts');
  await expect(title).toBeVisible();
  const before = await openReviews(request);
  expect(before).toHaveLength(3);
  const status = (text: string) => page.getByRole('status').filter({ hasText: text });
  let undone = false;
  for (let left = 2; left >= 0; left--) {
    const listing = ((await title.textContent()) ?? '').split(' ')[0];
    if (listing === 'L003') {
      // Approve, take it back with u (nothing reaches the server), then approve for good.
      if (!undone) {
        await page.keyboard.press('a');
        await expect(status(say(`Approved. ${left} left.`, `Approuvé. Reste : ${left}.`))).toBeAttached();
        await page.keyboard.press('u');
        await expect(title).toHaveText(/^L003/);
        await page.clock.runFor(11_000);
        expect(await openReviews(request)).toHaveLength(left + 1);
        undone = true;
      }
      await page.keyboard.press('a');
      await expect(status(say(`Approved. ${left} left.`, `Approuvé. Reste : ${left}.`))).toBeAttached();
    } else if (listing === 'L004') {
      // e focuses the first editable field: lower the score, then approve with the change (Ctrl+Enter).
      await page.keyboard.press('e');
      const score = page.locator('#review-field-0');
      await expect(score).toBeFocused();
      await score.fill('72');
      await expect(
        panel.getByRole('button', { name: say(/^Approve with changes \(1\)/, /^Approuver avec modifications \(1\)/) }),
      ).toBeVisible();
      await check(page);
      await page.keyboard.press('Control+Enter');
      await expect(
        status(say(`Approved with changes. ${left} left.`, `Approuvé avec modifications. Reste : ${left}.`)),
      ).toBeAttached();
    } else {
      // r opens the reasons; 2 picks the second, poor-condition.
      expect(listing).toBe('L001');
      await page.keyboard.press('r');
      const dialog = page.getByRole('dialog', { name: say('Why reject?', 'Pourquoi rejeter ?') });
      await expect(dialog).toBeVisible();
      await check(page);
      await page.keyboard.press('2');
      await expect(dialog).toHaveCount(0);
      await expect(status(say(`Rejected. ${left} left.`, `Rejeté. Reste : ${left}.`))).toBeAttached();
    }
    await page.clock.runFor(10_000);
    await expect.poll(async () => (await openReviews(request)).length).toBe(left);
  }
  await check(page);

  // The server recorded what was decided, by whom.
  const decisions = await Promise.all(
    before.map(async (id) => {
      const response = await request.get(`${api}/reviews/${id}`);
      return (await response.json()) as { decision?: string; reason?: string; reviewer?: string; patch?: unknown[] };
    }),
  );
  expect(decisions.map((d) => [d.decision, d.reason ?? null, (d.patch ?? []).length]).sort()).toEqual([
    ['approve', null, 0],
    ['approve', null, 1],
    ['reject', 'poor-condition', 0],
  ]);
  expect(new Set(decisions.map((d) => d.reviewer))).toEqual(new Set(['analyst']));

  // 5. The run settles: the two approved listings are priced and reported, L002's dead letter remains.
  const latestRun = async () =>
    ((await (await request.get(`${api}/runs`)).json()) as { items: { id: string; state: string }[] }).items[0];
  await expect.poll(async () => (await latestRun())?.state, { timeout: 30_000 }).toBe('completedWithErrors');
  await page.goto(`/p/${pipeline}/runs/${(await latestRun())?.id ?? ''}`);
  await expect(report).toBeVisible();
  await expect(page.getByText(say('Completed with errors', 'Terminé avec erreurs')).first()).toBeVisible();
  await expect(report).toContainText(say('Cost', 'Coût'));
  await check(page);

  // The cost insight, projected once the run settled, gives the cost per approved item.
  const cost = page.getByRole('article').filter({ hasText: say(/^.*score cost/, /^.*score a coûté/) });
  await expect(async () => {
    await page.goto(`/p/${pipeline}/insights`);
    await expect(cost).toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 45_000 });
  const perApproved = cost.locator('div', {
    has: page.getByText(say('Cost per approved item', 'Coût par item approuvé')),
  });
  await expect(perApproved.locator('dd')).toHaveText(/\d/);
  await check(page);

  // 6. Replay the dead letter: the confirmation says what happens, then that it was done. Without its recording,
  // L002 dies again, and the replayed letter is kept as replayed.
  await page.goto(`/p/${pipeline}/dead-letters`);
  const group = page.getByRole('region', { name: say(/^Group at score/, /^Groupe à score/) });
  await expect(group).toBeVisible();
  await check(page);
  await group.getByRole('button', { name: say('Replay 1', 'Rejouer 1') }).click();
  const confirm = page.getByRole('dialog', { name: say('Replay 1 item at score?', 'Rejouer 1 item à score ?') });
  await expect(confirm).toBeVisible();
  await check(page);
  await confirm.getByRole('button', { name: say('Replay 1 item', 'Rejouer 1 item') }).click();
  await expect(page.getByText(say('1 item replayed', '1 item rejoué'))).toBeVisible();
  await expect(page.getByRole('button', { name: say(/^Replayed 1/, /^Rejouées? 1/) })).toBeVisible();
  await check(page);

  // 7. The metrics explorer shows what the run spent, by step.
  await expect(async () => {
    await page.goto(`/metrics?range=1h&p=${pipeline}&m=delivery_cost_usd`);
    await expect(page.locator('canvas')).toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 45_000 });
  await expect(page.getByRole('combobox', { name: say('Metric', 'Métrique') })).toHaveValue('delivery_cost_usd');
  await expect(page.getByRole('region', { name: say('Series', 'Séries') })).toContainText('score');
  await check(page);
});
