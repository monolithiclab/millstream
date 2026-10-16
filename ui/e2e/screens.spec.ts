import AxeBuilder from '@axe-core/playwright';
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';

// The pipeline graph, item detail, dead letters and pipelines screens against millstream run on the e2e fixture
// (builtin steps only): axe finds no WCAG 2.2 AA violation in either theme or language at 375 and 1280 px, no text
// is clipped and the page never scrolls sideways; then one flow: open the graph, select a step, open an item,
// replay a dead letter.
const themes = ['light', 'dark'] as const;
const languages = ['en', 'fr'] as const;
const widths = [375, 1280] as const;
const api = '/api/v1/pipelines/listings-daily';

async function expectNoViolations(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

// Review focus 4: French runs longer, yet no text is cut by its box and the page never scrolls sideways. Ids and
// URLs may truncate in the middle by design (they carry a title); the graph's pane pans and clips by nature.
async function expectNoClipping(page: Page) {
  const faults = await page.evaluate(() => {
    const out: string[] = [];
    if (document.documentElement.scrollWidth > window.innerWidth + 1)
      out.push(`page scrolls sideways: ${document.documentElement.scrollWidth} > ${window.innerWidth}`);
    for (const el of document.querySelectorAll<HTMLElement>('#main *')) {
      if (el.closest('.react-flow, [title], .sr-only, [aria-hidden="true"]')) continue;
      const own = [...el.childNodes].some((n) => n.nodeType === Node.TEXT_NODE && (n.textContent ?? '').trim() !== '');
      if (!own) continue;
      const style = getComputedStyle(el);
      const clips = ['hidden', 'clip'].includes(style.overflowX) || style.textOverflow === 'ellipsis';
      if (clips && el.scrollWidth > el.clientWidth + 1) out.push(`${el.tagName}: “${(el.textContent ?? '').trim()}”`);
    }
    return out;
  });
  expect(faults).toEqual([]);
}

// The data the screens show: one listing that dies at price-check (a division by zero), one that waits in review.
// Request ids make the runs idempotent across tests and workers.
async function seed(request: APIRequestContext): Promise<{ item: string; deadLetter: string }> {
  for (const [requestId, payload] of [
    ['e2e-dead', { title: 'T3 · 68 m² · Paris 19e', priceEur: 412000, surfaceM2: 0 }],
    ['e2e-review', { title: 'Studio · 22 m² · Paris 11e', priceEur: 200000, surfaceM2: 22 }],
  ] as const) {
    const run = await request.post(`${api}/runs`, { data: { payload, requestId } });
    expect(run.ok()).toBe(true);
  }
  let groups: { step: string; deadLetters: { id: string }[] }[] = [];
  await expect
    .poll(async () => {
      groups = (await (await request.get(`${api}/dead-letters`)).json()) as typeof groups;
      return groups.length;
    })
    .toBeGreaterThan(0);
  const deadLetter = groups[0]?.deadLetters[0]?.id ?? '';
  const letter = (await (await request.get(`${api}/items/${deadLetter}`)).json()) as { parents: string[] };
  const item = letter.parents[0] ?? '';
  expect([deadLetter, item]).not.toContain('');
  return { item, deadLetter };
}

let data: { item: string; deadLetter: string };

test.beforeAll(async ({ request }) => {
  data = await seed(request);
});

async function open(page: Page, path: string, theme: string, language: string, width: number) {
  await page.setViewportSize({ width, height: 900 });
  await page.emulateMedia({ colorScheme: theme as 'light' | 'dark', reducedMotion: 'reduce' });
  await page.addInitScript((lang) => window.localStorage.setItem('millstream.language', lang), language);
  await page.goto(path);
  await expect(page.locator('html')).toHaveAttribute('lang', language);
}

for (const theme of themes) {
  for (const language of languages) {
    for (const width of widths) {
      test(`screens · ${theme} · ${language} · ${width}px`, async ({ page }) => {
        await open(page, '/pipelines', theme, language, width);
        await expect(page.getByRole('table')).toBeVisible();
        await expectNoViolations(page);
        await expectNoClipping(page);

        await page.goto('/p/listings-daily/graph');
        await expect(page.locator('[data-step="price-check"]')).toBeVisible();
        await expectNoViolations(page);
        await expectNoClipping(page);
        await page.locator('[data-step="price-check"]').click();
        await expect(page.getByRole('complementary')).toBeVisible();
        await expectNoViolations(page);

        await page.goto(`/p/listings-daily/items/${data.item}`);
        await expect(page.getByRole('tablist')).toBeVisible();
        await expectNoViolations(page);
        await expectNoClipping(page);

        await page.goto('/p/listings-daily/dead-letters');
        await expect(page.getByRole('region').first()).toBeVisible();
        await expectNoViolations(page);
        await expectNoClipping(page);
        await page
          .getByRole('button', { name: /^(Replay|Rejouer) \d+$/ })
          .first()
          .click();
        await expect(page.getByRole('dialog')).toBeVisible();
        await expectNoViolations(page);
      });
    }
  }
}

test('open the graph, select a step, open an item, replay a dead letter', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto('/p/listings-daily/graph');

  // The failing step is outlined and badged; Enter opens its panel with focus on its heading.
  const step = page.locator('[data-step="price-check"]');
  await expect(step).toHaveAccessibleName(/^price-check, Filter step, Failing/);
  await step.focus();
  await page.keyboard.press('Enter');
  const panel = page.getByRole('complementary', { name: 'price-check' });
  await expect(panel.getByRole('heading', { name: 'price-check' })).toBeFocused();
  await expect(panel.getByText('Recent failures')).toBeVisible();

  // Its recent failure opens the dead letter's item; the journey shows the dead delivery, open on its error.
  await panel.getByRole('link', { name: /^it_/ }).first().click();
  await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
  const journey = page.getByRole('region', { name: 'Journey' });
  await expect(journey.getByRole('button', { name: /price-check/, expanded: true })).toContainText('Dead');
  await expect(journey.getByText(/division by zero/).first()).toBeVisible();

  // Replay it from the dead letters: the confirmation states the consequence, then says it was done.
  await page.goto('/p/listings-daily/dead-letters');
  const group = page.getByRole('region', { name: /^Group at price-check/ });
  await group.getByRole('button', { name: /^Replay \d+$/ }).click();
  const dialog = page.getByRole('dialog', { name: /^Replay \d+ items? at price-check\?$/ });
  await expect(dialog).toContainText('They go back into price-check’s queue now, in their own runs.');
  await dialog.getByRole('button', { name: /^Replay \d+ items?$/ }).click();
  await expect(page.getByText(/^\d+ items? replayed$/)).toBeVisible();
  await expect(page.getByRole('dialog')).toHaveCount(0);

  // The replay fails the same way: a new dead letter comes back, and the replayed one is kept as replayed.
  await expect(page.getByRole('button', { name: /^Replayed [1-9]/ })).toBeVisible();
});
