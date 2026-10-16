import { expect, test, type Page } from '@playwright/test';
import { expectNoClipping, expectNoViolations } from './checks';

// Every built screen against millstream run on the e2e fixture, seeded by global.setup.ts: in both themes, both
// languages, at 375 and 1280 px, axe finds no WCAG 2.2 AA violation and no text is clipped. Then the flows that
// cross screens: the graph to an item to a replayed dead letter, the reject sheet, the timeline's density strip and
// the metrics explorer.
const themes = ['light', 'dark'] as const;
const languages = ['en', 'fr'] as const;
const widths = [375, 1280] as const;
const api = '/api/v1/pipelines/listings-daily';

async function read<T>(page: Page, path: string): Promise<T> {
  return (await (await page.request.get(`${api}${path}`)).json()) as T;
}

async function settledRun(page: Page): Promise<string> {
  const { items } = await read<{ items: { id: string }[] }>(page, '/runs?state=succeeded');
  return items[0]?.id ?? 'none';
}

// The listing that died at price-check: the dead letter's parent.
async function deadItem(page: Page): Promise<string> {
  const groups = await read<{ deadLetters: { id: string }[] }[]>(page, '/dead-letters');
  const letter = await read<{ parents: string[] }>(page, `/items/${groups[0]?.deadLetters[0]?.id ?? 'none'}`);
  return letter.parents[0] ?? 'none';
}

interface ScreenCase {
  name: string;
  path: (page: Page) => Promise<string>;
  // What shows the screen has loaded past its placeholders.
  ready: (page: Page) => Promise<void>;
  // A state the screen opens into (a side panel, a dialog), checked as well.
  open?: (page: Page) => Promise<void>;
}

const screens: ScreenCase[] = [
  {
    name: 'pipelines',
    path: async () => '/pipelines',
    ready: (page) => expect(page.getByRole('table')).toBeVisible(),
    open: async (page) => {
      await page.getByRole('button', { name: /^(Roll back|Restaurer une révision de) listings-daily$/ }).click();
      await expect(page.getByRole('dialog')).toBeVisible();
    },
  },
  {
    name: 'pipelines disable',
    path: async () => '/pipelines',
    ready: (page) => expect(page.getByRole('table')).toBeVisible(),
    open: async (page) => {
      await page.getByRole('button', { name: /^(Disable|Désactiver) listings-daily$/ }).click();
      await expect(page.getByRole('dialog')).toBeVisible();
    },
  },
  {
    name: 'graph',
    path: async () => '/p/listings-daily/graph',
    ready: async (page) => {
      await expect(page.locator('[data-step="price-check"]')).toBeVisible();
      await expect(page.getByRole('region', { name: 'Webhooks' })).toBeVisible();
    },
    open: async (page) => {
      await page.locator('[data-step="price-check"]').click();
      await expect(page.getByRole('complementary')).toBeVisible();
    },
  },
  {
    name: 'item',
    path: async (page) => `/p/listings-daily/items/${await deadItem(page)}`,
    ready: (page) => expect(page.getByRole('tablist')).toBeVisible(),
  },
  {
    name: 'items',
    path: async () => '/p/listings-daily/items',
    ready: (page) =>
      expect(page.getByRole('list', { name: /Items, oldest first|Items, du plus ancien/ })).toBeVisible(),
    // A where the server cannot compile: its message at the field.
    open: async (page) => {
      const where = page.getByRole('textbox', { name: /Where \(CEL\)|Condition \(CEL\)/ });
      await where.fill('item.priceEur >');
      await where.press('Enter');
      await expect(page.getByRole('alert')).toContainText(/where:/);
      await expect(where).toHaveAttribute('aria-invalid', 'true');
    },
  },
  {
    name: 'dead letters',
    path: async () => '/p/listings-daily/dead-letters',
    ready: (page) => expect(page.getByRole('region').first()).toBeVisible(),
    open: async (page) => {
      await page
        .getByRole('button', { name: /^(Replay|Rejouer) \d+$/ })
        .first()
        .click();
      await expect(page.getByRole('dialog')).toBeVisible();
    },
  },
  {
    name: 'inbox',
    path: async () => '/inbox?inbox=analysts',
    ready: (page) => expect(page.getByRole('region', { name: /Item under review|Item en revue/ })).toBeVisible(),
  },
  {
    name: 'runs',
    path: async () => '/p/listings-daily/runs',
    ready: (page) => expect(page.getByRole('list', { name: /Runs, newest first|Runs, du plus récent/ })).toBeVisible(),
  },
  {
    name: 'run',
    path: async (page) => `/p/listings-daily/runs/${await settledRun(page)}`,
    ready: (page) => expect(page.getByRole('complementary', { name: /Run report|Rapport du run/ })).toBeVisible(),
  },
  {
    name: 'metrics',
    path: async () => '/metrics?range=1h',
    ready: (page) => expect(page.getByTestId('loading')).toHaveCount(0),
  },
  {
    name: 'insights',
    path: async () => '/p/listings-daily/insights',
    ready: (page) =>
      expect(page.getByRole('complementary', { name: /Filter insights|Filtrer les constats/ })).toBeVisible(),
  },
  {
    name: 'lessons',
    path: async () => '/p/listings-daily/lessons',
    ready: (page) => expect(page.getByTestId('loading')).toHaveCount(0),
  },
  {
    name: 'nodes',
    path: async () => '/nodes',
    ready: (page) => expect(page.getByRole('table', { name: /^(Nodes|Nœuds), / })).toBeVisible(),
  },
  {
    name: 'logs',
    path: async () => '/logs?pipeline=listings-daily',
    ready: (page) => expect(page.getByRole('log')).toBeVisible(),
  },
];

for (const screen of screens) {
  for (const theme of themes) {
    for (const language of languages) {
      for (const width of widths) {
        test(`${screen.name} · ${theme} · ${language} · ${width}px`, async ({ page }) => {
          await page.setViewportSize({ width, height: 900 });
          await page.emulateMedia({ colorScheme: theme, reducedMotion: 'reduce' });
          await page.addInitScript((lang) => window.localStorage.setItem('millstream.language', lang), language);
          await page.goto(await screen.path(page));
          await expect(page.locator('html')).toHaveAttribute('lang', language);
          await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
          await screen.ready(page);
          await expectNoViolations(page);
          await expectNoClipping(page);
          if (screen.open) {
            await screen.open(page);
            await expectNoViolations(page);
            await expectNoClipping(page);
          }
        });
      }
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

test('rotate a webhook token from the graph and see the new one once', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto('/p/listings-daily/graph');
  const webhooks = page.getByRole('region', { name: 'Webhooks' });
  await expect(webhooks.getByText(/\/hooks\/listings-daily\/listing$/)).toBeVisible();

  const rotate = webhooks.getByRole('button', { name: 'Rotate token of listing' });
  await rotate.click();
  const confirm = page.getByRole('dialog', { name: 'Rotate the token of listing?' });
  await expect(confirm).toContainText('stops working immediately');
  await confirm.getByRole('button', { name: 'Rotate token' }).click();

  const dialog = page.getByRole('dialog', { name: 'New token for listing' });
  await expect(dialog.getByTestId('trigger-token')).toHaveText(/^\S{20,}$/);
  await expect(dialog).toContainText('it will not be shown again');
  await expectNoViolations(page);
  await expectNoClipping(page);
  await dialog.getByRole('button', { name: 'Done' }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  await expect(rotate).toBeFocused();
});

test('the items list filters a stream with CEL and opens an item', async ({ page }) => {
  await page.goto('/p/listings-daily/items');
  await page.getByRole('combobox', { name: 'Stream' }).selectOption('listings.shortlist');
  await expect(page).toHaveURL(/stream=listings.shortlist/);
  const where = page.getByRole('textbox', { name: 'Where (CEL)' });
  await where.fill('item.surfaceM2 > 60');
  await where.press('Enter');
  await expect(page).toHaveURL(/where=item.surfaceM2/);
  const list = page.getByRole('list', { name: 'Items, oldest first' });
  // T3 · 68 m² and T4 · 91 m² are over 60 m².
  await expect(list.getByRole('link')).toHaveCount(2);
  await list.getByRole('link', { name: /T4 · 91 m²/ }).click();
  await expect(page.getByRole('heading', { level: 1, name: 'T4 · 91 m² · Montreuil' })).toBeVisible();
  await page.goBack();
  await expect(where).toHaveValue('item.surfaceM2 > 60');
});

test('the inbox at 375 px opens the reject sheet, also axe-clean', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 800 });
  await page.addInitScript(() => window.localStorage.setItem('millstream.language', 'fr'));
  await page.goto('/inbox?inbox=analysts');
  await page.getByRole('button', { name: 'Rejeter…' }).click();
  await expect(page.getByRole('dialog', { name: 'Pourquoi rejeter ?' })).toBeVisible();
  await expectNoViolations(page);
  await expectNoClipping(page);
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).toHaveCount(0);
});

test('the reject sheet with nine reasons scrolls on a short phone', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 600 });
  await page.goto('/inbox?inbox=analysts');
  await page.getByRole('button', { name: 'Reject…' }).click();
  const sheet = page.getByRole('dialog', { name: 'Why reject?' });
  await expect(sheet.getByRole('button')).toHaveCount(9 + 1);
  await expectNoClipping(page);
  // The last reason and the note are reachable by scrolling the sheet.
  const last = sheet.getByRole('button', { name: /other/ });
  await last.scrollIntoViewIfNeeded();
  await expect(last).toBeInViewport();
  await sheet.getByRole('textbox').scrollIntoViewIfNeeded();
  await expect(sheet.getByRole('textbox')).toBeInViewport();
  await expectNoViolations(page);
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).toHaveCount(0);
});

test('the run timeline expands a step into its density strip', async ({ page }) => {
  await page.goto(`/p/listings-daily/runs/${await settledRun(page)}`);
  const step = page.getByRole('button', { name: 'shortlist' });
  await step.click();
  await expect(step).toHaveAttribute('aria-expanded', 'true');
  await expectNoViolations(page);
});

test('the metrics explorer draws a chart and keeps the view in the URL', async ({ page }) => {
  await page.goto('/metrics?range=1h');
  const metric = page.getByRole('combobox', { name: 'Metric' });
  await expect(metric).toBeEnabled();
  await metric.selectOption('items_in');
  await expect(page).toHaveURL(/m=items_in/);
  await expect(page.locator('canvas')).toBeVisible();
  await page.reload();
  await expect(page.getByRole('combobox', { name: 'Metric' })).toHaveValue('items_in');
  await expectNoViolations(page);
});
