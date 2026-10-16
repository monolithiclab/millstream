import { expect, test, type Page } from '@playwright/test';
import { expectNoClipping, expectNoViolations } from './checks';

// The screens of M3 Task 5 against millstream run on the e2e fixture, seeded by global.setup.ts: in both themes,
// both languages, at 375 and 1280 px, axe finds no WCAG 2.2 AA violation and no text is clipped.
const themes = ['light', 'dark'] as const;
const languages = ['en', 'fr'] as const;
const widths = [375, 1280] as const;

async function settledRun(page: Page): Promise<string> {
  const response = await page.request.get('/api/v1/pipelines/listings-daily/runs?state=succeeded');
  const { items } = (await response.json()) as { items: { id: string }[] };
  return items[0]?.id ?? 'none';
}

// Each screen, and what shows it has loaded past its placeholders.
const screens: { name: string; path: (page: Page) => Promise<string>; ready: (page: Page) => Promise<void> }[] = [
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
    ready: (page) => expect(page.getByRole('table')).toBeVisible(),
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
        });
      }
    }
  }
}

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
