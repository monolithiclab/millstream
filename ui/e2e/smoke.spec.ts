import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';

// The app shell in both themes, both languages and at 375 px and 1280 px, on the in-memory API (?demo): axe finds
// no WCAG 2.2 AA violation, and the shell's landmarks and controls are present.
const themes = ['light', 'dark'] as const;
const languages = ['en', 'fr'] as const;
const widths = [375, 1280] as const;

async function expectNoViolations(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

for (const theme of themes) {
  for (const language of languages) {
    for (const width of widths) {
      test(`shell · ${theme} · ${language} · ${width}px`, async ({ page }) => {
        await page.setViewportSize({ width, height: 900 });
        await page.emulateMedia({ colorScheme: theme, reducedMotion: 'reduce' });
        await page.addInitScript((lang) => window.localStorage.setItem('millstream.language', lang), language);
        await page.goto('/p/listings-daily/graph?demo');

        await expect(page.locator('html')).toHaveAttribute('lang', language);
        await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
        await expect(page.getByRole('status').first()).toBeVisible();
        await expectNoViolations(page);

        if (width < 768) {
          await page.getByRole('button', { expanded: false }).first().click();
          await expect(page.getByRole('navigation')).toBeVisible();
          await expectNoViolations(page);
          await page.keyboard.press('Escape');
          await expect(page.getByRole('navigation')).toBeHidden();
        }

        await page.keyboard.press('Control+k');
        await expect(page.getByRole('dialog')).toBeVisible();
        await expect(page.getByRole('dialog').getByRole('combobox')).toBeFocused();
        await expectNoViolations(page);
        await page.keyboard.press('Escape');
        await expect(page.getByRole('dialog')).toHaveCount(0);

        await page.keyboard.press('?');
        await expect(page.getByRole('dialog')).toBeVisible();
        await expectNoViolations(page);
      });
    }
  }
}

test('demo mode is marked, and Exit demo returns to the server', async ({ page }) => {
  await page.goto('/pipelines?demo');
  const header = page.getByRole('banner');
  await expect(header.getByText('Demo data')).toBeVisible();
  // It lasts for the tab, without ?demo.
  await page.goto('/nodes');
  await expect(header.getByText('Demo data')).toBeVisible();
  await header.getByRole('button', { name: 'Exit demo' }).click();
  await expect(page).not.toHaveURL(/demo/);
  await expect(page.getByRole('heading', { level: 1, name: 'Nodes' })).toBeVisible();
  await expect(header.getByText('Demo data')).toHaveCount(0);
  await page.reload();
  await expect(header.getByText('Demo data')).toHaveCount(0);
});

test('explicit data-theme overrides the system theme', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'light' });
  await page.addInitScript(() => window.localStorage.setItem('millstream.theme', 'dark'));
  await page.goto('/pipelines?demo');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  const background = await page.evaluate(() => getComputedStyle(document.documentElement).backgroundColor);
  expect(background).toBe('rgb(20, 18, 15)');
  await expectNoViolations(page);
});

test('client routes load from a deep link and the skip link reaches main', async ({ page }) => {
  await page.goto('/inbox?demo');
  await expect(page.getByRole('heading', { level: 1, name: 'Inbox' })).toBeVisible();
  await page.keyboard.press('Tab');
  const skip = page.getByRole('link', { name: 'Skip to main content' });
  await expect(skip).toBeFocused();
  await skip.press('Enter');
  await expect(page.locator('#main')).toBeFocused();
});
