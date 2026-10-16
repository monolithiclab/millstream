import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import { token } from '../playwright.auth.config';
import { expectNoClipping, expectNoViolations } from './checks';

// The UI on a server with auth.token: the sign-in screen in both themes, both languages, at 375 and 1280 px, axe
// clean and unclipped; then signing in, deciding a review from the keyboard and signing out.
const themes = ['light', 'dark'] as const;
const languages = ['en', 'fr'] as const;
const widths = [375, 1280] as const;
const bearer = { Authorization: `Bearer ${token}` };

async function openReviews(request: APIRequestContext): Promise<string[]> {
  const response = await request.get('/api/v1/inbox?inbox=analysts&state=open', { headers: bearer });
  expect(response.ok(), await response.text()).toBe(true);
  return ((await response.json()) as { items: { deliveryId: string }[] }).items.map((r) => r.deliveryId);
}

async function signIn(page: Page, value: string) {
  await page.getByLabel(/^(Token|Jeton)$/).fill(value);
  await page.keyboard.press('Enter');
}

test.beforeAll(async ({ request }) => {
  // Without the token the API refuses; with it, one listing parks in the analysts inbox.
  expect((await request.get('/api/v1/pipelines')).status()).toBe(401);
  const response = await request.post('/api/v1/pipelines/listings-daily/runs', {
    headers: bearer,
    data: {
      trigger: 'adhoc',
      payload: { title: 'T2 · 44 m² · Pantin', priceEur: 268000, surfaceM2: 44 },
      requestId: 'auth-1',
    },
  });
  expect(response.ok(), await response.text()).toBe(true);
  await expect.poll(async () => (await openReviews(request)).length, { timeout: 30_000 }).toBe(1);
});

for (const theme of themes) {
  for (const language of languages) {
    for (const width of widths) {
      test(`sign in · ${theme} · ${language} · ${width}px`, async ({ page }) => {
        await page.setViewportSize({ width, height: 900 });
        await page.emulateMedia({ colorScheme: theme, reducedMotion: 'reduce' });
        await page.addInitScript((lang) => window.localStorage.setItem('millstream.language', lang), language);
        await page.goto('/pipelines');
        await expect(page.getByRole('heading', { level: 1, name: /^(Sign in|Connexion)$/ })).toBeVisible();
        await expect(page.locator('html')).toHaveAttribute('lang', language);
        await expectNoViolations(page);
        await expectNoClipping(page);
        await signIn(page, 'not-the-token');
        await expect(page.getByRole('alert')).toBeVisible();
        await expectNoViolations(page);
        await expectNoClipping(page);
      });
    }
  }
}

test('sign in, decide a review from the keyboard, sign out', async ({ page, request }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.clock.install();
  await page.goto('/inbox?inbox=analysts');
  await signIn(page, token);
  const panel = page.getByRole('region', { name: 'Item under review' });
  await expect(panel.getByRole('heading', { level: 2 })).toBeVisible();
  const [delivery] = await openReviews(request);

  // a approves; the decision is posted once the undo window closes, on the session cookie.
  await page.keyboard.press('a');
  await page.clock.runFor(10_000);
  await expect.poll(async () => (await openReviews(request)).length).toBe(0);
  const review = (await (
    await request.get(`/api/v1/pipelines/listings-daily/reviews/${delivery ?? 'none'}`, { headers: bearer })
  ).json()) as { decision?: string; reviewer?: string };
  expect([review.decision, review.reviewer]).toEqual(['approve', 'operator']);

  // Sign out from the menu: the sign-in screen again, and the cookie no longer opens the API.
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('heading', { level: 1, name: 'Sign in' })).toBeVisible();
  expect((await page.request.get('/api/v1/pipelines')).status()).toBe(401);
  await page.reload();
  await expect(page.getByRole('heading', { level: 1, name: 'Sign in' })).toBeVisible();
});
