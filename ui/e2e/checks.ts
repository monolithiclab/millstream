import AxeBuilder from '@axe-core/playwright';
import { expect, type Page } from '@playwright/test';

// WCAG 2.2 AA through axe: no violation.
export async function expectNoViolations(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

// Review focus 4: French runs longer, yet nothing is clipped. The page never scrolls sideways; no element whose box
// clips (overflow hidden or clip, or an ellipsis) cuts its own text; and no button, link, label, heading, table
// header or badge leaves the viewport. Exempt: what the design truncates on purpose (ids and URLs carry a title, the
// queue's titles are class "truncate"), what is not shown, the graph's pane (it pans and clips by nature), and for
// the viewport, content that scrolls in its own region (tables, the log tail, code). Vertically, an open dialog fits
// the viewport and scrolls what it cannot show.
export async function expectNoClipping(page: Page) {
  const clipped = await page.evaluate(() => {
    const out: string[] = [];
    if (document.documentElement.scrollWidth > window.innerWidth + 1) {
      // Name the innermost elements that stick out, to say what to fix.
      const wide = [...document.querySelectorAll<HTMLElement>('body *')].filter(
        (el) => el.getBoundingClientRect().right > window.innerWidth + 1 && el.children.length === 0,
      );
      const names = wide
        .slice(0, 3)
        .map((el) => `${el.tagName}.${el.className.toString().split(' ').slice(0, 3).join('.')}`);
      out.push(
        `page scrolls sideways: ${document.documentElement.scrollWidth} > ${window.innerWidth} (${names.join(', ')})`,
      );
    }
    const exempt = '.react-flow, [title], .truncate, .sr-only, [aria-hidden="true"]';
    const scrolls = '[role="log"], pre, table, .overflow-x-auto';
    const labels = 'button, a, label, h1, h2, h3, th, dt, dd, legend, [role="status"], kbd';
    const text = (el: HTMLElement) => (el.textContent ?? '').trim();
    for (const el of document.querySelectorAll<HTMLElement>('body *')) {
      if (el.closest(exempt) || el.offsetParent === null) continue;
      const style = getComputedStyle(el);
      const clips = ['hidden', 'clip'].includes(style.overflowX) || style.textOverflow === 'ellipsis';
      const own = [...el.childNodes].some((n) => n.nodeType === Node.TEXT_NODE && (n.textContent ?? '').trim() !== '');
      if (clips && (own || el.matches(labels)) && el.scrollWidth > el.clientWidth + 1) {
        out.push(`${el.tagName} "${text(el)}" is clipped`);
      } else if (!clips && el.matches(labels) && !el.closest(scrolls)) {
        if (el.getBoundingClientRect().right > window.innerWidth + 1) {
          out.push(`${el.tagName} "${text(el)}" leaves the viewport`);
        }
      }
    }
    // A dialog stays within the viewport's height, and what does not fit scrolls inside it, so every control is
    // reachable on a short screen.
    for (const dialog of document.querySelectorAll<HTMLElement>('[role="dialog"]')) {
      const box = dialog.getBoundingClientRect();
      const name = dialog.getAttribute('aria-labelledby') ?? '';
      const title = (document.getElementById(name)?.textContent ?? '').trim();
      if (box.top < -1 || box.bottom > window.innerHeight + 1) {
        out.push(
          `dialog "${title}" is cut off: ${Math.round(box.top)} to ${Math.round(box.bottom)} of ${window.innerHeight}`,
        );
      }
      const scrolls = ['auto', 'scroll'].includes(getComputedStyle(dialog).overflowY);
      if (dialog.scrollHeight > dialog.clientHeight + 1 && !scrolls) {
        out.push(`dialog "${title}" hides content it cannot scroll to`);
      }
    }
    return out;
  });
  expect(clipped).toEqual([]);
}

// The page never scrolls sideways, at any width.
export async function expectNoPageScroll(page: Page) {
  const widths = await page.evaluate(() => ({
    scroll: document.documentElement.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  expect(widths.scroll).toBeLessThanOrEqual(widths.client);
}

// Where the screen has a help toggle: the keyboard opens it onto its text, axe passes with it open, and Escape
// closes it with focus back on the toggle. A screen without one passes.
export async function expectHelpToggleWorks(page: Page) {
  const toggle = page.getByRole('button', { name: /^(About this page|À propos de cette page)$/ }).first();
  if ((await toggle.count()) === 0) return;
  await toggle.focus();
  await page.keyboard.press('Enter');
  await expect(toggle).toHaveAttribute('aria-expanded', 'true');
  const popover = page.locator(`[id="${(await toggle.getAttribute('aria-controls')) ?? ''}"]`);
  await expect(popover).toBeVisible();
  await expect(popover).not.toHaveText('');
  await expectNoViolations(page);
  await page.keyboard.press('Escape');
  await expect(toggle).toHaveAttribute('aria-expanded', 'false');
  await expect(toggle).toBeFocused();
}

// Every select draws its chevron inside its own box (Chromium put the platform arrow against the border).
export async function expectChevronsInsideSelects(page: Page) {
  const outside = await page.evaluate(() =>
    [...document.querySelectorAll<HTMLSelectElement>('select')]
      .filter((select) => select.offsetParent !== null)
      .flatMap((select) => {
        const chevron = select.parentElement?.querySelector('svg');
        if (!chevron) return [`select "${select.getAttribute('aria-label') ?? select.name}" has no chevron`];
        const s = select.getBoundingClientRect();
        const c = chevron.getBoundingClientRect();
        const inside = c.left >= s.left && c.right <= s.right && c.top >= s.top && c.bottom <= s.bottom;
        return inside ? [] : [`select "${select.getAttribute('aria-label') ?? select.name}": chevron outside its box`];
      }),
  );
  expect(outside).toEqual([]);
}
