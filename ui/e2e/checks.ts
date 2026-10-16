import AxeBuilder from '@axe-core/playwright';
import { expect, type Page } from '@playwright/test';

// WCAG 2.2 AA through axe: no violation.
export async function expectNoViolations(page: Page) {
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

// Review focus 4: French runs longer, nothing may be clipped. The page never scrolls sideways, and no button, link,
// label, heading, table header or badge cuts its own text, except where the design truncates on purpose (ids, the
// queue's titles: class "truncate") or the content scrolls in its own region (tables, the log tail, code).
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
    const selector = 'button, a, label, h1, h2, h3, th, dt, dd, legend, [role="status"], kbd';
    for (const el of document.querySelectorAll<HTMLElement>(selector)) {
      if (el.closest('.truncate, .sr-only, [role="log"], pre, table, .overflow-x-auto') || el.offsetParent === null) {
        continue;
      }
      const style = getComputedStyle(el);
      if (style.overflow === 'visible' && style.textOverflow !== 'ellipsis') {
        // Text that overflows a visible box still shows; check it does not leave the viewport.
        const box = el.getBoundingClientRect();
        if (box.right > window.innerWidth + 1)
          out.push(`${el.tagName} "${el.textContent?.trim()}" leaves the viewport`);
        continue;
      }
      if (el.scrollWidth > el.clientWidth + 1) out.push(`${el.tagName} "${el.textContent?.trim()}" is clipped`);
    }
    return out;
  });
  expect(clipped).toEqual([]);
}
