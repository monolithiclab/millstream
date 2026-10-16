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
// the viewport, content that scrolls in its own region (tables, the log tail, code).
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
    return out;
  });
  expect(clipped).toEqual([]);
}
