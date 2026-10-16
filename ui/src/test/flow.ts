import { vi } from 'vitest';

// What React Flow needs from the browser that jsdom lacks (React Flow's testing guide): a ResizeObserver that reports
// at once, DOMMatrixReadOnly for the viewport transform, and element sizes from their inline style, so nodes count
// as measured and render visible. setup.ts installs it for every test file: any route may reach the graph.
export function installReactFlowDom(): void {
  class ResizeObserver {
    constructor(private readonly callback: ResizeObserverCallback) {}
    observe(target: Element) {
      const { offsetWidth: width, offsetHeight: height } = target as HTMLElement;
      const entry = {
        target,
        contentRect: { width, height },
        borderBoxSize: [{ inlineSize: width, blockSize: height }],
      };
      this.callback([entry as unknown as ResizeObserverEntry], this as unknown as globalThis.ResizeObserver);
    }
    unobserve() {
      // nothing observed to forget
    }
    disconnect() {
      // nothing to disconnect
    }
  }
  class DOMMatrixReadOnly {
    m22: number;
    constructor(transform?: string) {
      const scale = /scale\(([\d.]+)\)/.exec(transform ?? '')?.[1];
      this.m22 = scale === undefined ? 1 : Number(scale);
    }
  }
  Object.assign(globalThis, { ResizeObserver, DOMMatrixReadOnly });
  Object.defineProperties(HTMLElement.prototype, {
    offsetHeight: {
      configurable: true,
      get(this: HTMLElement) {
        return parseFloat(this.style.height) || 600;
      },
    },
    offsetWidth: {
      configurable: true,
      get(this: HTMLElement) {
        return parseFloat(this.style.width) || 1000;
      },
    },
  });
  (SVGElement.prototype as unknown as { getBBox: () => DOMRect }).getBBox = () =>
    ({ x: 0, y: 0, width: 0, height: 0 }) as DOMRect;
}

// A matchMedia that answers each query with matches(query): reduced motion, a phone width (undone by
// vi.unstubAllGlobals after each test).
export function mockMatchMedia(matches: (query: string) => boolean): void {
  vi.stubGlobal(
    'matchMedia',
    (query: string): MediaQueryList =>
      ({
        matches: matches(query),
        media: query,
        addEventListener: () => undefined,
        removeEventListener: () => undefined,
      }) as unknown as MediaQueryList,
  );
}
