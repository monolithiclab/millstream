import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

// The build lands in internal/ui/dist, which the Go binary embeds (internal/ui). make ui empties it first, keeping
// the committed placeholder.html, so Vite must not empty it itself.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: '../internal/ui/dist',
    emptyOutDir: false,
  },
  server: {
    // millstream run serves the API; the dev server proxies to it.
    proxy: { '/api': 'http://host.docker.internal:7070' },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/**/*.test.{ts,tsx}', 'src/test/**', 'src/main.tsx'],
      reporter: ['text-summary'],
      thresholds: { lines: 85, functions: 85, branches: 80, statements: 85 },
    },
  },
});
