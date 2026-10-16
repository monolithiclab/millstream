import { defineConfig } from '@playwright/test';

// Runs in the pinned Playwright image (make test-ui-e2e) against the binary itself: make test-ui-e2e cross-compiles
// millstream with the built UI embedded into .build/, and millstream run serves the API and the UI over the e2e
// fixture package on a database of its own.
export default defineConfig({
  testDir: 'e2e',
  outputDir: 'test-results',
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  use: { baseURL: 'http://127.0.0.1:4173', trace: 'off' },
  webServer: {
    command:
      'rm -rf /tmp/millstream-e2e && ../.build/millstream --db /tmp/millstream-e2e/millstream.db run --listen 127.0.0.1:4173 e2e/fixture',
    url: 'http://127.0.0.1:4173/api/v1/pipelines',
    reuseExistingServer: false,
  },
});
