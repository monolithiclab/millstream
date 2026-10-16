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
  // The setup project seeds runs and reviews through the API once the server is up; the tests depend on it.
  projects: [
    { name: 'setup', testMatch: /global\.setup\.ts/ },
    // The reference demo runs on its own servers: playwright.demo.config.ts.
    { name: 'chromium', dependencies: ['setup'], testIgnore: /global\.setup\.ts|demo\.spec\.ts/ },
  ],
  webServer: {
    // USER names the reviewer of decisions made from the UI, as on a laptop (no authentication until M4).
    command:
      'rm -rf /tmp/millstream-e2e && USER=e2e ../.build/millstream --db /tmp/millstream-e2e/millstream.db run --listen 127.0.0.1:4173 e2e/fixture',
    url: 'http://127.0.0.1:4173/api/v1/pipelines',
    reuseExistingServer: false,
  },
});
