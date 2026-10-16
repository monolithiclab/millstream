import { defineConfig } from '@playwright/test';

// Runs in the pinned Playwright image (make test-ui-e2e) against the built UI in internal/ui/dist, served by the
// Go handler the binary ships (internal/ui/uiserve, built by make test-ui-e2e into .build/).
export default defineConfig({
  testDir: 'e2e',
  outputDir: 'test-results',
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  use: { baseURL: 'http://127.0.0.1:4173', trace: 'off' },
  webServer: {
    command: '../.build/uiserve -dir ../internal/ui/dist -addr 127.0.0.1:4173',
    url: 'http://127.0.0.1:4173/',
    reuseExistingServer: false,
  },
});
