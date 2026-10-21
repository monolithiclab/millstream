import { defineConfig } from '@playwright/test';

// make test-ui-e2e's run with auth.token set (e2e/auth.spec.ts): millstream run over the e2e fixture with the
// environment file e2e/auth/millstream.yaml, on a database and a port of its own. No USER: the reviewer is auth.user.
export const token = 'e2e-token-0123456789abcdef-012345';

export default defineConfig({
  testDir: 'e2e',
  testMatch: /auth\.spec\.ts/,
  outputDir: 'test-results/auth',
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  use: { baseURL: 'http://127.0.0.1:4176', trace: 'off' },
  webServer: {
    command:
      `rm -rf /tmp/millstream-e2e-auth && MILLSTREAM_TOKEN=${token} ../.build/millstream ` +
      '--env-file e2e/auth/millstream.yaml --db sqlite:///tmp/millstream-e2e-auth/millstream.db run --listen 127.0.0.1:4176 ' +
      'e2e/fixture',
    // The UI's index answers without the token.
    url: 'http://127.0.0.1:4176/',
    reuseExistingServer: false,
  },
});
