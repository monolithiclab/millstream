import { resolve } from 'node:path';
import { defineConfig } from '@playwright/test';

// The M3 reference demo (make test-ui-demo), in the pinned Playwright image: millstream run serves the demo package
// make assembles in .build/demo (ui/e2e/demo-fixture), its agent replaying the M2 fixture's recordings offline. One
// server per language, each on a database of its own, as the demo starts from an empty pipeline and its dedupe step
// remembers the listings it has seen.
const root = resolve('..');

const demos = [
  { language: 'en', theme: 'light', port: 4174 },
  { language: 'fr', theme: 'dark', port: 4175 },
] as const;

const server = (port: number) => ({
  // USER names the reviewer of decisions made from the UI, as on a laptop (a server without auth.token).
  command: [
    `rm -rf /tmp/millstream-demo-${port} &&`,
    'USER=analyst MILLSTREAM_HARNESS_MODE=replay',
    `MILLSTREAM_HARNESS_DIR=${root}/.build/demo-recordings MILLSTREAM_SECRET_SCORER_KEY=sk-fake-0123456789`,
    `${root}/.build/millstream --db /tmp/millstream-demo-${port}/millstream.db`,
    `--env-file ${root}/ui/e2e/demo-fixture/millstream.yaml`,
    `run --listen 127.0.0.1:${port} ${root}/.build/demo`,
  ].join(' '),
  url: `http://127.0.0.1:${port}/api/v1/pipelines`,
  reuseExistingServer: false,
});

export default defineConfig({
  testDir: 'e2e',
  testMatch: /demo\.spec\.ts/,
  outputDir: 'test-results/demo',
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  timeout: 120_000,
  use: { trace: 'off' },
  // Both themes across the two runs: English in light, French in dark.
  projects: demos.map(({ language, theme, port }) => ({
    name: `${language} · ${theme}`,
    use: { baseURL: `http://127.0.0.1:${port}`, colorScheme: theme },
    metadata: { language },
  })),
  webServer: demos.map(({ port }) => server(port)),
});
