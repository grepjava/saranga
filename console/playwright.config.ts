import { defineConfig, devices } from '@playwright/test';

// The end-to-end tests run against a running broker with the console built
// in: SARANGA_URL (http://localhost:8161 by default), with the console
// suite's users (tests/interop/console/api.sh sets them up).
export default defineConfig({
	testDir: 'e2e',
	testMatch: '**/*.e2e.ts',
	fullyParallel: false,
	// The tests share one broker.
	workers: 1,
	retries: 0,
	reporter: [['list']],
	use: {
		baseURL: process.env.SARANGA_URL ?? 'http://localhost:8161',
		trace: 'retain-on-failure',
		screenshot: 'only-on-failure'
	},
	projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }]
});
