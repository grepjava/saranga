import { defineConfig } from 'vitest/config';
import { playwright } from '@vitest/browser-playwright';
import tailwindcss from '@tailwindcss/vite';
import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';

// `npm run dev` serves the pages and sends the API to a running broker.
const broker = process.env.SARANGA_URL ?? 'http://localhost:8161';

export default defineConfig({
	plugins: [
		tailwindcss(),
		sveltekit({
			compilerOptions: {
				// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true
			},
			// The broker embeds `build/` and serves it at /saranga/. The routes
			// are in the fragment, so every page is index.html.
			adapter: adapter({ strict: true }),
			paths: { base: '/saranga' },
			router: { type: 'hash' },
			// The pages are embedded in the broker: a new build is a new broker.
			version: { name: process.env.npm_package_version, pollInterval: 0 }
		})
	],
	server: {
		proxy: { '/saranga/api': { target: broker, changeOrigin: true } }
	},
	test: {
		expect: { requireAssertions: true },
		projects: [
			{
				extends: './vite.config.ts',
				test: {
					name: 'client',
					// Vitest's default port, 63315, is in a range Windows often reserves.
					api: { port: 63480 },
					browser: {
						enabled: true,
						provider: playwright(),
						instances: [{ browser: 'chromium', headless: true }]
					},
					include: ['src/**/*.svelte.{test,spec}.{js,ts}']
				}
			},
			{
				extends: './vite.config.ts',
				test: {
					name: 'server',
					environment: 'node',
					include: ['src/**/*.{test,spec}.{js,ts}'],
					exclude: ['src/**/*.svelte.{test,spec}.{js,ts}']
				}
			}
		]
	}
});
