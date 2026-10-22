import { expect, test, type Page } from '@playwright/test';

/**
 * Fails each test of the file that leaves an error in the browser (a
 * Content-Security-Policy violation among them). A request the broker
 * refuses is logged by the browser too, which `allowed` lets through.
 */
export function noErrors(allowed: RegExp = /status of 401/) {
	const errors: string[] = [];
	test.beforeEach(({ page }) => {
		errors.length = 0;
		page.on('pageerror', (e) => errors.push(e.message));
		page.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
	});
	test.afterEach(() => {
		expect(errors.filter((e) => !allowed.test(e))).toEqual([]);
	});
}

export async function login(page: Page, user = 'admin', password = 'secret') {
	await page.goto('/saranga/');
	await page.getByLabel('User').fill(user);
	await page.getByLabel('Password').fill(password);
	await page.getByRole('button', { name: 'Log in' }).click();
	await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
}

/** A name no other run has used. */
export function unique(prefix: string): string {
	return `${prefix}.${Date.now().toString(36)}${Math.floor(Math.random() * 1e4)}`;
}
