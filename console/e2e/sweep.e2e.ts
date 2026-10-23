import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import { login, noErrors, unique } from './common.js';

// Every page, as each role: admin, a viewer (the view role, which may only
// read) and a user without a console role; each tab of a page checked by axe
// for WCAG 2.1 A and AA, and each page at a phone's width. Against one broker
// (tests/interop/console/api.sh) the pages of this node; against the cluster
// of tests/interop/console/cluster.sh those of node 1, of node 3 (Artemis
// where it is built) and of every node.

const CLUSTER = !!process.env.SARANGA_CLUSTER;
const ADMIN = CLUSTER ? 'admin' : 'secret';

// The viewer is refused what it may not do, which the browser logs.
noErrors(/status of 40[134]/);

/** A node's pages, by their path under /n/{node}/. */
const NODE_PAGES = [
	'',
	'connections',
	'sessions',
	'consumers',
	'producers',
	'addresses',
	'queues',
	'addresses/DLQ',
	'queues/DLQ',
	'diagram',
	'events',
	'cluster',
	'ha',
	'federation',
	'dead-letters',
	'configuration',
	'security',
	'transactions',
	'explorer',
	'explorer?r=broker',
	'runtime',
	'preferences'
];

/** The pages that show every node at once. */
const EVERY_NODE_PAGES = [
	'',
	'connections',
	'sessions',
	'consumers',
	'producers',
	'addresses',
	'queues',
	'cluster',
	'ha'
];

/** The scopes the pages are seen in: a node's ID is read from /nodes. */
const SCOPES: { name: string; pages: string[] }[] = CLUSTER
	? [
			{ name: 'local', pages: NODE_PAGES },
			{ name: 'node3', pages: NODE_PAGES },
			{ name: 'all', pages: EVERY_NODE_PAGES }
		]
	: [{ name: 'local', pages: NODE_PAGES }];

/** What a viewer must not be offered: a control that changes the broker. */
const CHANGES =
	/^(add|create|delete|remove|purge|move|copy|retry|resend|send|close|kill|destroy|pause|resume|start|stop|reload|import|commit|roll ?back|force|freeze|edit|change|save|apply|clear|expire|reset|enable|disable)\b/i;

async function scope(page: Page, name: string): Promise<string> {
	if (name !== 'node3') return name;
	// Its ID, from the node picker, which names each node as it answers.
	await page.getByRole('button', { name: 'Node' }).click();
	await page.getByRole('menuitem', { name: /^node3/ }).click();
	await expect(page).toHaveURL(/#\/n\/[0-9a-f-]{36}$/);
	return page.url().split('/n/')[1];
}

/** Waits for the page to show what it read. */
async function settled(page: Page) {
	await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
	await expect(page.locator('[data-slot=skeleton]:visible')).toHaveCount(0, { timeout: 15_000 });
	await page.waitForTimeout(300);
}

async function axe(page: Page, where: string) {
	const { violations } = await new AxeBuilder({ page })
		.withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
		.analyze();
	expect
		.soft(
			violations.map((v) => ({
				where,
				id: v.id,
				help: v.help,
				nodes: v.nodes.slice(0, 3).map((n) => n.target.join(' '))
			}))
		)
		.toEqual([]);
}

/** Each tab of the page in turn, the page itself where it has none. */
async function eachTab(page: Page, check: (tab: string) => Promise<void>) {
	const tabs = page.locator('main [role=tablist]').first().getByRole('tab');
	const n = await tabs.count();
	if (n === 0) return check('');
	for (let i = 0; i < n; i++) {
		const tab = tabs.nth(i);
		const name = (await tab.textContent())?.trim() ?? `${i}`;
		await tab.click();
		await settled(page);
		await check(name);
	}
}

/** The enabled controls of the page that change the broker. */
async function changes(page: Page): Promise<string[]> {
	const names = await page
		.locator('main')
		.evaluate((main) =>
			[...main.querySelectorAll<HTMLElement>('button, [role=button], [role=menuitem], a[href]')]
				.filter(
					(c) =>
						c.checkVisibility() && !c.matches(':disabled, [aria-disabled=true], [data-disabled]')
				)
				.map((c) => (c.getAttribute('aria-label') ?? c.innerText).trim())
		);
	return names.filter((n) => CHANGES.test(n));
}

for (const { name, pages } of SCOPES) {
	test.describe(`${name}:`, () => {
		for (const path of pages) {
			const title = `/${path}`;

			test(`admin sees ${title}, and it is accessible`, async ({ page }) => {
				await login(page, 'admin', ADMIN);
				await page.goto(`/saranga/#/n/${await scope(page, name)}/${path}`);
				await settled(page);
				await eachTab(page, async (tab) => {
					await expect
						.soft(page.locator('main [data-slot=alert].text-destructive'), tab)
						.toHaveCount(0);
					await axe(page, `${title} ${tab}`);
				});
			});

			test(`a viewer sees ${title} and is offered no change`, async ({ page }) => {
				await login(page, 'viewer', 'viewer');
				await page.goto(`/saranga/#/n/${await scope(page, name)}/${path}`);
				await settled(page);
				await eachTab(page, async (tab) => {
					// The preferences are this browser's, which anyone may change.
					// Another node decides only when asked, so its actions are
					// offered (and refused, below).
					if (path !== 'preferences' && name !== 'node3')
						expect.soft(await changes(page), `${title} ${tab}`).toEqual([]);
					await axe(page, `${title} ${tab}`);
				});
			});

			test(`${title} fits a phone`, async ({ page }) => {
				await login(page, 'admin', ADMIN);
				// The node picker is in the sidebar, which a phone shows on demand.
				const node = await scope(page, name);
				await page.setViewportSize({ width: 390, height: 844 });
				await page.goto(`/saranga/#/n/${node}/${path}`);
				await settled(page);
				await eachTab(page, async (tab) => {
					const overflow = await page.evaluate(
						() => document.documentElement.scrollWidth - document.documentElement.clientWidth
					);
					expect.soft(overflow, `${title} ${tab} scrolls sideways`).toBeLessThanOrEqual(0);
				});
			});
		}

		if (name === 'node3')
			test("a viewer's change is refused by the node, and the page says so", async ({ page }) => {
				await login(page, 'viewer', 'viewer');
				await page.goto(`/saranga/#/n/${await scope(page, name)}/addresses`);
				await page.getByRole('button', { name: 'Create address' }).click();
				const dialog = page.getByRole('dialog');
				await dialog.getByLabel('Name').fill(unique('e2e.refused'));
				await dialog.getByText(/Multicast/).click();
				await dialog.getByRole('button', { name: 'Create' }).click();
				await expect(dialog.getByRole('alert')).toContainText(
					/AMQ229032|not authorized|permission/i
				);
			});

		test('a user without a console role sees no page', async ({ page }) => {
			await page.goto('/saranga/');
			await page.getByLabel('User').fill('outsider');
			await page.getByLabel('Password').fill('outsider');
			await page.getByRole('button', { name: 'Log in' }).click();
			await expect(page.getByRole('alert')).toBeVisible();
			const node = name === 'node3' ? 'all' : name;
			for (const path of pages) {
				await page.goto(`/saranga/#/n/${node}/${path}`);
				await expect(page.getByRole('button', { name: 'Log in' })).toBeVisible();
				await expect(page.locator('[data-slot=sidebar]')).toHaveCount(0);
			}
			const r = await page.request.get('/saranga/api/v1/me', {
				headers: { 'X-Saranga-Console': '1' }
			});
			expect(r.status()).toBe(401);
		});
	});
}

test('the login page is accessible, and fits a phone', async ({ page }) => {
	await page.goto('/saranga/');
	await expect(page.getByRole('button', { name: 'Log in' })).toBeVisible();
	await axe(page, 'login');
	await page.setViewportSize({ width: 390, height: 844 });
	const overflow = await page.evaluate(
		() => document.documentElement.scrollWidth - document.documentElement.clientWidth
	);
	expect(overflow).toBeLessThanOrEqual(0);
});
