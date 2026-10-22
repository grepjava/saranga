import { expect, test, type Page } from '@playwright/test';
import { login, noErrors, unique } from './common.js';

// The live data: the pages follow the broker without being reloaded, the
// charts draw, and the events page shows the notifications as they come.

noErrors();

const API = '/saranga/api/v1';

/** An operation, as the logged-in user. */
async function op(page: Page, resource: string, operation: string, args: unknown[]) {
	const r = await page.request.post(`${API}/resources/${encodeURIComponent(resource)}/operations`, {
		headers: { 'X-Saranga-Console': '1' },
		data: { operation, arguments: args }
	});
	expect(r.status(), await r.text()).toBe(200);
	return (await r.json()).result;
}

const createQueue = (page: Page, name: string) =>
	op(page, 'broker', 'createQueue(java.lang.String,boolean)', [
		JSON.stringify({ name, address: name, 'routing-type': 'ANYCAST' }),
		false
	]);

const deleteQueue = (page: Page, name: string) =>
	op(page, 'broker', 'destroyQueue(java.lang.String,boolean,boolean)', [name, true, true]);

const send = (page: Page, address: string) =>
	op(
		page,
		`address.${address}`,
		'sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)',
		[{}, 3, 'live', true, null, null, true]
	);

test('the overview follows the broker and charts it', async ({ page }) => {
	await login(page);
	await expect(page.getByRole('status', { name: 'Live' })).toBeVisible();
	const charts = page.getByRole('region', { name: 'Charts' });
	await expect(charts.locator('canvas')).toHaveCount(4);

	const counters = page.getByRole('region', { name: 'Counters' });
	const messages = counters.getByRole('link', { name: /^[\d,.\s]+$/ }).nth(2);
	await expect(messages).toBeVisible();
	const before = await messages.textContent();
	const name = unique('e2e.live');
	await createQueue(page, name);
	await send(page, name);
	await expect(messages).not.toHaveText(before ?? '');
	await deleteQueue(page, name);
});

test('a listing follows the broker while live', async ({ page }) => {
	const name = unique('e2e.table');
	await login(page);
	await createQueue(page, name);
	await page.goto(`/saranga/#/n/local/queues?filter=name:EQUALS:${name}`);
	const table = page.getByRole('table', { name: 'Queues' });
	await expect(table.getByRole('row')).toHaveCount(2);
	const row = table.getByRole('row').nth(1);
	await expect(page.getByRole('switch', { name: 'Live' })).toBeChecked();
	const count = row.getByRole('cell').nth(5);
	await expect(count).toHaveText('0');

	await send(page, name);
	await expect(count).toHaveText('1');

	// Not live, it waits to be refreshed.
	await page.getByRole('switch', { name: 'Live' }).click();
	await send(page, name);
	await page.waitForTimeout(3000);
	await expect(count).toHaveText('1');
	await page.getByRole('button', { name: 'Refresh' }).click();
	await expect(count).toHaveText('2');
	await page.getByRole('switch', { name: 'Live' }).click();
	await deleteQueue(page, name);
});

test('a queue page counts and charts live', async ({ page }) => {
	const name = unique('e2e.charted');
	await login(page);
	await createQueue(page, name);
	await page.goto(`/saranga/#/n/local/queues/${name}`);
	await expect(page.getByRole('region', { name: 'Charts' }).locator('canvas')).toHaveCount(3);
	const messages = page.getByRole('region', { name: 'Counters' }).locator('span').first();
	await expect(messages).toHaveText('0');
	await send(page, name);
	await expect(messages).toHaveText('1');
	await deleteQueue(page, name);
});

test('the events page shows notifications as they come', async ({ page }) => {
	const name = unique('e2e.notified');
	await login(page);
	await page.getByRole('link', { name: 'Events', exact: true }).click();
	await expect(page.getByRole('heading', { level: 1 })).toHaveText('Events');
	const table = page.getByRole('table', { name: 'Events' });

	await createQueue(page, name);
	const added = table
		.getByRole('row')
		.filter({ hasText: 'BINDING_ADDED' })
		.filter({ hasText: name });
	await expect(added).toHaveCount(1);

	// By its type, from the URL.
	await page.getByLabel('Type').selectOption('BINDING_ADDED');
	await expect(page).toHaveURL(/type=BINDING_ADDED/);
	await expect(table.getByRole('row').filter({ hasText: 'SESSION_CREATED' })).toHaveCount(0);

	// Its properties, with a link to the queue.
	await added.click();
	await table.getByRole('link', { name, exact: true }).last().click();
	await expect(page.getByRole('heading', { level: 1 })).toHaveText(name);
	await deleteQueue(page, name);

	// Seen there, they are not new; one that comes elsewhere is.
	await page.goto('/saranga/#/n/local');
	const bell = page.getByRole('link', { name: /^Notifications/ });
	await expect(bell).toHaveAccessibleName('Notifications');
	await createQueue(page, `${name}.2`);
	await expect(bell).toHaveAccessibleName(/^Notifications, \d+ new$/);
	await deleteQueue(page, `${name}.2`);
});

test('a viewer sees no notifications', async ({ page }) => {
	await login(page, 'viewer', 'viewer');
	await expect(page.getByRole('status', { name: 'Live' })).toBeVisible();
	await expect(page.getByRole('link', { name: /^Notifications/ })).toHaveCount(0);
	await page.goto('/saranga/#/n/local/events');
	await expect(page.getByText('Your role may not see the notifications')).toBeVisible();
});
