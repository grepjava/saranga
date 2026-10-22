import { expect, test, type Page } from '@playwright/test';
import { login, noErrors, unique } from './common.js';

// The pages that match Artemis's console: queues and addresses, created and
// deleted; messages sent, browsed, moved and deleted; the explorer, the
// diagram, the runtime and the preferences; and what a viewer may not do.

noErrors();

async function createQueue(page: Page, name: string) {
	await page.goto('/saranga/#/n/local/queues');
	await page.getByRole('button', { name: 'Create queue' }).click();
	const dialog = page.getByRole('dialog');
	await dialog.getByLabel('Name').fill(name);
	await dialog.getByRole('button', { name: 'Create' }).click();
	await expect(page.getByText(`Created queue ${name}`)).toBeVisible();
}

async function deleteQueue(page: Page, name: string) {
	await page.goto(`/saranga/#/n/local/queues/${encodeURIComponent(name)}`);
	await page.getByRole('button', { name: 'More actions' }).click();
	await page.getByRole('menuitem', { name: 'Delete' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Delete' }).click();
	await expect(page.getByText(`Deleted queue ${name}`)).toBeVisible();
	await expect(page).toHaveURL(/#\/n\/local\/queues$/);
}

test('admin sends, browses, moves and deletes messages', async ({ page }) => {
	const from = unique('e2e.from');
	const to = unique('e2e.to');
	await login(page);
	await createQueue(page, from);
	await createQueue(page, to);

	// A message with a property and a JSON body.
	await page.goto(`/saranga/#/n/local/queues/${from}`);
	await expect(page.getByRole('heading', { level: 1 })).toHaveText(from);
	await expect(page.getByText('The queue is empty.')).toBeVisible();
	await page.getByRole('button', { name: 'Send' }).click();
	const send = page.getByRole('dialog');
	await send.getByRole('button', { name: 'Add' }).click();
	await send.getByLabel('Property name').fill('color');
	await send.getByLabel('Property value').fill('red');
	await send.getByRole('textbox', { name: 'Body' }).fill('{"a":1}');
	await send.getByRole('button', { name: 'Format' }).click();
	await send.getByRole('button', { name: 'Send' }).click();
	await expect(page.getByText(/^Sent message \d+$/)).toBeVisible();

	// Browsed, and seen whole.
	const messages = page.getByRole('table', { name: 'Messages' });
	await expect(messages.getByRole('row')).toHaveCount(2);
	await messages.getByRole('button', { name: /^\d+$/ }).click();
	const sheet = page.getByRole('dialog');
	await expect(sheet.getByRole('heading', { name: /^Message \d+$/ })).toBeVisible();
	await expect(sheet.getByRole('cell', { name: 'color' })).toBeVisible();
	await expect(sheet.getByRole('cell', { name: 'red' })).toBeVisible();
	await expect(sheet.getByRole('textbox', { name: 'Body' })).toContainText('"a": 1');

	// Moved to the other queue.
	await sheet.getByRole('button', { name: 'Move' }).click();
	const picker = page.getByRole('dialog', { name: 'Move to a queue' });
	await picker.getByLabel('Queue').fill(to);
	await picker.getByRole('button', { name: 'Move' }).click();
	await expect(page.getByText('Moved 1 message')).toBeVisible();
	await expect(page.getByText('The queue is empty.')).toBeVisible();

	// Deleted there, chosen in the table.
	await page.goto(`/saranga/#/n/local/queues/${to}`);
	const moved = page.getByRole('table', { name: 'Messages' });
	await expect(moved.getByRole('button', { name: /^\d+$/ })).toHaveCount(1);
	await moved.getByRole('checkbox', { name: /^Select message/ }).check();
	await page.getByRole('button', { name: 'Delete', exact: true }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Delete' }).click();
	await expect(page.getByText('Deleted 1 message')).toBeVisible();
	await expect(page.getByText('The queue is empty.')).toBeVisible();

	await deleteQueue(page, from);
	await deleteQueue(page, to);
});

test('admin purges a queue', async ({ page }) => {
	const name = unique('e2e.purge');
	await login(page);
	await createQueue(page, name);
	await page.goto(`/saranga/#/n/local/queues/${name}`);
	for (let i = 0; i < 2; i++) {
		await page.getByRole('button', { name: 'Send' }).click();
		await page.getByRole('dialog').getByRole('textbox', { name: 'Body' }).fill(`m${i}`);
		await page.getByRole('dialog').getByRole('button', { name: 'Send' }).click();
		await expect(page.getByRole('dialog')).toHaveCount(0);
	}
	await expect(page.getByRole('table', { name: 'Messages' }).getByRole('row')).toHaveCount(3);
	await page.getByRole('button', { name: 'More actions' }).click();
	await page.getByRole('menuitem', { name: 'Purge' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Purge' }).click();
	await expect(page.getByText('Purged: 2 messages')).toBeVisible();
	await expect(page.getByText('The queue is empty.')).toBeVisible();
	await deleteQueue(page, name);
});

test('admin creates and deletes an address', async ({ page }) => {
	const name = unique('e2e.address');
	await login(page);
	await page.goto('/saranga/#/n/local/addresses');
	await page.getByRole('button', { name: 'Create address' }).click();
	const dialog = page.getByRole('dialog');
	await dialog.getByLabel('Name').fill(name);
	await dialog.getByText(/Multicast/).click();
	await dialog.getByRole('button', { name: 'Create' }).click();
	await expect(page.getByText(`Created address ${name}`)).toBeVisible();

	await page.goto(`/saranga/#/n/local/addresses?filter=name:EQUALS:${name}`);
	const table = page.getByRole('table', { name: 'Addresses' });
	await expect(table.getByRole('row')).toHaveCount(2);
	await expect(table).toContainText('MULTICAST');
	await table.getByRole('row').nth(1).getByRole('checkbox').check();
	await page.getByRole('button', { name: 'Delete the address' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Delete' }).click();
	await expect(page.getByText('Deleted the address')).toBeVisible();
	await expect(table.getByRole('row')).toHaveCount(2);
	await expect(table).toContainText('Nothing matches the filters.');
});

test('the explorer reads, charts and calls a resource', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Explorer' }).click();
	const tree = page.getByRole('navigation', { name: 'Resources' });
	await tree.getByLabel('Find a resource').fill('orders.eu');
	await tree.getByRole('link', { name: 'orders.eu' }).first().click();
	await expect(page.getByRole('heading', { name: 'address.orders.eu' })).toBeVisible();

	await page.goto('/saranga/#/n/local/explorer?r=queue.orders.eu');
	await expect(page.getByRole('heading', { name: 'queue.orders.eu' })).toBeVisible();
	await expect(page.getByRole('cell', { name: 'MessageCount', exact: true })).toBeVisible();
	await page.getByRole('button', { name: 'Chart MessageCount', exact: true }).click();
	await expect(page.getByText('Read every two seconds')).toBeVisible();

	await page.getByRole('tab', { name: /Operations/ }).click();
	await page.getByLabel('Find an operation').fill('countMessages');
	await page.getByRole('button', { name: /^countMessages\(\)/ }).click();
	await page.getByRole('button', { name: 'Run' }).click();
	await expect(page.getByRole('tabpanel').getByRole('status')).toHaveText(/^\d+$/);
});

test('the diagram draws the broker, its addresses and its queues', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Diagram' }).click();
	const diagram = page.getByLabel("The broker's topology");
	await expect(diagram.getByText('orders.eu', { exact: true })).toBeVisible();
	await diagram.getByText(/invoices · /).click();
	await expect(page).toHaveURL(/#\/n\/local\/queues\/invoices$/);
});

test('the runtime shows the process', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Runtime' }).click();
	await expect(page.getByRole('region', { name: 'The process' })).toContainText('Processors');
	await page.getByRole('tab', { name: 'System properties' }).click();
	await page.getByLabel('Find a property').fill('artemis.instance');
	await expect(page.getByRole('cell', { name: 'artemis.instance', exact: true })).toBeVisible();
});

test('the preferences are kept', async ({ page }) => {
	await login(page);
	await page.goto('/saranga/#/n/local/preferences');
	await page.getByLabel('Bytes').selectOption('decimal');
	await page.getByLabel('Dead-letter queues').fill('^(DLQ|invoices)$');
	await page.getByLabel('Dead-letter queues').blur();
	await page.reload();
	await expect(page.getByLabel('Bytes')).toHaveValue('decimal');
	await expect(page.getByLabel('Dead-letter queues')).toHaveValue('^(DLQ|invoices)$');
	// A queue the pattern matches holds dead letters, which can be retried.
	await page.goto('/saranga/#/n/local/queues/invoices');
	await expect(page.getByText('dead letters', { exact: true })).toBeVisible();
	await page.goto('/saranga/#/n/local/preferences');
	await page.getByRole('button', { name: 'Reset all' }).click();
	await expect(page.getByLabel('Dead-letter queues')).toHaveValue('^DLQ$');
});

test('a viewer browses but changes nothing', async ({ page }) => {
	await login(page, 'viewer', 'viewer');
	await page.goto('/saranga/#/n/local/queues/orders.us');
	await expect(page.getByRole('heading', { level: 1 })).toHaveText('orders.us');
	const messages = page.getByRole('table', { name: 'Messages' });
	await expect(messages.getByRole('row').nth(1)).toBeVisible();
	await expect(page.getByRole('button', { name: 'Send' })).toHaveCount(0);
	await messages
		.getByRole('checkbox', { name: /^Select message/ })
		.first()
		.check();
	await expect(page.getByText('1 selected')).toBeVisible();
	await expect(page.getByRole('button', { name: 'Delete', exact: true })).toHaveCount(0);
	await expect(page.getByRole('button', { name: 'Move', exact: true })).toHaveCount(0);
	await page.getByRole('button', { name: 'More actions' }).click();
	await expect(page.getByRole('menuitem', { name: 'Purge' })).toBeDisabled();
	await page.keyboard.press('Escape');

	await page.goto('/saranga/#/n/local/queues');
	await expect(page.getByRole('table', { name: 'Queues' }).getByRole('row').nth(1)).toBeVisible();
	await expect(page.getByRole('button', { name: 'Create queue' })).toHaveCount(0);

	await page.goto('/saranga/#/n/local/explorer?r=broker');
	await page.getByRole('tab', { name: /Operations/ }).click();
	await page.getByLabel('Find an operation').fill('createAddress');
	await expect(
		page.getByRole('button', { name: /createAddress\(String, String\).*Not permitted/ })
	).toBeVisible();
});
