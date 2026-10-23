import { readFile } from 'node:fs/promises';
import { expect, test, type Page } from '@playwright/test';
import { login, noErrors, op, unique } from './common.js';

// The administration pages: address settings and diverts edited, the
// configuration exported, users and permissions, transactions, the
// dead-letter workbench, and a queue's messages acted on by filter,
// exported and imported.

noErrors(/status of 40[13]/);

const SEND =
	'sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)';

const createQueue = (page: Page, name: string) =>
	op(page, 'broker', 'createQueue(java.lang.String,boolean)', [
		JSON.stringify({ name, address: name, 'routing-type': 'ANYCAST' }),
		false
	]);

const deleteQueue = (page: Page, name: string) =>
	op(page, 'broker', 'destroyQueue(java.lang.String,boolean,boolean)', [name, true, true]);

const send = (page: Page, queue: string, body: string, headers: Record<string, string> = {}) =>
	op(page, `queue.${queue}`, SEND, [headers, 3, body, true, null, null, true]);

test('admin adds, reads and removes address settings', async ({ page }) => {
	const match = `${unique('e2e.settings')}.#`;
	await login(page);
	await page.goto('/saranga/#/n/local/configuration');
	await expect(page.getByRole('heading', { level: 1 })).toHaveText('Configuration');
	await page.getByRole('button', { name: 'Add address settings' }).click();
	const editor = page.getByRole('dialog');
	await editor.getByLabel('Match').fill(match);
	await editor.getByLabel('Setting to add').selectOption('maxSizeBytes');
	await editor.getByRole('button', { name: 'Add', exact: true }).click();
	await editor.getByLabel('maxSizeBytes', { exact: true }).fill('20971520');
	await editor.getByLabel('Setting to add').selectOption('addressFullMessagePolicy');
	await editor.getByRole('button', { name: 'Add', exact: true }).click();
	await editor.getByLabel('addressFullMessagePolicy', { exact: true }).selectOption('BLOCK');
	await editor.getByRole('button', { name: 'Save' }).click();
	await expect(page.getByText(`Saved the settings of ${match}`)).toBeVisible();

	const row = page.getByRole('row', { name: new RegExp(match.replaceAll('.', '\\.')) });
	await expect(row.getByText('management')).toBeVisible();
	await expect(row.getByText('addressFullMessagePolicy=BLOCK')).toBeVisible();

	// An address under the match: each value with the match it comes from.
	await page.getByRole('textbox', { name: 'Address', exact: true }).fill(match.replace('#', 'a'));
	await page.getByRole('button', { name: 'Show' }).click();
	const policy = page.getByRole('row', { name: /^addressFullMessagePolicy / });
	await expect(policy.getByRole('cell').nth(1)).toHaveText('BLOCK');
	await expect(policy.getByRole('cell').nth(2)).toHaveText(match);
	await expect(
		page
			.getByRole('row', { name: /^deadLetterAddress / })
			.getByRole('cell')
			.nth(2)
	).toHaveText('#');

	await row.getByRole('button', { name: 'Remove' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Remove' }).click();
	await expect(page.getByText(`Removed the settings of ${match}`)).toBeVisible();
	await expect(row).toHaveCount(0);
});

test('admin creates, changes and destroys a divert', async ({ page }) => {
	const name = unique('e2e.divert');
	await login(page);
	// A divert needs its address.
	await createQueue(page, `${name}.from`);
	await page.goto('/saranga/#/n/local/configuration?tab=diverts');
	await page.getByRole('button', { name: 'Create a divert' }).click();
	const dialog = page.getByRole('dialog');
	await dialog.getByLabel('Name', { exact: true }).fill(name);
	await dialog.getByLabel('Address', { exact: true }).fill(`${name}.from`);
	await dialog.getByRole('textbox', { name: 'Forwarding address' }).fill(`${name}.to`);
	await dialog.getByRole('button', { name: 'Create' }).click();
	await expect(page.getByText(`Created divert ${name}`)).toBeVisible();

	const row = page.getByRole('row', { name: new RegExp(`^${name.replaceAll('.', '\\.')} `) });
	await expect(row.getByRole('cell').nth(2)).toHaveText(`${name}.to`);
	await row.getByRole('button', { name: 'Edit' }).click();
	await dialog.getByRole('textbox', { name: 'Forwarding address' }).fill(`${name}.elsewhere`);
	await dialog.getByRole('button', { name: 'Save' }).click();
	await expect(page.getByText(`Changed divert ${name}`)).toBeVisible();
	await expect(row.getByRole('cell').nth(2)).toHaveText(`${name}.elsewhere`);

	await row.getByRole('button', { name: 'Destroy' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Destroy' }).click();
	await expect(page.getByText(`Destroyed divert ${name}`)).toBeVisible();
	await expect(row).toHaveCount(0);
	await deleteQueue(page, `${name}.from`);
});

test('admin exports the configuration as broker properties', async ({ page }) => {
	await login(page);
	await page.goto('/saranga/#/n/local/configuration?tab=export');
	await page.getByRole('button', { name: 'Export', exact: true }).click();
	await expect(page.getByText('Exported the configuration as broker properties')).toBeVisible();
	await expect(page.getByLabel('Exported configuration')).toContainText('addressSettings.');
	const download = page.waitForEvent('download');
	await page.getByRole('button', { name: 'Download' }).click();
	expect((await download).suggestedFilename()).toBe('broker_config_as_properties.txt');
});

test('admin sees the users and checks what one may do', async ({ page }) => {
	await login(page);
	await page.goto('/saranga/#/n/local/security');
	const users = page.getByRole('table');
	await expect(users.getByRole('row', { name: /^viewer view/ })).toBeVisible();

	await page.getByRole('tab', { name: 'Roles' }).click();
	await expect(page.getByRole('row', { name: /^# / })).toContainText('amq');

	await page.getByRole('tab', { name: 'Check permissions' }).click();
	await page.getByLabel('Address').fill('orders');
	await page.getByLabel('User (optional)').fill('viewer');
	await page.getByRole('button', { name: 'Check' }).click();
	await expect(page.getByText('The roles of # apply to orders.')).toBeVisible();
	await expect(page).toHaveURL(/tab=check&address=orders&user=viewer/);
	const sendRow = page.getByRole('row', { name: /^Send / });
	await expect(sendRow.getByRole('cell').nth(1)).toHaveText('No');
});

test('admin sees no prepared transaction', async ({ page }) => {
	await login(page);
	await page.goto('/saranga/#/n/local/transactions');
	await expect(page.getByText('No prepared transaction.')).toBeVisible();
	await expect(page.getByRole('heading', { name: 'Heuristically committed' })).toBeVisible();
});

test('admin retries dead letters by the queue they came from', async ({ page }) => {
	const q = unique('e2e.failing');
	await login(page);
	await createQueue(page, q);
	for (const body of ['a', 'b', 'c']) await send(page, q, body);
	expect(
		await op(page, `queue.${q}`, 'sendMessagesToDeadLetterAddress(java.lang.String)', [''])
	).toBe(3);

	await page.goto('/saranga/#/n/local/dead-letters?queue=DLQ');
	const group = page
		.getByRole('table', { name: 'Groups' })
		.getByRole('row', { name: new RegExp(q) });
	await expect(group.getByRole('cell').nth(1)).toHaveText('3');
	await page.getByLabel('Group by').selectOption('reason');
	await expect(page).toHaveURL(/by=reason/);
	await page.getByLabel('Group by').selectOption('queue');

	await group.getByRole('button', { name: 'Retry' }).click();
	await expect(page.getByText('Retried 3 messages')).toBeVisible();
	await expect(group).toHaveCount(0);
	expect(await op(page, `queue.${q}`, 'countMessages()')).toBe(3);
	await deleteQueue(page, q);
});

test('admin acts on every matching message, and exports and imports them', async ({
	page
}, info) => {
	const from = unique('e2e.bulk');
	const to = unique('e2e.imported');
	await login(page);
	await createQueue(page, from);
	await createQueue(page, to);
	for (const color of ['red', 'red', 'blue']) await send(page, from, color, { color });

	await page.goto(`/saranga/#/n/local/queues/${from}`);
	await page.getByLabel('Filter').fill("color = 'red'");
	await page.getByRole('button', { name: 'Filter' }).click();
	await expect(page.getByText('2 messages match')).toBeVisible();

	await page.getByRole('button', { name: 'Every match' }).click();
	await page.getByRole('menuitem', { name: 'Change the priority…' }).click();
	await page.getByLabel('Priority, 0 to 9').fill('8');
	await page.getByRole('button', { name: 'Change' }).click();
	await expect(page.getByText('Changed the priority of 2 messages')).toBeVisible();
	expect(
		await op(page, `queue.${from}`, 'countMessages(java.lang.String)', ['AMQPriority = 8'])
	).toBe(2);

	// Exported, then imported into another queue as they were.
	await page.getByRole('button', { name: 'Every match' }).click();
	const download = page.waitForEvent('download');
	await page.getByRole('menuitem', { name: 'Export as JSON' }).click();
	const file = info.outputPath('messages.json');
	await (await download).saveAs(file);
	const exported = JSON.parse(await readFile(file, 'utf8'));
	expect(exported.map((m: { text: string; priority: number }) => [m.text, m.priority])).toEqual([
		['red', 8],
		['red', 8]
	]);
	await page.goto(`/saranga/#/n/local/queues/${to}`);
	await page.getByLabel('Messages to import').setInputFiles(file);
	await expect(page.getByText('Imported 2 messages')).toBeVisible();
	await expect(
		page.getByRole('table', { name: 'Messages' }).getByRole('button', { name: /^\d+$/ })
	).toHaveCount(2);

	// What the filter selects, deleted.
	await page.goto(`/saranga/#/n/local/queues/${from}`);
	await page.getByLabel('Filter').fill("color = 'red'");
	await page.getByRole('button', { name: 'Filter' }).click();
	await page.getByRole('button', { name: 'Every match' }).click();
	await page.getByRole('menuitem', { name: 'Delete' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Delete' }).click();
	await expect(page.getByText('Deleted 2 messages')).toBeVisible();
	expect(await op(page, `queue.${from}`, 'countMessages()')).toBe(1);

	await deleteQueue(page, from);
	await deleteQueue(page, to);
});

test('viewer reads the administration pages but changes nothing', async ({ page }) => {
	await login(page, 'viewer', 'viewer');
	await page.goto('/saranga/#/n/local/configuration');
	await expect(page.getByRole('row', { name: /^# / })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Add address settings' })).toBeDisabled();
	await page.goto('/saranga/#/n/local/security');
	await expect(page.getByRole('button', { name: 'Add a user' })).toBeDisabled();
	await page.goto('/saranga/#/n/local/configuration?tab=export');
	await expect(page.getByRole('button', { name: 'Export', exact: true })).toBeDisabled();
});
