import { expect, test } from '@playwright/test';
import { login, noErrors } from './common.js';

// The shell against a running broker: logging in and out, the layout, the
// tables, the command palette and the theme, with no error in the browser
// (a Content-Security-Policy violation among them).

noErrors();

test('a wrong password is refused', async ({ page }) => {
	await page.goto('/saranga/');
	await page.getByLabel('User').fill('admin');
	await page.getByLabel('Password').fill('wrong');
	await page.getByRole('button', { name: 'Log in' }).click();
	await expect(page.getByText('The user name or the password is wrong.')).toBeVisible();
});

test('a user without a console role is refused', async ({ page }) => {
	await page.goto('/saranga/');
	await page.getByLabel('User').fill('outsider');
	await page.getByLabel('Password').fill('outsider');
	await page.getByRole('button', { name: 'Log in' }).click();
	await expect(page.getByRole('alert')).toBeVisible();
	await expect(page.getByRole('button', { name: 'Log in' })).toBeVisible();
});

test('the overview shows the broker', async ({ page }) => {
	await login(page);
	await expect(page).toHaveURL(/#\/n\/local$/);
	await expect(page.getByRole('region', { name: 'Counters' })).toContainText('Connections');
	await expect(page.getByText('Address memory')).toBeVisible();
	await expect(page.getByRole('button', { name: 'Node' })).toBeVisible();
	// The acceptors and the cluster, as Artemis's status page shows them.
	await expect(page.getByRole('link', { name: 'artemis', exact: true })).toBeVisible();
	await expect(page.getByText('No cluster connection.')).toBeVisible();
});

test('the queues table pages, sorts and filters on the broker', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Queues' }).click();
	await expect(page).toHaveURL(/#\/n\/local\/queues$/);
	const table = page.getByRole('table', { name: 'Queues' });
	await expect(table.getByRole('row').nth(1)).toBeVisible();

	// Sorting is the broker's, and the URL keeps it.
	await table.getByRole('button', { name: 'Message count' }).click();
	await expect(page).toHaveURL(/sort=messageCount/);
	await table.getByRole('button', { name: 'Message count' }).click();
	await expect(page).toHaveURL(/order=desc/);

	// A filter.
	await page.getByLabel('Field').selectOption('name');
	await page.getByLabel('Operation').selectOption('EQUALS');
	await page.getByLabel('Value').fill('orders.eu');
	await page.getByRole('button', { name: 'Filter' }).click();
	await expect(page).toHaveURL(/filter=name%3AEQUALS%3Aorders\.eu|filter=name:EQUALS:orders\.eu/);
	await expect(table.getByRole('row')).toHaveCount(2);
	await expect(table).toContainText('orders.eu');

	// The URL alone gives the same page.
	await page.reload();
	await expect(page.getByRole('table', { name: 'Queues' }).getByRole('row')).toHaveCount(2);

	// The address leads to its page.
	await page
		.getByRole('table', { name: 'Queues' })
		.getByRole('link', { name: 'orders.eu' })
		.nth(1)
		.click();
	await expect(page).toHaveURL(/#\/n\/local\/addresses\/orders\.eu$/);
	await expect(page.getByRole('heading', { level: 1 })).toHaveText('orders.eu');
});

test('the columns can be chosen and are remembered', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Queues' }).click();
	const table = page.getByRole('table', { name: 'Queues' });
	await expect(table.getByRole('columnheader', { name: 'Ring size' })).toHaveCount(0);
	await page.getByRole('button', { name: 'Columns' }).click();
	await page.getByRole('menuitemcheckbox', { name: 'Ring size' }).click();
	await page.keyboard.press('Escape');
	await expect(table.getByRole('columnheader', { name: 'Ring size' })).toBeVisible();
	await page.reload();
	await expect(
		page.getByRole('table', { name: 'Queues' }).getByRole('columnheader', { name: 'Ring size' })
	).toBeVisible();
	await page.getByRole('button', { name: 'Columns' }).click();
	await page.getByRole('menuitem', { name: 'Default columns' }).click();
	await expect(table.getByRole('columnheader', { name: 'Ring size' })).toHaveCount(0);
});

test('the command palette goes to pages and queues', async ({ page }) => {
	await login(page);
	await page.keyboard.press('Control+k');
	const palette = page.getByRole('dialog');
	await palette.getByRole('combobox').fill('connect');
	await palette.getByRole('option', { name: 'Connections' }).click();
	await expect(page).toHaveURL(/#\/n\/local\/connections$/);

	await page.keyboard.press('Control+k');
	await page.getByRole('dialog').getByRole('combobox').fill('invoices');
	await page
		.getByRole('dialog')
		.getByRole('option', { name: /invoices.*Queue/ })
		.click();
	await expect(page).toHaveURL(/#\/n\/local\/queues\/invoices$/);
	await expect(page.getByRole('heading', { level: 1 })).toHaveText('invoices');
});

test('the theme follows the choice', async ({ page }) => {
	await login(page);
	await page.getByRole('button', { name: 'Theme' }).click();
	await page.getByRole('menuitemradio', { name: 'Dark' }).click();
	await expect(page.locator('html')).toHaveClass(/dark/);
	await page.getByRole('button', { name: 'Theme' }).click();
	await page.getByRole('menuitemradio', { name: 'Light' }).click();
	await expect(page.locator('html')).not.toHaveClass(/dark/);
});

test('a viewer sees no close actions', async ({ page }) => {
	await login(page, 'viewer', 'viewer');
	await page.getByRole('link', { name: 'Connections' }).click();
	const table = page.getByRole('table', { name: 'Connections' });
	await expect(table.getByRole('row').nth(1)).toBeVisible();
	await expect(table.getByRole('checkbox')).toHaveCount(0);
});

test('admin closes a connection, once confirmed', async ({ page }) => {
	await login(page);
	await page.getByRole('link', { name: 'Connections' }).click();
	const table = page.getByRole('table', { name: 'Connections' });
	const id = (await table.getByRole('row').nth(1).getByRole('cell').nth(1).textContent())!.trim();
	await page.goto(`/saranga/#/n/local/connections?filter=connectionID:EQUALS:${id}`);
	await expect(table.getByRole('row')).toHaveCount(2);
	await table.getByRole('checkbox', { name: 'Select the row' }).check();
	await page.getByRole('button', { name: 'Close the connection' }).click();

	// Cancelling leaves it.
	await page.getByRole('alertdialog').getByRole('button', { name: 'Cancel' }).click();
	await expect(table.getByRole('row')).toHaveCount(2);

	await page.getByRole('button', { name: 'Close the connection' }).click();
	await page.getByRole('alertdialog').getByRole('button', { name: 'Close' }).click();
	await expect(page.getByText('Closed the connection')).toBeVisible();
	await expect(table).toContainText('Nothing matches the filters.');
});

test('logging out ends the session', async ({ page }) => {
	await login(page);
	await page.getByRole('button', { name: 'User' }).click();
	await page.getByRole('menuitem', { name: 'Log out' }).click();
	await expect(page.getByRole('button', { name: 'Log in' })).toBeVisible();
	await page.reload();
	await expect(page.getByRole('button', { name: 'Log in' })).toBeVisible();
});
