import { expect, test, type Page } from '@playwright/test';
import { login, noErrors } from './common.js';

// The console across a cluster (tests/interop/console/cluster.sh): node 1,
// node 2 and node 3 (Artemis where it is built), each with admin/admin, and
// the queues only.node2 and only.node3 that cluster.py made. Skipped against
// a single broker.

test.skip(!process.env.SARANGA_CLUSTER, 'needs the cluster of tests/interop/console/cluster.sh');

// A node that is not there answers 404 to the views that name it.
noErrors(/status of (401|404)/);

async function pick(page: Page, name: string) {
	await page.getByRole('button', { name: 'Node' }).click();
	await page.getByRole('menuitem', { name: new RegExp(`^${name}`) }).click();
}

test('the node picker lists the cluster, and a page follows the node picked', async ({ page }) => {
	await login(page, 'admin', 'admin');
	await page.getByRole('button', { name: 'Node' }).click();
	for (const n of ['node1', 'node2', 'node3', 'Every node']) {
		await expect(page.getByRole('menuitem', { name: new RegExp(`^${n}`) })).toBeVisible();
	}
	await page.keyboard.press('Escape');
	await pick(page, 'node3');
	await expect(page.getByRole('heading', { level: 1, name: 'node3' })).toBeVisible();
	await expect(page).toHaveURL(/#\/n\/[0-9a-f-]{36}$/);
	await pick(page, 'node2');
	await expect(page.getByRole('heading', { level: 1, name: 'node2' })).toBeVisible();
});

test("another node's queues, and a queue's page there", async ({ page }) => {
	await login(page, 'admin', 'admin');
	await pick(page, 'node3');
	await page.getByRole('link', { name: 'Queues' }).first().click();
	const table = page.getByRole('table', { name: 'Queues' });
	await expect(table.getByRole('link', { name: 'only.node3', exact: true }).first()).toBeVisible();
	await expect(table.getByRole('link', { name: 'only.node2', exact: true })).toHaveCount(0);
	await table.getByRole('link', { name: 'only.node3', exact: true }).first().click();
	await expect(page.getByRole('heading', { level: 1, name: 'only.node3' })).toBeVisible();
});

test('every node: the listings span them, each row on its node', async ({ page }) => {
	await login(page, 'admin', 'admin');
	await pick(page, 'Every node');
	// The overview of every node is the cluster's page.
	await expect(page.getByRole('heading', { level: 1, name: 'Nodes' })).toBeVisible();
	// Only the pages that span the nodes are offered.
	await expect(page.getByRole('link', { name: 'Explorer' })).toHaveCount(0);
	await page.getByRole('link', { name: 'Queues' }).first().click();
	const table = page.getByRole('table', { name: 'Queues' });
	await expect(table.getByRole('columnheader', { name: 'Node' })).toBeVisible();
	const row = table.getByRole('row').filter({ hasText: 'only.node2' });
	await expect(row.getByRole('link', { name: 'node2', exact: true })).toBeVisible();
	await row.getByRole('link', { name: 'only.node2', exact: true }).first().click();
	await expect(page.getByRole('heading', { level: 1, name: 'only.node2' })).toBeVisible();
	await expect(page.getByRole('button', { name: 'Node' })).toContainText('node2');
});

test("the cluster's page: each node's status, and the queues across them", async ({ page }) => {
	await login(page, 'admin', 'admin');
	await page.getByRole('link', { name: 'Nodes' }).click();
	const cards = page.getByRole('region', { name: 'Nodes' });
	for (const n of ['node1', 'node2', 'node3']) {
		await expect(cards.getByRole('link', { name: n })).toBeVisible();
	}
	await expect(cards.getByText('Live')).toHaveCount(3);
	const spread = page.getByRole('table').filter({ hasText: 'only.node2' });
	await expect(spread.getByRole('row').filter({ hasText: 'only.node2' })).toBeVisible();
	await expect(page.getByRole('heading', { name: /Cluster connections of/ })).toBeVisible();
	await expect(page.getByRole('cell', { name: 'my-cluster' })).toBeVisible();
});

test('high availability: each node with its policy, and the failover actions', async ({ page }) => {
	await login(page, 'admin', 'admin');
	await page.getByRole('link', { name: 'High availability' }).click();
	const table = page.getByRole('table').first();
	for (const n of ['node1', 'node2', 'node3']) {
		await expect(table.getByRole('link', { name: n })).toBeVisible();
	}
	await expect(table.getByRole('cell', { name: 'Primary Only' })).toHaveCount(3);
	// Nothing here replicates, so there is no replication to freeze.
	await page.getByRole('button', { name: 'Actions on node2' }).click();
	await expect(page.getByRole('menuitem', { name: 'Force a failover…' })).toBeEnabled();
	await expect(page.getByRole('menuitem', { name: 'Freeze replication…' })).toBeDisabled();
	await page.keyboard.press('Escape');
	await expect(page.getByText('No node has a lock coordinator.')).toBeVisible();
});

test("a node's bridges and connections, and those of another", async ({ page }) => {
	await login(page, 'admin', 'admin');
	await page.getByRole('link', { name: 'Bridges and connections' }).click();
	const clusters = page.getByRole('region', { name: 'Cluster connections' });
	await expect(clusters.getByRole('link', { name: 'my-cluster' })).toBeVisible();
	await expect(clusters.getByText('Started')).toBeVisible();
	await expect(page.getByText('No broker connection')).toBeVisible();
	await pick(page, 'node3');
	await expect(
		page
			.getByRole('region', { name: 'Cluster connections' })
			.getByRole('link', { name: 'my-cluster' })
	).toBeVisible();
});

test("another node's events are this broker's, and the page says so", async ({ page }) => {
	await login(page, 'admin', 'admin');
	await pick(page, 'node2');
	await page.getByRole('link', { name: 'Events' }).click();
	await expect(page.getByText(/These are the notifications of node1/)).toBeVisible();
});
