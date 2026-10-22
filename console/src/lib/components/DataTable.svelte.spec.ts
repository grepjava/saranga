import { page } from 'vitest/browser';
import { describe, expect, it, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import { SvelteSet } from 'svelte/reactivity';
import DataTable, { VIRTUAL_FROM, type Column, type Row } from './DataTable.svelte';
import type { ListQuery } from '#lib/query.js';

const columns: Column[] = [
	{ key: 'name', label: 'Name', sortable: true },
	{ key: 'messageCount', label: 'Message count', sortable: true },
	{ key: 'durable', label: 'Durable', sortable: false }
];

function queues(n: number): Row[] {
	return Array.from({ length: n }, (_, i) => ({
		name: `q${i}`,
		messageCount: String(i * 1000),
		durable: i % 2 ? 'true' : 'false'
	}));
}

const query: ListQuery = { filters: [], order: 'asc', page: 1, size: 100 };

function props(rows: Row[], extra: Record<string, unknown> = {}) {
	return {
		label: 'Queues',
		columns,
		allColumns: columns,
		onColumns: () => {},
		rows,
		rowKey: (r: Row) => String(r.name),
		count: rows.length,
		query,
		onquery: vi.fn(),
		fields: [{ name: 'name', label: 'Name' }],
		exportName: 'queues',
		...extra
	};
}

describe('DataTable', () => {
	it('shows numbers and flags as such', async () => {
		render(DataTable, props(queues(3)));
		const table = page.getByRole('table', { name: 'Queues' });
		// toHaveTextContent takes no RegExp in Vitest 5's browser mode.
		await expect.element(table.getByRole('cell', { name: 'q2', exact: true })).toBeInTheDocument();
		const row = (name: string) =>
			table.getByRole('cell', { name, exact: true }).element().closest('tr')!.textContent;
		expect(row('q2')).toMatch(/q2\s*2,000\s*No/);
		expect(row('q1')).toMatch(/Yes/);
		await expect.element(page.getByText('1–3 of 3')).toBeInTheDocument();
	});

	it('asks the broker to sort, and then to sort the other way', async () => {
		const p = props(queues(3));
		render(DataTable, p);
		await page.getByRole('button', { name: 'Message count' }).click();
		expect(p.onquery).toHaveBeenLastCalledWith({
			...query,
			sort: 'messageCount',
			order: 'asc'
		});
		expect(p.onquery).toHaveBeenCalledTimes(1);
	});

	it('renders only the rows in view of a long page', async () => {
		const n = 5000;
		render(DataTable, props(queues(n), { count: n }));
		const table = page.getByRole('table', { name: 'Queues' });
		await expect.element(table.getByRole('cell', { name: 'q0', exact: true })).toBeInTheDocument();
		const rendered = table.getByRole('row').all().length;
		expect(n).toBeGreaterThan(VIRTUAL_FROM);
		expect(rendered).toBeLessThan(100);
	});

	it('selects rows and offers the bulk actions', async () => {
		const selected = new SvelteSet<string>();
		render(DataTable, props(queues(4), { selectable: true, selected }));
		await page.getByRole('checkbox', { name: 'Select every row on the page' }).click();
		expect([...selected].sort()).toEqual(['q0', 'q1', 'q2', 'q3']);
		await page.getByRole('checkbox', { name: 'Select the row' }).first().click();
		expect(selected.has('q0')).toBe(false);
		expect(selected.size).toBe(3);
	});
});
