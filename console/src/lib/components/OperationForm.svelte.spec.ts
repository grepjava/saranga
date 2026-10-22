import { page } from 'vitest/browser';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import OperationForm from './OperationForm.svelte';
import QueuePicker from './QueuePicker.svelte';
import type { Operation } from '#lib/api/index.js';

// The broker, as `fetch` reaches it: the client keeps the `fetch` there is when it
// is made, so the stub is in place before any module loads.
const broker = vi.hoisted(() => {
	const b = {
		requests: [] as { url: string; body: unknown }[],
		status: 200,
		answer: (() => ({})) as (url: string) => unknown
	};
	vi.stubGlobal('fetch', async (input: Request) => {
		const body = input.method === 'GET' ? undefined : await input.clone().json();
		b.requests.push({ url: input.url, body });
		return new Response(JSON.stringify(b.answer(input.url)), {
			status: b.status,
			headers: { 'Content-Type': 'application/json' }
		});
	});
	return b;
});

beforeEach(() => {
	broker.requests.length = 0;
	broker.status = 200;
	broker.answer = () => ({});
});

const move: Operation = {
	name: 'moveMessages',
	signature: 'moveMessages(int,java.lang.String,java.lang.String,boolean)',
	description: 'Move the messages',
	returns: 'int',
	impact: 'ACTION',
	params: [
		{ name: 'flushLimit', type: 'int', description: 'Limit' },
		{ name: 'filter', type: 'java.lang.String', description: 'A filter' },
		{ name: 'otherQueueName', type: 'java.lang.String', description: 'The queue' },
		{ name: 'rejectDuplicates', type: 'boolean', description: 'Reject duplicates' }
	],
	allowed: true
};

describe('OperationForm', () => {
	it('takes each parameter by its type and calls the operation by signature', async () => {
		broker.answer = () => ({ result: 3 });
		render(OperationForm, { resource: 'queue.a', operation: move });
		await page
			.getByRole('button', { name: /^moveMessages\(int, String, String, boolean\)/ })
			.click();
		await page.getByLabelText(/^flushLimit/).fill('100');
		await page.getByLabelText(/^filter/).fill("color = 'red'");
		await page.getByLabelText(/^otherQueueName/).fill('b');
		await page.getByRole('checkbox').click();
		await page.getByRole('button', { name: 'Run' }).click();
		await expect.element(page.getByRole('status')).toBeInTheDocument();
		expect(page.getByRole('status').element().textContent).toBe('3');
		expect(broker.requests).toEqual([
			{
				url: expect.stringMatching(/\/saranga\/api\/v1\/resources\/queue\.a\/operations$/),
				body: { operation: move.signature, arguments: ['100', "color = 'red'", 'b', true] }
			}
		]);
	});

	it('shows what the broker answered when it fails', async () => {
		broker.status = 400;
		broker.answer = () => ({ error: 'AMQ229017: Queue b does not exist', status: 400 });
		render(OperationForm, { resource: 'queue.a', operation: move });
		await page.getByRole('button', { name: /^moveMessages/ }).click();
		await page.getByRole('button', { name: 'Run' }).click();
		await expect.element(page.getByRole('alert')).toBeInTheDocument();
		expect(page.getByRole('alert').element().textContent).toMatch(/AMQ229017/);
	});

	it('says when the guard refuses it', async () => {
		render(OperationForm, { resource: 'queue.a', operation: { ...move, allowed: false } });
		await expect.element(page.getByRole('button', { name: /^moveMessages/ })).toBeInTheDocument();
		expect(page.getByRole('button', { name: /^moveMessages/ }).element().textContent).toMatch(
			/Not permitted/
		);
		await page.getByRole('button', { name: /^moveMessages/ }).click();
		await expect.element(page.getByRole('button', { name: 'Run' })).toBeDisabled();
	});
});

describe('QueuePicker', () => {
	it('offers the other queues and gives the one picked', async () => {
		broker.answer = () => ({
			data: [{ name: 'a' }, { name: 'b' }],
			count: 2,
			page: 1,
			size: 1000
		});
		const onpick = vi.fn();
		render(QueuePicker, {
			open: true,
			title: 'Move to a queue',
			description: 'From a',
			action: 'Move',
			exclude: 'a',
			onpick
		});
		await expect.poll(() => document.querySelectorAll('#target-queues option').length).toBe(1);
		await page.getByLabelText('Queue').fill('b');
		await page.getByRole('button', { name: 'Move' }).click();
		expect(onpick).toHaveBeenCalledWith('b');
		await expect.element(page.getByRole('dialog')).not.toBeInTheDocument();
	});
});
