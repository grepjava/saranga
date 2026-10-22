import { describe, expect, it } from 'vitest';
import { pages, parseFilter, parseQuery, queryParams } from './query';

describe('filters', () => {
	it('keeps the colons of the value', () => {
		expect(parseFilter('remoteAddress:CONTAINS:127.0.0.1:61616')).toEqual({
			field: 'remoteAddress',
			operation: 'CONTAINS',
			value: '127.0.0.1:61616'
		});
	});

	it('allows an empty value', () => {
		expect(parseFilter('clientID:EQUALS:')).toEqual({
			field: 'clientID',
			operation: 'EQUALS',
			value: ''
		});
	});

	it('rejects what is not field:OPERATION:value', () => {
		expect(parseFilter('name')).toBeUndefined();
		expect(parseFilter('name:CONTAINS')).toBeUndefined();
		expect(parseFilter('name:LIKE:x')).toBeUndefined();
	});
});

describe('queries', () => {
	it('round-trips through the URL', () => {
		const p = new URLSearchParams(
			'filter=name:CONTAINS:orders&filter=messageCount:GREATER_THAN:10&sort=messageCount&order=desc&page=3&size=50'
		);
		const q = parseQuery(p, 100);
		expect(q).toEqual({
			filters: [
				{ field: 'name', operation: 'CONTAINS', value: 'orders' },
				{ field: 'messageCount', operation: 'GREATER_THAN', value: '10' }
			],
			sort: 'messageCount',
			order: 'desc',
			page: 3,
			size: 50
		});
		expect(queryParams(q, 100).toString()).toBe(p.toString());
		expect(parseQuery(queryParams(q, 100), 100)).toEqual(q);
	});

	it('leaves out the defaults', () => {
		expect(queryParams(parseQuery(new URLSearchParams(), 100), 100).toString()).toBe('');
	});

	it('ignores bad pages and caps the size', () => {
		const q = parseQuery(new URLSearchParams('page=0&size=99999'), 100);
		expect(q.page).toBe(1);
		expect(q.size).toBe(10000);
		expect(parseQuery(new URLSearchParams('page=x&size=-1'), 25)).toMatchObject({
			page: 1,
			size: 25
		});
	});

	it('counts at least one page', () => {
		expect(pages(0, 100)).toBe(1);
		expect(pages(100, 100)).toBe(1);
		expect(pages(101, 100)).toBe(2);
	});
});
