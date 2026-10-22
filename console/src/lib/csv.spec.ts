import { describe, expect, it } from 'vitest';
import { toCsv } from './csv';

describe('CSV', () => {
	it('quotes what needs quoting', () => {
		const csv = toCsv(
			['name', 'filter', 'count'],
			['Name', 'Filter', 'Count'],
			[
				{ name: 'orders', filter: "color = 'red', size > 2", count: '3' },
				{ name: 'say "hi"', filter: '', count: 0 },
				{ name: ' padded', filter: 'a\nb', count: null }
			]
		);
		expect(csv).toBe(
			'Name,Filter,Count\r\n' +
				'orders,"color = \'red\', size > 2",3\r\n' +
				'"say ""hi""",,0\r\n' +
				'" padded","a\nb",\r\n'
		);
	});
});
