import { describe, expect, it } from 'vitest';
import { prepend, rates } from './series.js';

describe('rates', () => {
	it('is the change per second', () => {
		expect(rates([0, 2000, 4000], [10, 30, 30])).toEqual([null, 10, 0]);
	});
	it('leaves out a reset counter and missing samples', () => {
		expect(rates([0, 1000, 2000, 3000], [10, 5, null, 8])).toEqual([null, null, null, null]);
	});
});

describe('prepend', () => {
	it('puts the history before the live samples', () => {
		const past = { times: [1, 2, 3], attributes: { A: [1, 2, 3] } };
		expect(
			prepend(
				past,
				['A', 'B'],
				[3, 4],
				[
					[30, 40],
					[7, 8]
				]
			)
		).toEqual({
			times: [1, 2, 3, 4],
			values: [
				[1, 2, 30, 40],
				[null, null, 7, 8]
			]
		});
	});
	it('takes all of it before any live sample', () => {
		expect(prepend({ times: [1, 2], attributes: { A: [5, 6] } }, ['A'], [], [[]])).toEqual({
			times: [1, 2],
			values: [[5, 6]]
		});
	});
});
