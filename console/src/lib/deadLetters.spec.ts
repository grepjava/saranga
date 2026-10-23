import { describe, expect, it } from 'vitest';
import type { Message } from '#lib/broker.js';
import { groups, quoted } from './deadLetters.js';

const message = (timestamp: number, queue?: string, expired = false): Message =>
	({
		messageID: String(timestamp),
		address: 'DLA',
		type: 3,
		durable: true,
		expiration: 0,
		timestamp,
		priority: 4,
		redelivered: false,
		largeMessage: false,
		persistentSize: 100,
		protocol: 'CORE',
		StringProperties: queue
			? { _AMQ_ORIG_QUEUE: queue, _AMQ_ORIG_ADDRESS: `${queue}.address` }
			: {},
		LongProperties: expired ? { _AMQ_ACTUAL_EXPIRY: timestamp + 1 } : null
	}) as Message;

describe('groups', () => {
	const messages = [message(3, 'orders'), message(1, 'orders'), message(2, "it's"), message(5)];

	it('groups by the original queue, largest first, with a filter for each', () => {
		const g = groups(messages, 'queue');
		expect(g.map((x) => [x.label, x.count, x.first, x.last])).toEqual([
			['orders', 2, 1, 3],
			["it's", 1, 2, 2],
			['No queue', 1, 5, 5]
		]);
		expect(g[0].filter).toBe("_AMQ_ORIG_QUEUE = 'orders'");
		expect(g[1].filter).toBe("_AMQ_ORIG_QUEUE = 'it''s'");
		expect(g[2].filter).toBe('_AMQ_ORIG_QUEUE IS NULL');
	});

	it('groups by the original address', () => {
		expect(groups(messages, 'address')[0].filter).toBe("_AMQ_ORIG_ADDRESS = 'orders.address'");
	});

	it('groups by whether they expired', () => {
		const g = groups([message(1, 'a', true), message(2, 'a'), message(3, 'b')], 'reason');
		expect(g.map((x) => [x.key, x.count, x.filter])).toEqual([
			['undelivered', 2, '_AMQ_ACTUAL_EXPIRY IS NULL'],
			['expired', 1, '_AMQ_ACTUAL_EXPIRY IS NOT NULL']
		]);
	});
});

describe('quoted', () => {
	it('doubles quotes', () => {
		expect(quoted("a'b")).toBe("'a''b'");
	});
});
