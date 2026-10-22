import { describe, expect, it } from 'vitest';
import { scopedPath } from './index';

const API = '/saranga/api/v1';

describe('calls for another node', () => {
	it('go to /nodes/{node}/...', () => {
		expect(scopedPath(`${API}/queues`, 'n2')).toBe(`${API}/nodes/n2/queues`);
		expect(scopedPath(`${API}/resources/queue.a%2Fb/operations`, 'n2')).toBe(
			`${API}/nodes/n2/resources/queue.a%2Fb/operations`
		);
		expect(scopedPath(`${API}/me/can`, 'n2')).toBe(`${API}/nodes/n2/me/can`);
		expect(scopedPath(`${API}/queues`, 'all')).toBe(`${API}/nodes/all/queues`);
	});

	it('leave this broker as they are', () => {
		expect(scopedPath(`${API}/queues`, 'local')).toBe(`${API}/queues`);
	});

	it('keep the session and the live data on this broker', () => {
		for (const p of [
			'session',
			'me',
			'views',
			'nodes',
			'events',
			'events/abc',
			'history',
			'notifications',
			'openapi.json'
		]) {
			expect(scopedPath(`${API}/${p}`, 'n2')).toBe(`${API}/${p}`);
		}
	});

	it('name the node as a path segment', () => {
		expect(scopedPath(`${API}/broker`, 'a/b')).toBe(`${API}/nodes/a%2Fb/broker`);
	});
});
