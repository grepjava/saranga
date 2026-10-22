import { describe as group, expect, it } from 'vitest';
import { describe, typeTone } from './events.js';

group('a notification', () => {
	it('says what it is about', () => {
		expect(
			describe({
				seq: 1,
				type: 'CONSUMER_CREATED',
				timestamp: 0,
				properties: {
					_AMQ_RoutingName: 'orders',
					_AMQ_Address: 'orders',
					_AMQ_User: 'admin',
					_AMQ_Client_ID: null,
					_AMQ_Distance: 0
				}
			})
		).toBe('address orders, queue orders, user admin');
	});
	it('leaves out the notification address', () => {
		expect(
			describe({
				seq: 2,
				type: 'SESSION_CREATED',
				timestamp: 0,
				properties: { _AMQ_Address: 'activemq.notifications', _AMQ_User: 'admin' }
			})
		).toBe('user admin');
	});
	it('has a tone by its type', () => {
		expect(typeTone('SECURITY_PERMISSION_VIOLATION')).toBe('destructive');
		expect(typeTone('CONSUMER_CLOSED')).toBe('secondary');
		expect(typeTone('CONSUMER_SLOW')).toBe('outline');
		expect(typeTone('BINDING_ADDED')).toBe('default');
	});
});
