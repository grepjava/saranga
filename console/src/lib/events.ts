// How the feed shows a management notification.

import type { Notification } from '#lib/api/index.js';

/** The properties that say what a notification is about, in the order shown. */
const SAYS: [string, string][] = [
	['_AMQ_Address', 'address'],
	['_AMQ_RoutingName', 'queue'],
	['_AMQ_ConsumerCount', 'consumers'],
	['_AMQ_User', 'user'],
	['_AMQ_CheckType', 'permission'],
	['_AMQ_RemoteAddress', 'from'],
	['_AMQ_ConnectionName', 'connection'],
	['_AMQ_SessionName', 'session'],
	['_AMQ_Protocol_Name', 'protocol'],
	['_AMQ_Message_ID', 'message']
];

/**
 * `address orders, queue orders, user admin`: what the feed's row says. A
 * notification not about an address names the notification address, which
 * it leaves out.
 */
export function describe(n: Notification, notificationAddress = 'activemq.notifications'): string {
	const p = n.properties;
	return SAYS.filter(([k]) => p[k] !== undefined && p[k] !== null && p[k] !== '')
		.filter(([k]) => k !== '_AMQ_Address' || p[k] !== notificationAddress)
		.map(([k, label]) => `${label} ${String(p[k])}`)
		.join(', ');
}

export type Tone = 'default' | 'secondary' | 'destructive' | 'outline';

/** Violations stand out; what ends is quieter than what starts. */
export function typeTone(type: string): Tone {
	if (type.startsWith('SECURITY_')) return 'destructive';
	if (/CLOSED|REMOVED|STOPPED|DESTROYED|UNPROPOSAL/.test(type)) return 'secondary';
	if (/SLOW|EXPIRED/.test(type)) return 'outline';
	return 'default';
}
