// The resource explorer's helpers: how a Java type reads, which input a
// parameter takes, and how a value shows. The broker converts a string
// argument to any parameter type, as Jolokia does.

import type { ResourceSummary, ResourceType } from '#lib/api/index.js';

/** `[Ljava.lang.String;` is `String[]`, `java.util.Map` is `Map`. */
export function typeLabel(type: string): string {
	const array = /^\[+/.exec(type)?.[0].length ?? 0;
	let base = type.slice(array);
	if (array) {
		const primitive: Record<string, string> = {
			Z: 'boolean',
			B: 'byte',
			C: 'char',
			S: 'short',
			I: 'int',
			J: 'long',
			F: 'float',
			D: 'double'
		};
		base = base.startsWith('L') ? base.slice(1, -1) : (primitive[base] ?? base);
	}
	return base.slice(base.lastIndexOf('.') + 1) + '[]'.repeat(array);
}

export type Input = 'boolean' | 'number' | 'text' | 'json';

const NUMBERS = new Set([
	'int',
	'long',
	'short',
	'byte',
	'double',
	'float',
	'java.lang.Integer',
	'java.lang.Long',
	'java.lang.Short',
	'java.lang.Byte',
	'java.lang.Double',
	'java.lang.Float'
]);

/** The input a parameter of a type takes. */
export function inputFor(type: string): Input {
	if (type === 'boolean' || type === 'java.lang.Boolean') return 'boolean';
	if (NUMBERS.has(type)) return 'number';
	if (type === 'java.lang.String' || type === 'char' || type === 'java.lang.Character')
		return 'text';
	return 'json';
}

/** What an input sends: a boolean as one, anything else as its text, which the broker converts. */
export function argument(type: string, raw: string | boolean): unknown {
	if (inputFor(type) === 'boolean') return raw === true || raw === 'true';
	if (typeof raw !== 'string') return String(raw);
	// A nullable object parameter left empty is null.
	if (raw === '' && inputFor(type) === 'json') return null;
	return raw;
}

/** A value on one line, or indented when it is an object. */
export function display(value: unknown, indent = false): string {
	if (value === undefined) return '';
	if (typeof value === 'string') return value;
	return JSON.stringify(value, null, indent ? 2 : undefined) ?? String(value);
}

/** Whether a value can be charted. */
export function numeric(value: unknown): value is number {
	return typeof value === 'number' && Number.isFinite(value);
}

/** The groups of the explorer's tree, in its order. */
export const GROUPS: { type: ResourceType; label: string }[] = [
	{ type: 'broker', label: 'Broker' },
	{ type: 'address', label: 'Addresses' },
	{ type: 'queue', label: 'Queues' },
	{ type: 'divert', label: 'Diverts' },
	{ type: 'acceptor', label: 'Acceptors' },
	{ type: 'clusterconnection', label: 'Cluster connections' },
	{ type: 'bridge', label: 'Bridges' },
	{ type: 'broadcastgroup', label: 'Broadcast groups' },
	{ type: 'brokerconnection', label: 'Broker connections' },
	{ type: 'platform', label: 'Process' },
	{ type: 'hawtio', label: 'Console' }
];

/** A resource's name within its group: `queue.orders` is `orders`. */
export function shortName(r: ResourceSummary): string {
	if (r.type === 'broker') return 'broker';
	if (r.type === 'platform') return r.resource.slice('java.lang.'.length);
	if (r.type === 'hawtio') return 'security';
	return r.resource.slice(r.resource.indexOf('.') + 1);
}

/** The broker's own addresses, which the diagram and the tree hide unless asked. */
export function isInternal(name: string): boolean {
	return name.startsWith('activemq.') || name.startsWith('$sys.') || name.startsWith('$.artemis.');
}
