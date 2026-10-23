// The broker's operations the console's pages call, by the signatures
// Artemis's console calls them with, over the resource API.

import { resources } from '#lib/api/index.js';

export const queueResource = (name: string) => `queue.${name}`;
export const addressResource = (name: string) => `address.${name}`;

/** A message as `browse` gives it (`CompositeDataConstants`). */
export interface Message {
	messageID: string;
	userID?: string | null;
	address: string;
	type: number;
	durable: boolean;
	expiration: number;
	timestamp: number;
	priority: number;
	redelivered: boolean;
	largeMessage: boolean;
	persistentSize: number;
	protocol: string;
	/** A text message's body. */
	text?: string | null;
	/** The first bytes of any other body. */
	BodyPreview?: number[] | null;
	PropertiesText?: string;
	StringProperties?: Record<string, string> | null;
	BooleanProperties?: Record<string, boolean> | null;
	ByteProperties?: Record<string, number> | null;
	ShortProperties?: Record<string, number> | null;
	IntProperties?: Record<string, number> | null;
	LongProperties?: Record<string, number> | null;
	FloatProperties?: Record<string, number> | null;
	DoubleProperties?: Record<string, number> | null;
	[other: string]: unknown;
}

/** `Message`'s type byte, as Artemis's console names it. */
export const MESSAGE_TYPES = [
	'DEFAULT',
	'1',
	'object',
	'text',
	'bytes',
	'map',
	'stream',
	'embedded'
];

const PROPERTY_KINDS = [
	'StringProperties',
	'BooleanProperties',
	'ByteProperties',
	'ShortProperties',
	'IntProperties',
	'LongProperties',
	'FloatProperties',
	'DoubleProperties'
] as const;

/** A message's properties, each with its type. */
export function properties(m: Message): { name: string; type: string; value: unknown }[] {
	const out: { name: string; type: string; value: unknown }[] = [];
	for (const kind of PROPERTY_KINDS) {
		for (const [name, value] of Object.entries(m[kind] ?? {})) {
			out.push({ name, type: kind.replace('Properties', ''), value });
		}
	}
	return out.sort((a, b) => a.name.localeCompare(b.name));
}

/** The queue a message on a DLQ or expiry queue came from. */
export function originalQueue(m: Message): string | undefined {
	return m.StringProperties?._AMQ_ORIG_QUEUE ?? undefined;
}

const SEND =
	'sendMessage(java.util.Map,int,java.lang.String,boolean,java.lang.String,java.lang.String,boolean)';

export interface Send {
	/** String properties. */
	headers: Record<string, string>;
	body: string;
	durable: boolean;
	createMessageId: boolean;
	/** None: as the logged-in user. */
	credentials?: { user: string; password: string };
}

/** Sends a text message to an address's or a queue's resource. */
export function send(resource: string, s: Send): Promise<string> {
	return resources.invoke<string>(resource, SEND, [
		s.headers,
		3,
		s.body,
		s.durable,
		s.credentials?.user ?? null,
		s.credentials?.password ?? null,
		s.createMessageId
	]);
}

export interface QueueSettings {
	name: string;
	address: string;
	routingType: 'ANYCAST' | 'MULTICAST';
	durable: boolean;
	filter?: string;
	maxConsumers?: number;
	purgeOnNoConsumers?: boolean;
	exclusive?: boolean;
	lastValue?: boolean;
	lastValueKey?: string;
	nonDestructive?: boolean;
	groupRebalance?: boolean;
	groupBuckets?: number;
	consumersBeforeDispatch?: number;
	delayBeforeDispatch?: number;
	autoCreateAddress?: boolean;
}

/** `QueueConfiguration.toJSON`'s names. */
export function queueConfiguration(s: QueueSettings): string {
	const c: Record<string, unknown> = {
		name: s.name,
		address: s.address,
		'routing-type': s.routingType,
		durable: s.durable,
		'filter-string': s.filter || undefined,
		'max-consumers': s.maxConsumers,
		'purge-on-no-consumers': s.purgeOnNoConsumers,
		exclusive: s.exclusive,
		'last-value': s.lastValue,
		'last-value-key': s.lastValueKey || undefined,
		'non-destructive': s.nonDestructive,
		'group-rebalance': s.groupRebalance,
		'group-buckets': s.groupBuckets,
		'consumers-before-dispatch': s.consumersBeforeDispatch,
		'delay-before-dispatch': s.delayBeforeDispatch,
		'auto-create-address': s.autoCreateAddress
	};
	for (const k of Object.keys(c)) if (c[k] === undefined) delete c[k];
	return JSON.stringify(c);
}

/** A member of `listNetworkTopology`. */
export interface TopologyMember {
	nodeID: string;
	live?: string;
	backup?: string;
	primary?: string;
}

const onBroker = <T>(operation: string, args: unknown[] = []) =>
	resources.invoke<T>('broker', operation, args);

export const broker = {
	createAddress: (name: string, routingTypes: string) =>
		onBroker('createAddress(java.lang.String,java.lang.String)', [name, routingTypes]),
	deleteAddress: (name: string, force: boolean) =>
		onBroker('deleteAddress(java.lang.String,boolean)', [name, force]),
	createQueue: (s: QueueSettings) =>
		onBroker('createQueue(java.lang.String,boolean)', [queueConfiguration(s), false]),
	destroyQueue: (name: string, removeConsumers: boolean, autoDeleteAddress: boolean) =>
		onBroker('destroyQueue(java.lang.String,boolean,boolean)', [
			name,
			removeConsumers,
			autoDeleteAddress
		]),
	topology: () =>
		onBroker<string>('listNetworkTopology').then((t) => JSON.parse(t) as TopologyMember[])
};

const onQueue = <T>(queue: string, operation: string, args: unknown[] = []) =>
	resources.invoke<T>(queueResource(queue), operation, args);

export const queue = {
	browse: (q: string, page: number, size: number, filter: string) =>
		onQueue<Message[]>(q, 'browse(int,int,java.lang.String)', [page, size, filter]),
	count: (q: string, filter: string) =>
		filter
			? onQueue<number>(q, 'countMessages(java.lang.String)', [filter])
			: onQueue<number>(q, 'countMessages()'),
	remove: (q: string, id: string) => onQueue<boolean>(q, 'removeMessage(long)', [id]),
	move: (q: string, id: string, to: string) =>
		onQueue<boolean>(q, 'moveMessage(long,java.lang.String)', [id, to]),
	copy: (q: string, id: string, to: string) =>
		onQueue<boolean>(q, 'copyMessage(long,java.lang.String)', [id, to]),
	retry: (q: string, id: string) => onQueue<boolean>(q, 'retryMessage(long)', [id]),
	expire: (q: string, id: string) => onQueue<boolean>(q, 'expireMessage(long)', [id]),
	toDeadLetter: (q: string, id: string) =>
		onQueue<boolean>(q, 'sendMessageToDeadLetterAddress(long)', [id]),
	changePriority: (q: string, id: string, priority: number) =>
		onQueue<boolean>(q, 'changeMessagePriority(long,int)', [id, priority]),
	removeAll: (q: string) => onQueue<number>(q, 'removeAllMessages()'),
	removeMatching: (q: string, filter: string) =>
		onQueue<number>(q, 'removeMessages(java.lang.String)', [filter]),
	moveMatching: (q: string, filter: string, to: string) =>
		onQueue<number>(q, 'moveMessages(java.lang.String,java.lang.String)', [filter, to]),
	retryAll: (q: string) => onQueue<number>(q, 'retryMessages()'),
	pause: (q: string) => onQueue(q, 'pause()'),
	resume: (q: string) => onQueue(q, 'resume()'),
	enable: (q: string) => onQueue(q, 'enable()'),
	disable: (q: string) => onQueue(q, 'disable()'),
	resetCounters: (q: string) => onQueue(q, 'resetMessageCounter()')
};

const onAddress = <T>(address: string, operation: string, args: unknown[] = []) =>
	resources.invoke<T>(addressResource(address), operation, args);

export const address = {
	pause: (a: string) => onAddress(a, 'pause()'),
	resume: (a: string) => onAddress(a, 'resume()')
};

/** `DivertConfiguration.toJSON`'s names. */
export interface Divert {
	name: string;
	address: string;
	forwardingAddress: string;
	routingName?: string;
	exclusive: boolean;
	filter?: string;
	routingType: 'STRIP' | 'PASS' | 'ANYCAST' | 'MULTICAST';
	transformerClassName?: string;
	transformerProperties?: Record<string, string>;
}

export function divertConfiguration(d: Divert): string {
	const c: Record<string, unknown> = {
		name: d.name,
		address: d.address,
		'forwarding-address': d.forwardingAddress,
		'routing-name': d.routingName || undefined,
		exclusive: d.exclusive,
		'filter-string': d.filter || undefined,
		'routing-type': d.routingType,
		'transformer-configuration': d.transformerClassName
			? { 'class-name': d.transformerClassName, properties: d.transformerProperties ?? {} }
			: undefined
	};
	for (const k of Object.keys(c)) if (c[k] === undefined) delete c[k];
	return JSON.stringify(c);
}

/** A user of the basic security manager, as `listUser` gives them. */
export interface User {
	username: string;
	roles: string[];
}

/** A prepared transaction, as `listPreparedTransactionDetailsAsJSON` gives it. */
export interface PreparedTransaction {
	creation_time: string;
	xid_as_base64: string;
	xid_format_id: number;
	xid_global_txid: string;
	xid_branch_qual: string;
	tx_related_messages: {
		message_operation_type: string;
		message_type: string;
		message_properties: Record<string, unknown>;
	}[];
}

/** The permissions of a role, as `getRolesAsJSON` names them. */
export const PERMISSIONS = [
	'send',
	'consume',
	'createDurableQueue',
	'deleteDurableQueue',
	'createNonDurableQueue',
	'deleteNonDurableQueue',
	'manage',
	'browse',
	'createAddress',
	'deleteAddress',
	'view',
	'edit'
] as const;
export type Permission = (typeof PERMISSIONS)[number];

/** `addSecuritySettings(match, json)`'s body: each permission's roles. */
export function securitySettingsJson(grants: Record<Permission, string[]>): string {
	return JSON.stringify(Object.fromEntries(PERMISSIONS.map((p) => [p, grants[p] ?? []])));
}

const parsed = <T>(text: Promise<string>) => text.then((t) => JSON.parse(t) as T);

export const config = {
	addressSettings: (address: string, node?: string) =>
		parsed<Record<string, unknown>>(
			resources.invoke<string>('broker', 'getAddressSettingsAsJSON', [address], node)
		),
	addAddressSettings: (match: string, settings: Record<string, unknown>) =>
		onBroker<string>('addAddressSettings(java.lang.String,java.lang.String)', [
			match,
			JSON.stringify(settings)
		]),
	removeAddressSettings: (match: string) => onBroker('removeAddressSettings', [match]),
	createDivert: (d: Divert) => onBroker('createDivert(java.lang.String)', [divertConfiguration(d)]),
	updateDivert: (d: Divert) => onBroker('updateDivert(java.lang.String)', [divertConfiguration(d)]),
	destroyDivert: (name: string) => onBroker('destroyDivert', [name]),
	exportProperties: () => onBroker('exportConfigAsProperties'),
	reload: () => onBroker('reloadConfigurationFile')
};

export const security = {
	users: () => parsed<User[]>(onBroker<string>('listUser', [''])),
	addUser: (user: string, password: string, roles: string[], plaintext: boolean) =>
		onBroker('addUser', [user, password, roles.join(','), plaintext]),
	resetUser: (user: string, password: string | null, roles: string[], plaintext: boolean) =>
		onBroker('resetUser(java.lang.String,java.lang.String,java.lang.String,boolean)', [
			user,
			password,
			roles.join(','),
			plaintext
		]),
	removeUser: (user: string) => onBroker('removeUser', [user]),
	roles: (address: string, node?: string) =>
		parsed<Record<string, unknown>[]>(
			resources.invoke<string>('broker', 'getRolesAsJSON', [address], node)
		),
	addSecuritySettings: (match: string, grants: Record<Permission, string[]>) =>
		onBroker('addSecuritySettings(java.lang.String,java.lang.String)', [
			match,
			securitySettingsJson(grants)
		]),
	removeSecuritySettings: (match: string) => onBroker('removeSecuritySettings', [match]),
	clearAuthenticationCache: () => onBroker('clearAuthenticationCache'),
	clearAuthorizationCache: () => onBroker('clearAuthorizationCache')
};

export const transactions = {
	/** None is an empty text, as Artemis answers. */
	prepared: () =>
		onBroker<string>('listPreparedTransactionDetailsAsJSON').then((t) =>
			t ? (JSON.parse(t) as PreparedTransaction[]) : []
		),
	heuristicCommitted: () => onBroker<string[]>('listHeuristicCommittedTransactions'),
	heuristicRolledBack: () => onBroker<string[]>('listHeuristicRolledBackTransactions'),
	commit: (xid: string) => onBroker<boolean>('commitPreparedTransaction', [xid]),
	rollback: (xid: string) => onBroker<boolean>('rollbackPreparedTransaction', [xid])
};

/** A queue's operations on the messages a filter selects; each answers how many it touched. */
export const matching = {
	move: (q: string, filter: string, to: string) => queue.moveMatching(q, filter, to),
	remove: (q: string, filter: string) => queue.removeMatching(q, filter),
	retry: (q: string, filter: string) =>
		onQueue<number>(q, 'retryMessages(java.lang.String)', [filter]),
	expire: (q: string, filter: string) =>
		onQueue<number>(q, 'expireMessages(java.lang.String)', [filter]),
	toDeadLetter: (q: string, filter: string) =>
		onQueue<number>(q, 'sendMessagesToDeadLetterAddress(java.lang.String)', [filter]),
	changePriority: (q: string, filter: string, priority: number) =>
		onQueue<number>(q, 'changeMessagesPriority(java.lang.String,int)', [filter, priority])
};

/** A queue's scheduled and delivering messages. */
export const pending = {
	scheduled: (q: string) =>
		parsed<Record<string, unknown>[]>(onQueue<string>(q, 'listScheduledMessagesAsJSON')),
	/** By consumer: each one's messages, keyed by its description. */
	delivering: (q: string) =>
		parsed<Record<string, Record<string, unknown>[]>>(
			onQueue<string>(q, 'listDeliveringMessagesAsJSON')
		),
	deliverScheduled: (q: string, id: string) => onQueue(q, 'deliverScheduledMessage', [id]),
	deliverAllScheduled: (q: string, filter: string) =>
		onQueue(q, 'deliverScheduledMessages', [filter])
};
