// What the console knows of each listing: its columns, which of them it
// shows at first, how it names a row, where a value leads, and what can be
// done to a row.

import { api, type Listing, type Rows } from '#lib/api/index.js';

export interface Link {
	kind: Listing;
	field: string;
}

export interface Close<K extends Listing> {
	/** The broker operation the guard must allow. */
	operation: string;
	run: (row: Rows[K]) => Promise<unknown>;
}

export interface ListingInfo<K extends Listing> {
	title: string;
	/** One of them, lower case. */
	noun: string;
	/** Every member of a row, in the order the console offers them. */
	columns: readonly (keyof Rows[K] & string)[];
	/** The columns shown until the user picks others. */
	shown: readonly (keyof Rows[K] & string)[];
	key: (row: Rows[K]) => string;
	links?: Partial<Record<keyof Rows[K] & string, Link>>;
	close?: Close<K>;
}

/** Every member of `T`, each once: a compile error otherwise. */
function every<T>() {
	return <const C extends readonly (keyof T & string)[]>(
		columns: C & ([Exclude<keyof T, C[number]>] extends [never] ? unknown : never)
	) => columns;
}

const connections: ListingInfo<'connections'> = {
	title: 'Connections',
	noun: 'connection',
	columns: every<Rows['connections']>()([
		'connectionID',
		'clientID',
		'users',
		'protocol',
		'sessionCount',
		'remoteAddress',
		'localAddress',
		'creationTime',
		'implementation',
		'proxyAddress',
		'proxyProtocolVersion'
	]),
	shown: [
		'connectionID',
		'clientID',
		'users',
		'protocol',
		'sessionCount',
		'remoteAddress',
		'localAddress',
		'creationTime'
	],
	key: (r) => r.connectionID,
	close: {
		operation: 'closeConnectionWithID',
		run: (r) => api.closeConnection(r.connectionID)
	}
};

const sessions: ListingInfo<'sessions'> = {
	title: 'Sessions',
	noun: 'session',
	columns: every<Rows['sessions']>()([
		'id',
		'connectionID',
		'consumerCount',
		'producerCount',
		'user',
		'validatedUser',
		'clientID',
		'creationTime'
	]),
	shown: [
		'id',
		'connectionID',
		'consumerCount',
		'producerCount',
		'user',
		'validatedUser',
		'creationTime'
	],
	key: (r) => `${r.connectionID}/${r.id}`,
	links: { connectionID: { kind: 'connections', field: 'connectionID' } },
	close: {
		operation: 'closeSessionWithID',
		run: (r) => api.closeSession(r.connectionID, r.id)
	}
};

const consumers: ListingInfo<'consumers'> = {
	title: 'Consumers',
	noun: 'consumer',
	columns: every<Rows['consumers']>()([
		'id',
		'session',
		'clientID',
		'user',
		'validatedUser',
		'protocol',
		'queue',
		'queueType',
		'filter',
		'address',
		'localAddress',
		'remoteAddress',
		'creationTime',
		'messagesInTransit',
		'messagesInTransitSize',
		'messagesDelivered',
		'messagesDeliveredSize',
		'messagesAcknowledged',
		'messagesAcknowledgedAwaitingCommit',
		'lastDeliveredTime',
		'lastAcknowledgedTime',
		'status'
	]),
	shown: [
		'id',
		'session',
		'validatedUser',
		'protocol',
		'queue',
		'address',
		'remoteAddress',
		'messagesInTransit',
		'messagesDelivered',
		'messagesAcknowledged',
		'status'
	],
	key: (r) => `${r.session}/${r.id}`,
	links: {
		session: { kind: 'sessions', field: 'id' },
		queue: { kind: 'queues', field: 'name' },
		address: { kind: 'addresses', field: 'name' }
	},
	close: {
		operation: 'closeConsumerWithID',
		run: (r) => api.closeConsumer(r.session, r.id)
	}
};

const producers: ListingInfo<'producers'> = {
	title: 'Producers',
	noun: 'producer',
	columns: every<Rows['producers']>()([
		'id',
		'name',
		'session',
		'clientID',
		'user',
		'validatedUser',
		'protocol',
		'address',
		'localAddress',
		'remoteAddress',
		'creationTime',
		'msgSent',
		'msgSizeSent',
		'lastProducedMessageID'
	]),
	shown: [
		'id',
		'name',
		'session',
		'validatedUser',
		'protocol',
		'address',
		'remoteAddress',
		'msgSent',
		'msgSizeSent'
	],
	key: (r) => `${r.session}/${r.id}`,
	links: {
		session: { kind: 'sessions', field: 'id' },
		address: { kind: 'addresses', field: 'name' }
	}
};

const addresses: ListingInfo<'addresses'> = {
	title: 'Addresses',
	noun: 'address',
	columns: every<Rows['addresses']>()([
		'id',
		'name',
		'routingTypes',
		'queueCount',
		'messageCount',
		'routedMessageCount',
		'unroutedMessageCount',
		'internal',
		'temporary',
		'autoCreated',
		'paused',
		'paging',
		'numberOfPages',
		'addressSize',
		'addressLimitPercent',
		'currentDuplicateIdCacheSize',
		'retroactiveResource',
		'numberOfBytesPerPage',
		'maxPageReadBytes',
		'maxPageReadMessages',
		'prefetchPageBytes',
		'prefetchPageMessages'
	]),
	shown: [
		'id',
		'name',
		'routingTypes',
		'queueCount',
		'messageCount',
		'routedMessageCount',
		'internal',
		'paused',
		'paging'
	],
	key: (r) => r.name
};

const queues: ListingInfo<'queues'> = {
	title: 'Queues',
	noun: 'queue',
	columns: every<Rows['queues']>()([
		'id',
		'name',
		'address',
		'routingType',
		'filter',
		'messageCount',
		'consumerCount',
		'deliveringCount',
		'scheduledCount',
		'messagesAdded',
		'messagesAcked',
		'messagesExpired',
		'messagesKilled',
		'maxConsumers',
		'durable',
		'temporary',
		'autoCreated',
		'autoDelete',
		'internalQueue',
		'paused',
		'persistedPause',
		'enabled',
		'purgeOnNoConsumers',
		'exclusive',
		'lastValue',
		'lastValueKey',
		'directDeliver',
		'groupRebalance',
		'groupRebalancePauseDispatch',
		'groupBuckets',
		'groupFirstKey',
		'ringSize',
		'consumersBeforeDispatch',
		'delayBeforeDispatch',
		'user'
	]),
	shown: [
		'id',
		'name',
		'address',
		'routingType',
		'messageCount',
		'consumerCount',
		'deliveringCount',
		'messagesAdded',
		'messagesAcked',
		'durable',
		'paused'
	],
	key: (r) => r.name,
	links: { address: { kind: 'addresses', field: 'name' } }
};

export const LISTINGS: { [K in Listing]: ListingInfo<K> } = {
	connections,
	sessions,
	consumers,
	producers,
	addresses,
	queues
};

export const LISTING_KINDS = Object.keys(LISTINGS) as Listing[];

export function isListing(kind: string): kind is Listing {
	return Object.hasOwn(LISTINGS, kind);
}

const WORDS: Record<string, string> = { id: 'ID' };

const LABELS: Record<string, string> = {
	msgSent: 'Messages sent',
	msgSizeSent: 'Bytes sent',
	messagesInTransitSize: 'Bytes in transit',
	messagesDeliveredSize: 'Bytes delivered',
	messagesAcknowledgedAwaitingCommit: 'Acknowledged, awaiting commit',
	lastProducedMessageID: 'Last message ID',
	currentDuplicateIdCacheSize: 'Duplicate ID cache size',
	numberOfBytesPerPage: 'Page size (bytes)',
	addressSize: 'Size (bytes)',
	addressLimitPercent: 'Limit used (%)'
};

/** A column's heading: `connectionID` is "Connection ID". */
export function label(column: string): string {
	if (LABELS[column]) return LABELS[column];
	const words = column
		.replace(/ID\b|ID(?=[A-Z])/g, 'Id')
		.split(/(?=[A-Z])/)
		.map((w) => w.toLowerCase());
	return words
		.map((w, i) => {
			const word = WORDS[w] ?? w;
			return i === 0 ? word.charAt(0).toUpperCase() + word.slice(1) : word;
		})
		.join(' ');
}
