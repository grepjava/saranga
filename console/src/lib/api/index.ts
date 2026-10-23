// The console's API, typed from openapi.json (the broker's own document; see
// api_schema.rs). Every call goes through `call`, which turns an error into
// an ApiError and tells the session when the user is no longer logged in.
//
// The pages show one node, or every node: their calls go to it, at
// /saranga/api/v1/nodes/{node}/..., unless they name another. The session,
// the live stream, the history and the notifications are this broker's.

import createClient from 'openapi-fetch';
import type { components, paths } from './schema.js';
import type { ListQuery } from '#lib/query.js';
import { filterText } from '#lib/query.js';

export type Schemas = components['schemas'];
export type Me = Schemas['Me'];
export type Views = Schemas['Views'];
export type Listing = keyof Views;

/** A row of each listing, exactly as the broker writes it. */
export interface Rows {
	connections: Schemas['ConnectionsRow'];
	sessions: Schemas['SessionsRow'];
	consumers: Schemas['ConsumersRow'];
	producers: Schemas['ProducersRow'];
	addresses: Schemas['AddressesRow'];
	queues: Schemas['QueuesRow'];
}

export interface Page<K extends Listing> {
	data: Rows[K][];
	count: number;
	page: number;
	size: number;
	/** Across every node, those that could not answer. */
	errors?: Schemas['NodeError'][];
}

export class ApiError extends Error {
	constructor(
		readonly status: number,
		message: string
	) {
		super(message);
	}
}

const client = createClient<paths>({ baseUrl: '', credentials: 'same-origin' });

const PREFIX = '/saranga/api/v1/';
/** This broker, in the URL. */
export const LOCAL = 'local';
/** Every node, in the URL: only the listings span them. */
export const ALL = 'all';
/** The paths that are this broker's whatever node the pages show. */
const UNSCOPED =
	/^(?:(?:session|me|views|nodes|events|history|notifications|openapi\.json)$|events\/)/;
/** A call names its node with this header, which never reaches the broker. */
const NODE_HEADER = 'x-saranga-node';

let shown = LOCAL;

/** The node the pages show, which their calls go to. */
export function setNode(node: string) {
	shown = node;
}

export function currentNode(): string {
	return shown;
}

/** The options of a call to another node than the one shown. */
export function onNode(node: string | undefined): { headers?: Record<string, string> } {
	return node === undefined ? {} : { headers: { [NODE_HEADER]: node } };
}

/** Where a call to an API path goes for a node: the same path for this broker. */
export function scopedPath(path: string, node: string): string {
	if (node === LOCAL || !path.startsWith(PREFIX)) return path;
	const rest = path.slice(PREFIX.length);
	if (UNSCOPED.test(rest)) return path;
	return `${PREFIX}nodes/${encodeURIComponent(node)}/${rest}`;
}

client.use({
	async onRequest({ request }) {
		const node = request.headers.get(NODE_HEADER) ?? shown;
		request.headers.delete(NODE_HEADER);
		const url = new URL(request.url);
		const path = scopedPath(url.pathname, node);
		if (path === url.pathname) return request;
		url.pathname = path;
		// The body as bytes: a request copied with its body would stream it,
		// which a browser does over HTTP/2 only.
		const body =
			request.method === 'GET' || request.method === 'HEAD'
				? undefined
				: await request.arrayBuffer();
		return new Request(url, {
			method: request.method,
			headers: request.headers,
			body,
			credentials: request.credentials,
			signal: request.signal
		});
	}
});

/** The header every request that changes something carries. */
const mark = { 'X-Saranga-Console': '1' } as const;

let unauthorized: () => void = () => {};

/** Called whenever the broker answers 401: the session has ended. */
export function onUnauthorized(f: () => void) {
	unauthorized = f;
}

async function call<T>(
	request: Promise<{ data?: T; error?: unknown; response: Response }>
): Promise<T> {
	let r;
	try {
		r = await request;
	} catch (e) {
		throw new ApiError(0, `The broker cannot be reached: ${(e as Error).message}`);
	}
	if (r.response.status === 401) unauthorized();
	if (!r.response.ok) {
		const error = r.error as { error?: string } | undefined;
		throw new ApiError(r.response.status, error?.error ?? r.response.statusText);
	}
	return r.data as T;
}

export const api = {
	login: (user: string, password: string) =>
		call(
			client.POST('/saranga/api/v1/session', {
				params: { header: mark },
				body: { user, password }
			})
		),

	logout: () => call(client.DELETE('/saranga/api/v1/session', { params: { header: mark } })),

	me: () => call(client.GET('/saranga/api/v1/me')),

	can: (checks: { resource: string; method: string }[], node?: string) =>
		call(client.POST('/saranga/api/v1/me/can', { body: { checks }, ...onNode(node) })).then(
			(r) => r.results
		),

	views: () => call(client.GET('/saranga/api/v1/views')),

	nodes: () => call(client.GET('/saranga/api/v1/nodes')),

	broker: (node?: string) => call(client.GET('/saranga/api/v1/broker', onNode(node))),

	list: <K extends Listing>(kind: K, q: ListQuery, node?: string): Promise<Page<K>> =>
		call(
			// The six listings share one shape; the path's kind picks the row.
			client.GET(`/saranga/api/v1/${kind}` as '/saranga/api/v1/queues', {
				params: {
					query: {
						filter: q.filters.length ? q.filters.map(filterText) : undefined,
						sort: q.sort as Schemas['QueuesField'] | undefined,
						order: q.sort ? q.order : undefined,
						page: q.page,
						size: q.size
					}
				},
				...onNode(node)
			})
		) as Promise<Page<K>>,

	closeConnection: (id: string) =>
		call(
			client.DELETE('/saranga/api/v1/connections/{id}', {
				params: { path: { id }, header: mark }
			})
		),

	closeSession: (connection: string, id: string) =>
		call(
			client.DELETE('/saranga/api/v1/sessions/{connection}/{id}', {
				params: { path: { connection, id }, header: mark }
			})
		),

	closeConsumer: (session: string, id: string) =>
		call(
			client.DELETE('/saranga/api/v1/consumers/{session}/{id}', {
				params: { path: { session, id }, header: mark }
			})
		)
};

export type Node = Schemas['Node'];
export type NodeError = Schemas['NodeError'];
export type ResourceSummary = Schemas['ResourceSummary'];
export type Resource = Schemas['Resource'];
export type Attribute = Schemas['Attribute'];
export type Operation = Schemas['Operation'];
export type AttributeValue = Schemas['AttributeValue'];
export type ResourceType = ResourceSummary['type'];

/** The management resources (MBeans) and their operations. */
export const resources = {
	list: (node?: string) =>
		call(client.GET('/saranga/api/v1/resources', onNode(node))).then((r) => r.data),

	get: (resource: string) =>
		call(client.GET('/saranga/api/v1/resources/{resource}', { params: { path: { resource } } })),

	attributes: (resource: string, names: string[], node?: string) =>
		call(
			client.GET('/saranga/api/v1/resources/{resource}/attributes', {
				params: { path: { resource }, query: { name: names } },
				...onNode(node)
			})
		).then((r) => r.attributes),

	write: (resource: string, name: string, value: unknown) =>
		call(
			client.PUT('/saranga/api/v1/resources/{resource}/attributes/{name}', {
				params: { path: { resource, name }, header: mark },
				body: { value }
			})
		).then((r) => r.previous),

	/**
	 * An operation, by its signature when it is overloaded:
	 * `browse(int,int,java.lang.String)`; on the node shown, or another.
	 */
	invoke: <T = unknown>(
		resource: string,
		operation: string,
		args: unknown[] = [],
		node?: string
	): Promise<T> =>
		call(
			client.POST('/saranga/api/v1/resources/{resource}/operations', {
				params: { path: { resource }, header: mark },
				body: { operation, arguments: args },
				...onNode(node)
			})
		).then((r) => r.result as T)
};

export type View = Schemas['View'];
export type Notification = Schemas['Notification'];
export type ResourceHistory = Schemas['ResourceHistory'];

/** The live data: the stream's views, the history and the notifications. */
export const live = {
	/** The stream, which EventSource opens: it takes no client. */
	eventsUrl: (since: number) => `/saranga/api/v1/events?since=${since}`,

	watch: (stream: string, views: Record<string, View>) =>
		call(
			client.PUT('/saranga/api/v1/events/{stream}', {
				params: { path: { stream }, header: mark },
				body: { views }
			})
		),

	/** The last hour of resources' counters: this broker keeps its own only. */
	history: (resource: string[]) =>
		shown === LOCAL
			? call(client.GET('/saranga/api/v1/history', { params: { query: { resource } } })).then(
					(r) => r.history
				)
			: Promise.resolve<Record<string, Schemas['ResourceHistory']>>({}),

	notifications: (since = 0) =>
		call(client.GET('/saranga/api/v1/notifications', { params: { query: { since } } }))
};

export type AddressSettings = Schemas['AddressSettings'];
export type SecuritySettings = Schemas['SecuritySettings'];
export type Role = Schemas['Role'];

/**
 * What the administration pages read that no operation gives: this broker's
 * address-settings and security-settings matches, and the configuration
 * exported as broker properties.
 */
export const admin = {
	addressSettings: (address?: string) =>
		call(
			client.GET('/saranga/api/v1/address-settings', {
				params: { query: { address: address || undefined } }
			})
		),

	securitySettings: (address?: string) =>
		call(
			client.GET('/saranga/api/v1/security-settings', {
				params: { query: { address: address || undefined } }
			})
		),

	/** exportConfigAsProperties, and the file it wrote: none from another node. */
	exportConfiguration: () =>
		call(client.POST('/saranga/api/v1/configuration/export', { params: { header: mark } })),

	/**
	 * A queue's messages, whole, as JSON text: kept as text, since a long
	 * property is a number JavaScript cannot hold.
	 */
	exportMessages: (queue: string, filter?: string, max?: number) =>
		call(
			client.GET('/saranga/api/v1/queues/{name}/export', {
				params: { path: { name: queue }, query: { filter: filter || undefined, max } },
				parseAs: 'text'
			})
		) as Promise<string>,

	/** Messages as the export writes them, sent to a queue in order: the text as it is. */
	importMessages: (queue: string, text: string) =>
		call(
			client.POST('/saranga/api/v1/queues/{name}/import', {
				params: { path: { name: queue }, header: mark },
				body: text as unknown as Schemas['ExportedMessage'][],
				bodySerializer: (b) => b as unknown as string
			})
		)
};
