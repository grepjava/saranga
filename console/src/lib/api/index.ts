// The console's API, typed from openapi.json (the broker's own document; see
// api_schema.rs). Every call goes through `call`, which turns an error into
// an ApiError and tells the session when the user is no longer logged in.

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

	can: (checks: { resource: string; method: string }[]) =>
		call(client.POST('/saranga/api/v1/me/can', { body: { checks } })).then((r) => r.results),

	views: () => call(client.GET('/saranga/api/v1/views')),

	broker: () => call(client.GET('/saranga/api/v1/broker')),

	list: <K extends Listing>(kind: K, q: ListQuery): Promise<Page<K>> =>
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
				}
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
