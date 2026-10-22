// Whether the guard lets the user call methods of a resource, as the
// console asks before it offers an action.

import { api } from '#lib/api/index.js';

export type Allowed = Record<string, boolean>;

/** Each method of `resource`, allowed or not; none on an error. */
export async function allowed(resource: string, methods: readonly string[]): Promise<Allowed> {
	try {
		const results = await api.can(methods.map((method) => ({ resource, method })));
		return Object.fromEntries(methods.map((m, i) => [m, results[i] === true]));
	} catch {
		return {};
	}
}
