// Whether the guard lets the user call methods of a resource, as the
// console asks before it offers an action.

import { api } from '#lib/api/index.js';

export type Allowed = Record<string, boolean>;

/**
 * Each method of `resource`, allowed or not; none on an error. Another node
 * answers null, deciding when the method is called: it is offered.
 */
export async function allowed(
	resource: string,
	methods: readonly string[],
	node?: string
): Promise<Allowed> {
	try {
		const results = await api.can(
			methods.map((method) => ({ resource, method })),
			node
		);
		return Object.fromEntries(methods.map((m, i) => [m, results[i] !== false]));
	} catch {
		return {};
	}
}
