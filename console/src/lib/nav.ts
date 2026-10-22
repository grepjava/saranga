// Links within the console. Its routes are in the URL's fragment
// (`#/n/local/queues?filter=...`), so a page's query is the fragment's.

import { resolve } from '$app/paths';
import type { Listing } from '#lib/api/index.js';

/** The query of the fragment. */
export function hashQuery(url: { readonly hash: string }): URLSearchParams {
	const i = url.hash.indexOf('?');
	return new URLSearchParams(i < 0 ? '' : url.hash.slice(i + 1));
}

function withQuery(href: string, params?: URLSearchParams): string {
	const q = params?.toString();
	return q ? `${href}?${q}` : href;
}

export function overviewHref(node: string): string {
	return resolve('/n/[node]', { node });
}

export function listingHref(node: string, kind: Listing, params?: URLSearchParams): string {
	return withQuery(resolve('/n/[node]/[listing]', { node, listing: kind }), params);
}

export function preferencesHref(node: string): string {
	return resolve('/n/[node]/preferences', { node });
}

export function queueHref(node: string, name: string): string {
	return resolve('/n/[node]/queues/[name]', { node, name: encodeURIComponent(name) });
}

export function addressHref(node: string, name: string): string {
	return resolve('/n/[node]/addresses/[name]', { node, name: encodeURIComponent(name) });
}

export function explorerHref(node: string, resource?: string): string {
	const href = resolve('/n/[node]/explorer', { node });
	return resource ? withQuery(href, new URLSearchParams({ r: resource })) : href;
}

export function diagramHref(node: string): string {
	return resolve('/n/[node]/diagram', { node });
}

export function runtimeHref(node: string): string {
	return resolve('/n/[node]/runtime', { node });
}

/**
 * Where a linked value leads: a queue's or an address's name to its page, any
 * other to the listing it names, filtered to it.
 */
export function linkHref(
	node: string,
	link: { kind: Listing; field: string },
	value: string
): string {
	if (link.field === 'name' && link.kind === 'queues') return queueHref(node, value);
	if (link.field === 'name' && link.kind === 'addresses') return addressHref(node, value);
	return listingHref(
		node,
		link.kind,
		new URLSearchParams({ filter: `${link.field}:EQUALS:${value}` })
	);
}
