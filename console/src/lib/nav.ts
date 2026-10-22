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
