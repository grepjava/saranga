// The user's preferences, kept in this browser.

import { LISTINGS, type ListingInfo } from '#lib/listings.js';
import type { Listing } from '#lib/api/index.js';

const KEY = 'saranga.console.prefs';

interface Stored {
	pageSize: number;
	columns: Partial<Record<Listing, string[]>>;
}

function load(): Stored {
	try {
		const s = JSON.parse(localStorage.getItem(KEY) ?? '{}');
		return {
			pageSize: Number.isInteger(s.pageSize) && s.pageSize > 0 ? s.pageSize : 100,
			columns: typeof s.columns === 'object' && s.columns ? s.columns : {}
		};
	} catch {
		return { pageSize: 100, columns: {} };
	}
}

class Prefs {
	#stored = $state<Stored>(load());

	get pageSize() {
		return this.#stored.pageSize;
	}

	set pageSize(n: number) {
		this.#stored.pageSize = n;
		this.#save();
	}

	/** The columns a listing shows: the user's choice, or its defaults. */
	columns<K extends Listing>(kind: K): string[] {
		const info = LISTINGS[kind] as ListingInfo<K>;
		const chosen = this.#stored.columns[kind]?.filter((c) =>
			(info.columns as readonly string[]).includes(c)
		);
		return chosen?.length ? chosen : [...info.shown];
	}

	setColumns(kind: Listing, columns: string[] | undefined) {
		if (columns) this.#stored.columns[kind] = columns;
		else delete this.#stored.columns[kind];
		this.#save();
	}

	#save() {
		try {
			localStorage.setItem(KEY, JSON.stringify(this.#stored));
		} catch {
			// Storage may be off; the preferences then last until the page closes.
		}
	}
}

export const prefs = new Prefs();
