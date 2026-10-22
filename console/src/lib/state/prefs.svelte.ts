// The user's preferences, kept in this browser.

import { LISTINGS, type ListingInfo } from '#lib/listings.js';
import type { Listing } from '#lib/api/index.js';
import { isBytesFormat, type BytesFormat } from '#lib/body.js';

const KEY = 'saranga.console.prefs';

interface Stored {
	pageSize: number;
	columns: Partial<Record<Listing, string[]>>;
	/** The queues that hold dead letters and expired messages, as Artemis's console finds them. */
	dlqPattern: string;
	expiryPattern: string;
	bytesFormat: BytesFormat;
	/** The most addresses the diagram draws. */
	diagramAddresses: number;
	/** Whether the diagram and the explorer show the broker's internal addresses. */
	showInternal: boolean;
	/** Whether the listings follow the broker as it changes. */
	liveTables: boolean;
}

export const DEFAULTS: Omit<Stored, 'columns'> = {
	pageSize: 100,
	dlqPattern: '^DLQ$',
	expiryPattern: '^ExpiryQueue$',
	bytesFormat: 'hex-text',
	diagramAddresses: 100,
	showInternal: false,
	liveTables: true
};

const positive = (n: unknown, d: number) =>
	Number.isInteger(n) && (n as number) > 0 ? (n as number) : d;
const text = (s: unknown, d: string) => (typeof s === 'string' ? s : d);

function load(): Stored {
	let s: Record<string, unknown> = {};
	try {
		s = JSON.parse(localStorage.getItem(KEY) ?? '{}') ?? {};
	} catch {
		// Storage may be off, or hold something else.
	}
	return {
		pageSize: positive(s.pageSize, DEFAULTS.pageSize),
		columns: typeof s.columns === 'object' && s.columns ? (s.columns as Stored['columns']) : {},
		dlqPattern: text(s.dlqPattern, DEFAULTS.dlqPattern),
		expiryPattern: text(s.expiryPattern, DEFAULTS.expiryPattern),
		bytesFormat: isBytesFormat(s.bytesFormat) ? s.bytesFormat : DEFAULTS.bytesFormat,
		diagramAddresses: positive(s.diagramAddresses, DEFAULTS.diagramAddresses),
		showInternal: typeof s.showInternal === 'boolean' ? s.showInternal : DEFAULTS.showInternal,
		liveTables: typeof s.liveTables === 'boolean' ? s.liveTables : DEFAULTS.liveTables
	};
}

/** Whether a pattern matches a name; a pattern that is not a regular expression matches nothing. */
function matches(pattern: string, name: string): boolean {
	try {
		return pattern !== '' && new RegExp(pattern).test(name);
	} catch {
		return false;
	}
}

class Prefs {
	#stored = $state<Stored>(load());

	get pageSize() {
		return this.#stored.pageSize;
	}
	set pageSize(n: number) {
		this.#set('pageSize', n);
	}

	get dlqPattern() {
		return this.#stored.dlqPattern;
	}
	set dlqPattern(p: string) {
		this.#set('dlqPattern', p);
	}

	get expiryPattern() {
		return this.#stored.expiryPattern;
	}
	set expiryPattern(p: string) {
		this.#set('expiryPattern', p);
	}

	get bytesFormat() {
		return this.#stored.bytesFormat;
	}
	set bytesFormat(f: BytesFormat) {
		this.#set('bytesFormat', f);
	}

	get diagramAddresses() {
		return this.#stored.diagramAddresses;
	}
	set diagramAddresses(n: number) {
		this.#set('diagramAddresses', n);
	}

	get showInternal() {
		return this.#stored.showInternal;
	}
	set showInternal(b: boolean) {
		this.#set('showInternal', b);
	}

	get liveTables() {
		return this.#stored.liveTables;
	}
	set liveTables(b: boolean) {
		this.#set('liveTables', b);
	}

	/** A queue whose messages can be retried: a dead-letter or an expiry queue. */
	isRetryQueue(name: string): boolean {
		return matches(this.dlqPattern, name) || matches(this.expiryPattern, name);
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

	/** Every preference back to its default. */
	reset() {
		this.#stored = { ...DEFAULTS, columns: {} };
		this.#save();
	}

	#set<K extends keyof Stored>(key: K, value: Stored[K]) {
		this.#stored[key] = value;
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
