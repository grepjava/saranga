// The live stream (GET /events), one per tab while the user is logged in.
// Pages watch views through it: the broker reads each one every tick, as the
// user, and sends it when it changes. The stream also brings each
// management notification the user may see.

import {
	live as api,
	type Listing,
	type Notification,
	type Page,
	type View
} from '#lib/api/index.js';
import { queryParams, type ListQuery } from '#lib/query.js';
import { session } from './session.svelte.js';

/** A view's value, or why it could not be read. */
export interface ViewValue {
	value?: unknown;
	error?: string;
	status?: number;
}

/** The notifications the browser keeps. */
export const KEPT = 1000;

/** How long to wait before opening a stream the broker closed. */
const RETRY_MS = 5000;

export function viewKey(view: View): string {
	return JSON.stringify(view);
}

class Live {
	/** Whether the stream is open. */
	connected = $state(false);
	/** Whether the user may see notifications. */
	notifying = $state(false);
	/** The notifications heard, oldest first. */
	notes = $state.raw<Notification[]>([]);
	/** How many came since the feed was last looked at. */
	unseen = $state(0);
	/** When the broker last read the views, in milliseconds. */
	time = $state(0);
	/** How often it reads them. */
	interval = $state(2000);
	/** The notification address, which a notification not about an address names. */
	address = $state('activemq.notifications');

	#values = $state<Record<string, ViewValue>>({});
	#views = new Map<string, { view: View; count: number }>();
	#ticks = new Set<(time: number) => void>();
	#source: EventSource | null = null;
	#stream: string | null = null;
	#seq = 0;
	/** The last notification the broker had when the stream opened: those it replays are not new. */
	#old = 0;
	#pending: ReturnType<typeof setTimeout> | undefined;
	#retry: ReturnType<typeof setTimeout> | undefined;
	#running = false;

	/** Opens the stream, which first replays the notifications the broker kept. */
	start() {
		if (this.#running) return;
		this.#running = true;
		this.#open();
	}

	stop() {
		this.#running = false;
		clearTimeout(this.#retry);
		this.#close();
		this.notes = [];
		this.unseen = 0;
		this.#seq = 0;
		this.#old = 0;
		this.#values = {};
	}

	#open() {
		const s = new EventSource(api.eventsUrl(this.#seq));
		this.#source = s;
		s.addEventListener('hello', (e) => {
			const hello = JSON.parse(e.data) as {
				stream: string;
				interval: number;
				notifications: boolean;
				address: string;
				last: number;
			};
			if (this.#seq === 0) this.#old = hello.last;
			this.#stream = hello.stream;
			this.interval = hello.interval;
			this.notifying = hello.notifications;
			this.address = hello.address;
			this.connected = true;
			this.#send();
		});
		s.addEventListener('stats', (e) => {
			const stats = JSON.parse(e.data) as { time: number; views: Record<string, ViewValue> };
			for (const [k, v] of Object.entries(stats.views)) {
				if (this.#views.has(k)) this.#values[k] = v;
			}
			this.time = stats.time;
			for (const f of this.#ticks) f(stats.time);
		});
		s.addEventListener('notification', (e) => {
			const n = JSON.parse(e.data) as Notification;
			if (n.seq <= this.#seq) return;
			this.#seq = n.seq;
			const notes = this.notes.length >= KEPT ? this.notes.slice(1) : this.notes;
			this.notes = [...notes, n];
			if (n.seq > this.#old) this.unseen++;
		});
		s.addEventListener('end', () => {
			this.#close();
			// The session is over: the login page takes it from here.
			session.check().catch(() => {});
		});
		s.onerror = () => {
			this.connected = false;
			// The browser comes back by itself unless the broker refused the stream.
			if (s.readyState !== EventSource.CLOSED) return;
			this.#close();
			this.#retry = setTimeout(async () => {
				await session.check().catch(() => {});
				if (this.#running && session.state === 'in') this.#open();
			}, RETRY_MS);
		};
	}

	#close() {
		this.#source?.close();
		this.#source = null;
		this.#stream = null;
		this.connected = false;
	}

	/** Tells the broker what the stream watches, once the calls of this turn are made. */
	#schedule() {
		clearTimeout(this.#pending);
		this.#pending = setTimeout(() => this.#send(), 20);
	}

	#send() {
		const stream = this.#stream;
		if (!stream) return;
		const views = Object.fromEntries([...this.#views].map(([k, w]) => [k, w.view]));
		api.watch(stream, views).catch(() => {
			// A stream that is gone comes back with a hello, which sends them again.
		});
	}

	/**
	 * Watches a view until the function it returns is called; a page calls it
	 * in an effect: `$effect(() => live.watch(view))`.
	 */
	watch(view: View): () => void {
		const key = viewKey(view);
		const w = this.#views.get(key);
		if (w) w.count++;
		else {
			this.#views.set(key, { view, count: 1 });
			this.#schedule();
		}
		let done = false;
		return () => {
			if (done) return;
			done = true;
			const w = this.#views.get(key);
			if (!w || --w.count > 0) return;
			this.#views.delete(key);
			delete this.#values[key];
			this.#schedule();
		};
	}

	/** A watched view's last value. */
	value(view: View): ViewValue | undefined {
		return this.#values[viewKey(view)];
	}

	/** Every view read again now, after something changed them. */
	refresh() {
		this.#schedule();
	}

	/** Calls `f` with the time of each tick, after its values are in; the function returned stops it. */
	onTick(f: (time: number) => void): () => void {
		this.#ticks.add(f);
		return () => this.#ticks.delete(f);
	}

	/** The feed was looked at. */
	seen() {
		this.unseen = 0;
	}
}

export const live = new Live();

/** A value of an attributes view: the attribute's value, when it could be read. */
export function attributeValues(v: ViewValue | undefined): Record<string, unknown> | null {
	if (!v || v.value === undefined) return null;
	const attrs = v.value as Record<string, { value?: unknown }>;
	return Object.fromEntries(Object.entries(attrs).map(([k, a]) => [k, a.value]));
}

/** The view of one page of a listing. */
export function listingView(listing: Listing, q: ListQuery): View {
	// A size of 0 is never the default, so the page's size is always there.
	return { kind: 'listing', listing, query: queryParams(q, 0).toString() };
}

/** A listing view's page, when it could be read. */
export function listingPage<K extends Listing>(v: ViewValue | undefined): Page<K> | null {
	return v && v.value !== undefined ? (v.value as Page<K>) : null;
}
