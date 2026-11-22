/**
 * Sends values one at a time, always the latest: a value given while a send
 * is in flight waits for it, and replaces any value that was waiting. So a
 * request that replaces the whole of what it sets (a live stream's watched
 * views) never arrives after a newer one.
 */
export class Latest<T> {
	#send: (value: T) => Promise<unknown>;
	#busy = false;
	#waiting: { value: T } | null = null;

	constructor(send: (value: T) => Promise<unknown>) {
		this.#send = send;
	}

	/** Sends `value` now, or once the send in flight is done. */
	push(value: T) {
		if (this.#busy) {
			this.#waiting = { value };
			return;
		}
		this.#busy = true;
		this.#send(value)
			.catch(() => {})
			.finally(() => {
				this.#busy = false;
				const next = this.#waiting;
				this.#waiting = null;
				if (next) this.push(next.value);
			});
	}
}
