// Who is logged in, and what the broker's guard lets them do.

import { api, onUnauthorized, ApiError, currentNode, ALL, LOCAL, type Me } from '#lib/api/index.js';

export type SessionState = 'unknown' | 'out' | 'in';

class Session {
	state = $state<SessionState>('unknown');
	me = $state<Me | null>(null);
	/** Why the session ended, when the broker ended it. */
	ended = $state<string | null>(null);

	constructor() {
		onUnauthorized(() => {
			if (this.state === 'in') this.ended = 'Your session has ended. Log in again.';
			this.state = 'out';
			this.me = null;
		});
	}

	/** Whether the browser still holds a session. */
	async check() {
		try {
			this.me = await api.me();
			this.state = 'in';
		} catch (e) {
			if (!(e instanceof ApiError) || e.status !== 401) throw e;
			this.state = 'out';
		}
	}

	async login(user: string, password: string) {
		this.me = await api.login(user, password);
		this.ended = null;
		this.state = 'in';
	}

	async logout() {
		try {
			await api.logout();
		} finally {
			this.me = null;
			this.state = 'out';
		}
	}

	/**
	 * Whether the guard lets the user call a broker operation. Another node's
	 * guard decides when the operation is called, so the console offers it;
	 * across every node, only the listings are offered.
	 */
	may(operation: string): boolean {
		const node = currentNode();
		const here = this.me?.broker[operation] === true;
		if (node === LOCAL) return here;
		if (node === ALL) return here && operation.startsWith('list');
		return this.me !== null;
	}
}

export const session = new Session();
