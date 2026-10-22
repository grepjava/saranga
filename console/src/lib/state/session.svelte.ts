// Who is logged in, and what the broker's guard lets them do.

import { api, onUnauthorized, ApiError, type Me } from '#lib/api/index.js';

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

	/** Whether the guard lets the user call a broker operation. */
	may(operation: string): boolean {
		return this.me?.broker[operation] === true;
	}
}

export const session = new Session();
