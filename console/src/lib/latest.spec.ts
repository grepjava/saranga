import { describe, expect, it } from 'vitest';
import { Latest } from './latest.js';

/** A send that finishes when the test says so, recording what was sent. */
function deferred() {
	const sent: string[] = [];
	const finish: (() => void)[] = [];
	let inFlight = 0;
	let most = 0;
	const send = (v: string) => {
		sent.push(v);
		inFlight++;
		most = Math.max(most, inFlight);
		return new Promise<void>((resolve) => {
			finish.push(() => {
				inFlight--;
				resolve();
			});
		});
	};
	return { sent, finish, send, most: () => most };
}

const settle = () => new Promise((r) => setTimeout(r, 0));

describe('Latest', () => {
	it('sends one at a time, and only the latest of those given meanwhile', async () => {
		const d = deferred();
		const latest = new Latest(d.send);
		latest.push('first sort');
		latest.push('second sort');
		latest.push('third sort');
		expect(d.sent).toEqual(['first sort']);
		d.finish.shift()!();
		await settle();
		expect(d.sent).toEqual(['first sort', 'third sort']);
		d.finish.shift()!();
		await settle();
		expect(d.sent).toEqual(['first sort', 'third sort']);
		expect(d.most()).toBe(1);
	});

	it('goes on after a send that failed', async () => {
		const sent: string[] = [];
		let fail = true;
		const latest = new Latest((v: string) => {
			sent.push(v);
			return fail ? Promise.reject(new Error('gone')) : Promise.resolve();
		});
		latest.push('a');
		latest.push('b');
		fail = false;
		await settle();
		await settle();
		expect(sent).toEqual(['a', 'b']);
	});
});
