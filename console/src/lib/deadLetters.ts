// The dead-letter workbench's groups: a dead-letter or expiry queue's
// messages by the queue or the address they came from, or by why they are
// there, each with the filter that selects it, for acting on a group at once.

import type { Message } from '#lib/broker.js';

export type GroupBy = 'queue' | 'address' | 'reason';

export const GROUP_BY: { value: GroupBy; label: string }[] = [
	{ value: 'queue', label: 'Original queue' },
	{ value: 'address', label: 'Original address' },
	{ value: 'reason', label: 'Why' }
];

/** `Message.HDR_ORIGINAL_QUEUE`. */
const ORIGINAL_QUEUE = '_AMQ_ORIG_QUEUE';
/** `Message.HDR_ORIGINAL_ADDRESS`. */
const ORIGINAL_ADDRESS = '_AMQ_ORIG_ADDRESS';
/** `Message.HDR_ACTUAL_EXPIRY_TIME`: set on a message that expired. */
const ACTUAL_EXPIRY = '_AMQ_ACTUAL_EXPIRY';

export interface Group {
	/** What the group's messages share; null when they lack it. */
	key: string | null;
	label: string;
	/** The filter selecting the group's messages on the queue. */
	filter: string;
	count: number;
	/** The earliest and latest times the group's messages were sent. */
	first: number;
	last: number;
}

/** A string in a filter: quoted, a quote doubled. */
export function quoted(s: string): string {
	return `'${s.replaceAll("'", "''")}'`;
}

function propertyGroup(name: string, value: string | null): Pick<Group, 'key' | 'filter'> {
	return {
		key: value,
		filter: value === null ? `${name} IS NULL` : `${name} = ${quoted(value)}`
	};
}

const REASONS = {
	expired: { label: 'Expired', filter: `${ACTUAL_EXPIRY} IS NOT NULL` },
	undelivered: {
		label: 'Not delivered, or sent there by hand',
		filter: `${ACTUAL_EXPIRY} IS NULL`
	}
};

function keyOf(m: Message, by: GroupBy): Pick<Group, 'key' | 'filter' | 'label'> {
	if (by === 'reason') {
		const expired = m.LongProperties?.[ACTUAL_EXPIRY] != null;
		const r = expired ? 'expired' : 'undelivered';
		return { key: r, ...REASONS[r] };
	}
	const name = by === 'queue' ? ORIGINAL_QUEUE : ORIGINAL_ADDRESS;
	const value = m.StringProperties?.[name] ?? null;
	return {
		...propertyGroup(name, value),
		label: value ?? `No ${by === 'queue' ? 'queue' : 'address'}`
	};
}

/** The messages' groups, the largest first. */
export function groups(messages: Message[], by: GroupBy): Group[] {
	const out = new Map<string, Group>();
	for (const m of messages) {
		const k = keyOf(m, by);
		const id = k.key ?? '\u0000';
		const g = out.get(id);
		if (g) {
			g.count++;
			g.first = Math.min(g.first, m.timestamp);
			g.last = Math.max(g.last, m.timestamp);
		} else {
			out.set(id, { ...k, count: 1, first: m.timestamp, last: m.timestamp });
		}
	}
	return [...out.values()].sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}
