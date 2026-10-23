// What the security settings grant: the security page's permission checker.

import type { Role } from '#lib/api/index.js';
import { PERMISSIONS, type Permission } from '#lib/broker.js';

const LABELS: Record<Permission, string> = {
	send: 'Send',
	consume: 'Consume',
	createDurableQueue: 'Create durable queue',
	deleteDurableQueue: 'Delete durable queue',
	createNonDurableQueue: 'Create non-durable queue',
	deleteNonDurableQueue: 'Delete non-durable queue',
	manage: 'Manage',
	browse: 'Browse',
	createAddress: 'Create address',
	deleteAddress: 'Delete address',
	view: 'View',
	edit: 'Edit'
};

export function permissionLabel(p: Permission): string {
	return LABELS[p];
}

/**
 * For each permission, the roles that grant it, of the roles given: of
 * `held` only, when it names the roles a user holds.
 */
export function grantedBy(roles: Role[], held?: string[]): Record<Permission, string[]> {
	const of = held ? roles.filter((r) => held.includes(r.name)) : roles;
	return Object.fromEntries(
		PERMISSIONS.map((p) => [p, of.filter((r) => r[p] === true).map((r) => r.name)])
	) as Record<Permission, string[]>;
}

/** A comma-separated list of roles, as a user's are typed. */
export function roleList(text: string): string[] {
	return [
		...new Set(
			text
				.split(',')
				.map((r) => r.trim())
				.filter(Boolean)
		)
	];
}
