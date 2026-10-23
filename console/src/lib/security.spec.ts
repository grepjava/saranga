import { describe, expect, it } from 'vitest';
import type { Role } from '#lib/api/index.js';
import { grantedBy, roleList } from './security.js';

const role = (name: string, granted: string[]): Role =>
	({
		name,
		send: false,
		consume: false,
		createDurableQueue: false,
		deleteDurableQueue: false,
		createNonDurableQueue: false,
		deleteNonDurableQueue: false,
		manage: false,
		browse: false,
		createAddress: false,
		deleteAddress: false,
		view: false,
		edit: false,
		...Object.fromEntries(granted.map((g) => [g, true]))
	}) as Role;

describe('grantedBy', () => {
	const roles = [role('amq', ['send', 'consume', 'manage']), role('viewer', ['view', 'browse'])];

	it('names the roles granting each permission', () => {
		const g = grantedBy(roles);
		expect(g.send).toEqual(['amq']);
		expect(g.browse).toEqual(['viewer']);
		expect(g.edit).toEqual([]);
	});

	it('takes only the roles a user holds', () => {
		const g = grantedBy(roles, ['viewer', 'other']);
		expect(g.send).toEqual([]);
		expect(g.view).toEqual(['viewer']);
	});
});

describe('roleList', () => {
	it('splits, trims and drops empty and repeated roles', () => {
		expect(roleList(' amq, viewer,,amq ')).toEqual(['amq', 'viewer']);
		expect(roleList('')).toEqual([]);
	});
});
