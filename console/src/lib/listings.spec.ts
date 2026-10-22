import { describe, expect, it } from 'vitest';
import { LISTINGS, LISTING_KINDS, isListing, label } from './listings';
import doc from './api/openapi.json';

describe('listings', () => {
	it('offer every member of the rows the broker documents', () => {
		const schemas = doc.components.schemas as Record<string, { properties?: object }>;
		for (const kind of LISTING_KINDS) {
			const name = kind.charAt(0).toUpperCase() + kind.slice(1) + 'Row';
			const members = Object.keys(schemas[name].properties ?? {});
			expect([...LISTINGS[kind].columns].sort(), kind).toEqual(members.sort());
		}
	});

	it('show only their own columns', () => {
		for (const kind of LISTING_KINDS) {
			const info = LISTINGS[kind];
			for (const c of info.shown) expect(info.columns, kind).toContain(c);
		}
	});

	it('link to fields the other listings filter by', () => {
		const views = doc.components.schemas.Views.properties as Record<
			string,
			{ properties: { defaultSort: { const: string } } }
		>;
		expect(Object.keys(views).sort()).toEqual([...LISTING_KINDS].sort());
		const fields = doc.components.schemas as Record<string, { enum?: string[] }>;
		for (const kind of LISTING_KINDS) {
			for (const link of Object.values(LISTINGS[kind].links ?? {})) {
				const target = link!.kind.charAt(0).toUpperCase() + link!.kind.slice(1) + 'Field';
				expect(fields[target].enum, `${kind} → ${link!.kind}`).toContain(link!.field);
			}
		}
	});

	it('are told apart from other paths', () => {
		expect(isListing('queues')).toBe(true);
		expect(isListing('toString')).toBe(false);
	});
});

describe('labels', () => {
	it('spell out the members', () => {
		expect(label('connectionID')).toBe('Connection ID');
		expect(label('id')).toBe('ID');
		expect(label('messageCount')).toBe('Message count');
		expect(label('groupRebalancePauseDispatch')).toBe('Group rebalance pause dispatch');
		expect(label('msgSizeSent')).toBe('Bytes sent');
		expect(label('lastValueKey')).toBe('Last value key');
	});
});
