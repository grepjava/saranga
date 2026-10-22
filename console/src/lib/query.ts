// A listing's query (its filters, sort and page), as the page's URL carries
// it: `?filter=field:OPERATION:value&sort=field&order=asc&page=1&size=100`,
// the API's own parameters.

export const OPERATIONS = [
	'CONTAINS',
	'NOT_CONTAINS',
	'EQUALS',
	'NOT_EQUALS',
	'GREATER_THAN',
	'LESS_THAN'
] as const;

export type Operation = (typeof OPERATIONS)[number];

export const OPERATION_LABELS: Record<Operation, string> = {
	CONTAINS: 'contains',
	NOT_CONTAINS: 'does not contain',
	EQUALS: 'equals',
	NOT_EQUALS: 'does not equal',
	GREATER_THAN: 'greater than',
	LESS_THAN: 'less than'
};

export interface Filter {
	field: string;
	operation: Operation;
	value: string;
}

export interface ListQuery {
	filters: Filter[];
	sort?: string;
	order: 'asc' | 'desc';
	page: number;
	size: number;
}

export const PAGE_SIZES = [25, 50, 100, 250, 1000];
export const MAX_PAGE_SIZE = 10000;

export function filterText(f: Filter): string {
	return `${f.field}:${f.operation}:${f.value}`;
}

/** `field:OPERATION:value`; the value may hold colons. */
export function parseFilter(text: string): Filter | undefined {
	const first = text.indexOf(':');
	const second = first < 0 ? -1 : text.indexOf(':', first + 1);
	if (second < 0) return undefined;
	const operation = text.slice(first + 1, second) as Operation;
	if (!OPERATIONS.includes(operation)) return undefined;
	return {
		field: text.slice(0, first),
		operation,
		value: text.slice(second + 1)
	};
}

function positive(text: string | null, fallback: number, max = Infinity): number {
	const n = Number(text);
	return Number.isInteger(n) && n >= 1 ? Math.min(n, max) : fallback;
}

export function parseQuery(params: URLSearchParams, size: number): ListQuery {
	const filters = params
		.getAll('filter')
		.map(parseFilter)
		.filter((f): f is Filter => f !== undefined);
	return {
		filters,
		sort: params.get('sort') || undefined,
		order: params.get('order') === 'desc' ? 'desc' : 'asc',
		page: positive(params.get('page'), 1),
		size: positive(params.get('size'), size, MAX_PAGE_SIZE)
	};
}

/** The query as URL parameters, leaving out what is the default. */
export function queryParams(q: ListQuery, size: number): URLSearchParams {
	const p = new URLSearchParams();
	for (const f of q.filters) p.append('filter', filterText(f));
	if (q.sort) {
		p.set('sort', q.sort);
		if (q.order === 'desc') p.set('order', 'desc');
	}
	if (q.page > 1) p.set('page', String(q.page));
	if (q.size !== size) p.set('size', String(q.size));
	return p;
}

/** The number of pages `count` rows fill. */
export function pages(count: number, size: number): number {
	return Math.max(1, Math.ceil(count / size));
}
