// How the tables show the broker's values. The listings write most values as
// strings, as Artemis's views do; numbers and flags are shown as such.

export type Kind = 'number' | 'flag' | 'text';

const INTEGER = /^-?\d+$/;

/** What a column holds, from the values on the page. */
export function kindOf(values: readonly unknown[]): Kind {
	const present = values.filter((v) => v !== '' && v !== null && v !== undefined);
	if (present.length === 0) return 'text';
	if (present.every((v) => typeof v === 'number' || (typeof v === 'string' && INTEGER.test(v))))
		return 'number';
	if (present.every((v) => v === 'true' || v === 'false' || typeof v === 'boolean')) return 'flag';
	return 'text';
}

const grouped = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

export function formatNumber(value: unknown): string {
	const n = typeof value === 'number' ? value : Number(value);
	return Number.isSafeInteger(n) ? grouped.format(n) : String(value);
}

const tenths = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

/** A rate: to a tenth below ten. */
export function formatRate(value: number): string {
	return Math.abs(value) < 10 ? tenths.format(value) : grouped.format(value);
}

const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];

/** Bytes in binary units: 1536 is "1.5 KiB". */
export function formatBytes(value: unknown): string {
	let n = Number(value);
	if (!Number.isFinite(n) || n < 0) return String(value);
	let unit = 0;
	while (n >= 1024 && unit < UNITS.length - 1) {
		n /= 1024;
		unit++;
	}
	return unit === 0 ? `${n} B` : `${n.toFixed(n < 10 ? 1 : 0)} ${UNITS[unit]}`;
}

/** A duration in milliseconds, as "3d 4h", "5m 2s" or "800 ms". */
export function formatDuration(ms: number): string {
	if (!Number.isFinite(ms) || ms < 0) return '';
	if (ms < 1000) return `${Math.round(ms)} ms`;
	const s = Math.floor(ms / 1000);
	const parts: [number, string][] = [
		[Math.floor(s / 86400), 'd'],
		[Math.floor((s % 86400) / 3600), 'h'],
		[Math.floor((s % 3600) / 60), 'm'],
		[s % 60, 's']
	];
	const first = parts.findIndex(([n]) => n > 0);
	return parts
		.slice(first, first + 2)
		.filter(([n]) => n > 0)
		.map(([n, u]) => `${n}${u}`)
		.join(' ');
}
