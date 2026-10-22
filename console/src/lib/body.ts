// How a message's body reads: a text body as it is, and the bytes of any
// other in the format the preferences choose, as Artemis's console offers.

import type { Message } from '#lib/broker.js';

export const BYTES_FORMATS = {
	'hex-text': 'Hex and text',
	hex: 'Hex',
	'decimal-text': 'Decimal and text',
	decimal: 'Decimal',
	text: 'Text',
	off: 'Off'
} as const;

export type BytesFormat = keyof typeof BYTES_FORMATS;

export function isBytesFormat(f: unknown): f is BytesFormat {
	return typeof f === 'string' && f in BYTES_FORMATS;
}

const printable = (b: number) => (b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.');

/** Bytes, which Java writes as signed, in a format: 16 a line for the dumps. */
export function dumpBytes(bytes: readonly number[], format: BytesFormat): string {
	const u = bytes.map((b) => b & 0xff);
	if (format === 'off') return '';
	if (format === 'text') return u.map(printable).join('');
	const width = format.startsWith('hex') ? 2 : 3;
	const num = (b: number) =>
		format.startsWith('hex') ? b.toString(16).padStart(2, '0') : String(b).padStart(3, ' ');
	const withText = format.endsWith('-text');
	const lines: string[] = [];
	for (let i = 0; i < u.length; i += 16) {
		const row = u.slice(i, i + 16);
		const numbers = row.map(num).join(' ');
		lines.push(
			withText ? `${numbers.padEnd(16 * (width + 1) - 1)}  ${row.map(printable).join('')}` : numbers
		);
	}
	return lines.join('\n');
}

/** A message's body as it reads, and whether it is the whole of it. */
export function bodyText(m: Message, format: BytesFormat): { text: string; preview: boolean } {
	if (typeof m.text === 'string') return { text: m.text, preview: false };
	const bytes = m.BodyPreview ?? [];
	return { text: dumpBytes(bytes, format), preview: true };
}

/** The body indented, when it is JSON or XML. */
export function pretty(text: string): string | undefined {
	const t = text.trim();
	if (t.startsWith('{') || t.startsWith('[')) {
		try {
			return JSON.stringify(JSON.parse(t), null, 2);
		} catch {
			return undefined;
		}
	}
	if (t.startsWith('<')) {
		const tokens = t.match(/<[^>]+>|[^<]+/g) ?? [];
		const opens = (s: string) => /^<[^/!?]/.test(s) && !s.endsWith('/>');
		const out: string[] = [];
		let depth = 0;
		for (let i = 0; i < tokens.length; i++) {
			const s = tokens[i].trim();
			if (!s) continue;
			// An element holding only text stays on one line.
			if (opens(s) && !tokens[i + 1]?.startsWith('<') && tokens[i + 2]?.startsWith('</')) {
				out.push('  '.repeat(depth) + s + tokens[i + 1].trim() + tokens[i + 2]);
				i += 2;
				continue;
			}
			if (s.startsWith('</')) depth = Math.max(0, depth - 1);
			out.push('  '.repeat(depth) + s);
			if (opens(s)) depth++;
		}
		return out.join('\n');
	}
	return undefined;
}
