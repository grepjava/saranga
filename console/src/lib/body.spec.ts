import { describe, expect, it } from 'vitest';
import { bodyText, dumpBytes, pretty } from './body.js';
import type { Message } from './broker.js';

const message = (m: Partial<Message>): Message => ({
	messageID: '1',
	address: 'a',
	type: 4,
	durable: true,
	expiration: 0,
	timestamp: 0,
	priority: 4,
	redelivered: false,
	largeMessage: false,
	persistentSize: 0,
	protocol: 'CORE',
	...m
});

describe('dumpBytes', () => {
	// "Hi!" and a byte Java writes as -1.
	const bytes = [72, 105, 33, -1];

	it('dumps hex with the text beside it', () => {
		expect(dumpBytes(bytes, 'hex-text')).toBe(`48 69 21 ff${' '.repeat(36)}  Hi!.`);
	});

	it('dumps hex and decimal alone', () => {
		expect(dumpBytes(bytes, 'hex')).toBe('48 69 21 ff');
		expect(dumpBytes(bytes, 'decimal')).toBe(' 72 105  33 255');
	});

	it('writes 16 bytes a line', () => {
		expect(dumpBytes(new Array(17).fill(65), 'hex').split('\n')).toHaveLength(2);
	});

	it('shows text, or nothing', () => {
		expect(dumpBytes(bytes, 'text')).toBe('Hi!.');
		expect(dumpBytes(bytes, 'off')).toBe('');
	});
});

describe('bodyText', () => {
	it('gives a text body whole', () => {
		expect(bodyText(message({ type: 3, text: 'hello' }), 'hex')).toEqual({
			text: 'hello',
			preview: false
		});
	});

	it('gives the preview of any other', () => {
		expect(bodyText(message({ BodyPreview: [65, 66] }), 'text')).toEqual({
			text: 'AB',
			preview: true
		});
	});
});

describe('pretty', () => {
	it('indents JSON', () => {
		expect(pretty('{"a":[1,2]}')).toBe('{\n  "a": [\n    1,\n    2\n  ]\n}');
	});

	it('indents XML, with text on its element’s line', () => {
		expect(pretty('<a><b>x</b><c/></a>')).toBe('<a>\n  <b>x</b>\n  <c/>\n</a>');
	});

	it('leaves anything else', () => {
		expect(pretty('hello')).toBeUndefined();
		expect(pretty('{broken')).toBeUndefined();
	});
});
