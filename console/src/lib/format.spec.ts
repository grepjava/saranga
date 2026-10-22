import { describe, expect, it } from 'vitest';
import { formatBytes, formatDuration, kindOf } from './format';

describe('formats', () => {
	it('tell numbers and flags from text', () => {
		expect(kindOf(['1', '20', '', '-3'])).toBe('number');
		expect(kindOf([1, 2])).toBe('number');
		expect(kindOf(['true', 'false'])).toBe('flag');
		expect(kindOf(['1', 'x'])).toBe('text');
		expect(kindOf(['', ''])).toBe('text');
	});

	it('show bytes in binary units', () => {
		expect(formatBytes(0)).toBe('0 B');
		expect(formatBytes('1536')).toBe('1.5 KiB');
		expect(formatBytes(10 * 1024 * 1024)).toBe('10 MiB');
		expect(formatBytes('n/a')).toBe('n/a');
	});

	it('show durations by their two largest units', () => {
		expect(formatDuration(800)).toBe('800 ms');
		expect(formatDuration(62_000)).toBe('1m 2s');
		expect(formatDuration(3 * 86_400_000 + 4 * 3_600_000 + 5000)).toBe('3d 4h');
		expect(formatDuration(3_600_000)).toBe('1h');
	});
});
