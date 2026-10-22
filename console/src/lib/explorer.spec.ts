import { describe, expect, it } from 'vitest';
import { argument, display, inputFor, isInternal, shortName, typeLabel } from './explorer.js';

describe('typeLabel', () => {
	it('names classes by their simple name', () => {
		expect(typeLabel('java.lang.String')).toBe('String');
		expect(typeLabel('long')).toBe('long');
		expect(typeLabel('javax.management.openmbean.CompositeData')).toBe('CompositeData');
	});

	it('names arrays', () => {
		expect(typeLabel('[Ljava.lang.String;')).toBe('String[]');
		expect(typeLabel('[J')).toBe('long[]');
		expect(typeLabel('[[Ljava.util.Map;')).toBe('Map[][]');
	});
});

describe('inputs', () => {
	it('picks an input by type', () => {
		expect(inputFor('boolean')).toBe('boolean');
		expect(inputFor('java.lang.Long')).toBe('number');
		expect(inputFor('java.lang.String')).toBe('text');
		expect(inputFor('java.util.Map')).toBe('json');
		expect(inputFor('[Ljava.lang.String;')).toBe('json');
	});

	it('sends text, booleans and null', () => {
		expect(argument('int', '5')).toBe('5');
		expect(argument('boolean', true)).toBe(true);
		expect(argument('java.lang.Boolean', 'false')).toBe(false);
		expect(argument('java.util.Map', '')).toBeNull();
		expect(argument('java.lang.String', '')).toBe('');
	});
});

describe('display', () => {
	it('shows strings as they are and anything else as JSON', () => {
		expect(display('a')).toBe('a');
		expect(display(5)).toBe('5');
		expect(display(null)).toBe('null');
		expect(display({ a: 1 })).toBe('{"a":1}');
		expect(display({ a: 1 }, true)).toBe('{\n  "a": 1\n}');
	});
});

describe('resources', () => {
	const r = (resource: string, type: 'queue' | 'platform' | 'broker') => ({
		resource,
		type,
		objectName: '',
		className: ''
	});

	it('names them within their group', () => {
		expect(shortName(r('queue.a.b', 'queue'))).toBe('a.b');
		expect(shortName(r('java.lang.Runtime', 'platform'))).toBe('Runtime');
		expect(shortName(r('broker', 'broker'))).toBe('broker');
	});

	it('knows the internal addresses', () => {
		expect(isInternal('activemq.notifications')).toBe(true);
		expect(isInternal('$sys.mqtt.sessions')).toBe(true);
		expect(isInternal('orders')).toBe(false);
	});
});
