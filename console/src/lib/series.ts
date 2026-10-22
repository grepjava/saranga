// Time series for the live charts.

/** Changes per second; nothing where a counter went back (it was reset). */
export function rates(times: number[], values: (number | null)[]): (number | null)[] {
	return values.map((v, i) => {
		const p = values[i - 1];
		if (i === 0 || v === null || p === null || p === undefined) return null;
		const seconds = (times[i] - times[i - 1]) / 1000;
		const d = v - p;
		return seconds > 0 && d >= 0 ? d / seconds : null;
	});
}

/**
 * The history's samples before the first live one, then the live ones: one
 * time line, with a series per name (nothing where a sample is missing).
 */
export function prepend(
	past: { times: number[]; attributes: Record<string, (number | null)[]> },
	names: string[],
	times: number[],
	values: (number | null)[][]
): { times: number[]; values: (number | null)[][] } {
	const first = times[0] ?? Infinity;
	const n = past.times.findIndex((t) => t >= first);
	const upTo = n < 0 ? past.times.length : n;
	return {
		times: [...past.times.slice(0, upTo), ...times],
		values: names.map((name, i) => [
			...(past.attributes[name] ?? Array<null>(upTo).fill(null)).slice(0, upTo),
			...(values[i] ?? [])
		])
	};
}
