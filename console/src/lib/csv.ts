// Rows as CSV (RFC 4180), for the tables' export.

function cell(value: unknown): string {
	const text = value === null || value === undefined ? '' : String(value);
	return /[",\r\n]/.test(text) || /^\s|\s$/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function toCsv(
	columns: readonly string[],
	headings: readonly string[],
	rows: readonly Record<string, unknown>[]
): string {
	const lines = [headings.map(cell).join(',')];
	for (const row of rows) lines.push(columns.map((c) => cell(row[c])).join(','));
	return lines.join('\r\n') + '\r\n';
}

/** Hands the text to the browser as a file to save. */
export function download(name: string, text: string, type = 'text/csv;charset=utf-8') {
	const url = URL.createObjectURL(new Blob([text], { type }));
	const a = document.createElement('a');
	a.href = url;
	a.download = name;
	a.click();
	setTimeout(() => URL.revokeObjectURL(url), 0);
}
