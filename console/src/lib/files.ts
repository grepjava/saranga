// Files the console gives the user, and reads from them.

/** Offers text as a file to save. */
export function saveText(name: string, text: string, type = 'application/json') {
	const url = URL.createObjectURL(new Blob([text], { type }));
	const a = document.createElement('a');
	a.href = url;
	a.download = name;
	a.click();
	URL.revokeObjectURL(url);
}

/** A file name for a queue's messages: the queue's name, made safe, and the time. */
export function messagesFile(queue: string, now = new Date()): string {
	const safe = queue.replace(/[^\w.-]+/g, '_');
	const stamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
	return `${safe}-${stamp}.json`;
}

/** How many messages an export's text holds. */
export function messageCount(text: string): number {
	const parsed: unknown = JSON.parse(text);
	return Array.isArray(parsed) ? parsed.length : 0;
}
