// The diagram's layout: the brokers of the topology in a column, this one's
// addresses beside it, and each address's queues beside that.

import type { Edge, Node } from '@xyflow/svelte';
import type { TopologyMember } from '#lib/broker.js';

export interface Topology {
	name: string;
	nodeID: string;
	backup: boolean;
	members: TopologyMember[];
	addresses: string[];
	/** The addresses left out. */
	more: number;
	queues: { name: string; address: string; messages: number; consumers: number }[];
}

const ROW = 56;
const COLUMN = 300;

// Inline, as Svelte Flow's own node styles outweigh classes; the theme's
// tokens keep it right in light and dark.
const STYLES: Record<string, string> = {
	self: 'border: 2px solid var(--primary); background: var(--primary); color: var(--primary-foreground)',
	broker: 'border: 2px solid var(--border); background: var(--card); color: var(--card-foreground)',
	backup:
		'border: 2px dashed var(--border); background: var(--muted); color: var(--muted-foreground)',
	address:
		'border: 2px solid var(--chart-4); background: var(--card); color: var(--card-foreground)',
	queue: 'border: 2px solid var(--chart-2); background: var(--card); color: var(--card-foreground)'
};

function node(id: string, kind: string, name: string, label: string, x: number, y: number): Node {
	return {
		id,
		position: { x, y },
		data: { label, kind, name },
		class: `font-mono text-xs ${kind === 'address' || kind === 'queue' || kind === 'self' ? 'cursor-pointer' : ''}`,
		style: `${STYLES[kind] ?? ''}; border-radius: 6px`,
		// Brokers on the left feed addresses on their right.
		sourcePosition: 'right' as Node['sourcePosition'],
		targetPosition: 'left' as Node['targetPosition'],
		width: 220
	};
}

export function layout(
	t: Topology,
	count: (n: number) => string
): { nodes: Node[]; edges: Edge[] } {
	const nodes: Node[] = [];
	const edges: Edge[] = [];
	const edge = (source: string, target: string, dashed = false): Edge => ({
		id: `${source}->${target}`,
		source,
		target,
		style: dashed ? 'stroke-dasharray: 4 4' : undefined
	});

	// The brokers: this one first, then the others of the topology, each
	// with its backup.
	const others = t.members.filter((m) => m.nodeID !== t.nodeID);
	const mine = t.members.find((m) => m.nodeID === t.nodeID);
	nodes.push(node('self', 'self', t.name, `${t.name}${t.backup ? ' (backup)' : ''}`, 0, 0));
	let y = ROW;
	if (mine?.backup) {
		nodes.push(node('self-backup', 'backup', mine.backup, `backup ${mine.backup}`, 0, y));
		edges.push(edge('self', 'self-backup', true));
		y += ROW;
	}
	others.forEach((m, i) => {
		y += ROW / 2;
		const id = `member-${i}`;
		nodes.push(node(id, 'broker', m.nodeID, m.live ?? m.primary ?? m.nodeID, 0, y));
		edges.push(edge('self', id, true));
		y += ROW;
		if (m.backup) {
			nodes.push(node(`${id}-backup`, 'backup', m.backup, `backup ${m.backup}`, 0, y));
			edges.push(edge(id, `${id}-backup`, true));
			y += ROW;
		}
	});

	// The addresses, each in line with its first queue.
	const byAddress = new Map<string, Topology['queues']>();
	for (const q of t.queues) {
		const list = byAddress.get(q.address) ?? [];
		list.push(q);
		byAddress.set(q.address, list);
	}
	let row = 0;
	t.addresses.forEach((a, i) => {
		const id = `address-${i}`;
		nodes.push(node(id, 'address', a, a, COLUMN, row * ROW));
		edges.push(edge('self', id));
		const queues = byAddress.get(a) ?? [];
		queues.forEach((q, j) => {
			const qid = `${id}-queue-${j}`;
			nodes.push(
				node(
					qid,
					'queue',
					q.name,
					`${q.name} · ${count(q.messages)} msg · ${count(q.consumers)} cons`,
					COLUMN * 2,
					(row + j) * ROW
				)
			);
			edges.push(edge(id, qid));
		});
		row += Math.max(1, queues.length);
	});
	return { nodes, edges };
}
