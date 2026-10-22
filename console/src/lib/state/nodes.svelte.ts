// The nodes the console can show, and which one it shows. Every page's URL
// carries its node (`#/n/local/queues`); until the console reaches its
// peers, the only node is the one it was loaded from.

import { api } from '#lib/api/index.js';

export interface Node {
	/** The URL's name for it. */
	id: string;
	name: string;
	nodeID?: string;
	version?: string;
	backup?: boolean;
}

export const LOCAL = 'local';

class Nodes {
	list = $state<Node[]>([{ id: LOCAL, name: 'This broker' }]);

	async load() {
		const { attributes } = await api.broker();
		const text = (k: string) =>
			typeof attributes[k] === 'string' ? (attributes[k] as string) : undefined;
		this.list = [
			{
				id: LOCAL,
				name: text('Name') ?? 'This broker',
				nodeID: text('NodeID'),
				version: text('Version'),
				backup: attributes.Backup === true
			}
		];
	}

	find(id: string): Node | undefined {
		return this.list.find((n) => n.id === id);
	}
}

export const nodes = new Nodes();
