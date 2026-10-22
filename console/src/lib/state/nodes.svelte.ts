// The nodes the console can show, and which one it shows. Every page's URL
// carries its node (`#/n/local/queues`): this broker (`local`), another node
// of its cluster by its node ID, or every node (`all`), which the listings
// and the cluster's pages span.

import { api, ALL, LOCAL } from '#lib/api/index.js';

export { ALL, LOCAL };

export interface Node {
	/** The URL's name for it. */
	id: string;
	name: string;
	nodeID?: string;
	version?: string;
	backup?: boolean;
	/** This broker. */
	local?: boolean;
	/** The connector of its primary, `host:port`. */
	primary?: string | null;
	/** The connector of its backup, when it has one. */
	backupAt?: string | null;
	backupGroup?: string | null;
	clusters?: string[];
	/** Why it could not be asked, when it could not. */
	error?: string;
}

class Nodes {
	list = $state<Node[]>([{ id: LOCAL, name: 'This broker', local: true }]);
	/** The topology was read. */
	loaded = $state(false);

	/** The topology, then each node's name and version, as it answers. */
	async load() {
		const topology = await api.nodes();
		const members: Node[] = topology.data.map((n) => ({
			id: n.local ? LOCAL : n.nodeID,
			name: n.local ? 'This broker' : n.nodeID.slice(0, 8),
			nodeID: n.nodeID,
			local: n.local,
			primary: n.primary,
			backupAt: n.backup,
			backupGroup: n.backupGroup,
			clusters: n.clusters
		}));
		const every: Node[] = members.length > 1 ? [{ id: ALL, name: 'Every node' }] : [];
		this.list = [...members, ...every];
		this.loaded = true;
		await Promise.all(members.map((m) => this.#describe(m.id)));
	}

	async #describe(id: string) {
		let update: Partial<Node>;
		try {
			const { attributes } = await api.broker(id);
			const text = (k: string) =>
				typeof attributes[k] === 'string' ? (attributes[k] as string) : undefined;
			update = {
				name: text('Name'),
				version: text('Version'),
				backup: attributes.Backup === true,
				error: undefined
			};
		} catch (e) {
			update = { error: (e as Error).message };
		}
		const n = this.list.find((x) => x.id === id);
		if (!n) return;
		if (update.name !== undefined) n.name = update.name;
		n.version = update.version ?? n.version;
		n.backup = update.backup ?? n.backup;
		n.error = update.error;
	}

	find(id: string): Node | undefined {
		return this.list.find((n) => n.id === id);
	}

	/** The brokers of the cluster, without `all`. */
	get members(): Node[] {
		return this.list.filter((n) => n.id !== ALL);
	}

	/** A node's name, from a node ID as rows across every node give it. */
	nameOf(nodeID: string): string {
		return this.list.find((n) => n.nodeID === nodeID)?.name ?? nodeID.slice(0, 8);
	}

	/** The URL's name for a node ID. */
	idOf(nodeID: string): string {
		return this.list.find((n) => n.nodeID === nodeID)?.id ?? nodeID;
	}
}

export const nodes = new Nodes();
