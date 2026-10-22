<script lang="ts">
	import { goto } from '$app/navigation';
	import { mode } from 'mode-watcher';
	import { SvelteFlow, Background, Controls, MiniMap, type Node, type Edge } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Switch } from '#lib/components/ui/switch/index.js';
	import { api } from '#lib/api/index.js';
	import { broker, type TopologyMember } from '#lib/broker.js';
	import { isInternal } from '#lib/explorer.js';
	import { formatNumber } from '#lib/format.js';
	import { addressHref, overviewHref, queueHref } from '#lib/nav.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { layout, type Topology } from '#lib/diagram.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	let topology = $state<Topology | null>(null);
	let error = $state<string | null>(null);
	let tick = $state(0);

	$effect(() => {
		void tick;
		const most = prefs.diagramAddresses;
		const internal = prefs.showInternal;
		let stale = false;
		Promise.all([
			api.broker(),
			broker.topology().catch(() => [] as TopologyMember[]),
			api.list('addresses', { filters: [], sort: 'name', order: 'asc', page: 1, size: 10000 }),
			api.list('queues', { filters: [], sort: 'name', order: 'asc', page: 1, size: 10000 })
		])
			.then(([b, members, addresses, queues]) => {
				if (stale) return;
				const visible = addresses.data.map((a) => a.name).filter((a) => internal || !isInternal(a));
				const shown = visible.slice(0, most);
				topology = {
					name: String(b.attributes.Name ?? 'broker'),
					nodeID: String(b.attributes.NodeID ?? ''),
					backup: b.attributes.Backup === true,
					members,
					addresses: shown,
					more: visible.length - shown.length,
					queues: queues.data.map((q) => ({
						name: q.name,
						address: q.address,
						messages: Number(q.messageCount),
						consumers: Number(q.consumerCount)
					}))
				};
				error = null;
			})
			.catch((e: Error) => !stale && (error = e.message));
		return () => {
			stale = true;
		};
	});

	let nodes = $state.raw<Node[]>([]);
	let edges = $state.raw<Edge[]>([]);

	$effect(() => {
		if (!topology) return;
		const l = layout(topology, formatNumber);
		nodes = l.nodes;
		edges = l.edges;
	});

	function open(node: Node) {
		const [kind, name] = [node.data.kind as string, node.data.name as string];
		if (kind === 'address') goto(addressHref(params.node, name));
		else if (kind === 'queue') goto(queueHref(params.node, name));
		else if (kind === 'self') goto(overviewHref(params.node));
	}
</script>

<svelte:head>
	<title>Diagram · Saranga console</title>
</svelte:head>

<div class="flex h-[calc(100vh-7rem)] flex-col gap-3">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<h1 class="text-xl font-semibold">Diagram</h1>
		<div class="flex items-center gap-4">
			<Label class="font-normal">
				<Switch checked={prefs.showInternal} onCheckedChange={(c) => (prefs.showInternal = c)} />
				Internal addresses
			</Label>
			<Button variant="ghost" size="icon-sm" aria-label="Refresh" onclick={() => tick++}>
				<RefreshCwIcon />
			</Button>
		</div>
	</div>
	{#if error}
		<Alert.Root variant="destructive"><Alert.Description>{error}</Alert.Description></Alert.Root>
	{/if}
	{#if topology && topology.more > 0}
		<p class="text-sm text-muted-foreground">
			The first {formatNumber(topology.addresses.length)} addresses; the preferences set how many.
		</p>
	{/if}
	<div class="min-h-80 flex-1 overflow-hidden rounded-lg border" aria-label="The broker's topology">
		<SvelteFlow
			bind:nodes
			bind:edges
			fitView
			colorMode={mode.current ?? 'light'}
			nodesConnectable={false}
			nodesDraggable={true}
			elementsSelectable={false}
			onnodeclick={({ node }) => open(node)}
			proOptions={{ hideAttribution: true }}
		>
			<Background />
			<Controls showLock={false} />
			<MiniMap pannable zoomable />
		</SvelteFlow>
	</div>
</div>
