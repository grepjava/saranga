<!--
	The cluster: each node of the topology with its status, read live from it
	as the user; this broker's cluster connections; and where the queues'
	messages and consumers are across the nodes, which shows the load
	balancing and redistribution at work.
-->
<script lang="ts">
	import * as Card from '#lib/components/ui/card/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import { resources, type View } from '#lib/api/index.js';
	import { nodes, ALL, LOCAL, type Node } from '#lib/state/nodes.svelte.js';
	import { live, listingPage } from '#lib/state/live.svelte.js';
	import { formatDuration, formatNumber } from '#lib/format.js';
	import { display } from '#lib/explorer.js';
	import { overviewHref, queueHref, explorerHref } from '#lib/nav.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	/** The node whose own parts are shown: this broker across every node. */
	const here = $derived(params.node === ALL ? LOCAL : params.node);

	// Each node's status, as the stream reads it from that node.
	const statusView = (n: Node): View => ({ kind: 'broker', node: n.id });
	$effect(() => {
		const stops = nodes.members.map((n) => live.watch(statusView(n)));
		return () => stops.forEach((s) => s());
	});
	function status(n: Node) {
		const v = live.value(statusView(n));
		const attrs = (v?.value as { attributes?: Record<string, unknown> } | undefined)?.attributes;
		return { attrs, error: v?.error };
	}
	const num = (a: Record<string, unknown> | undefined, k: string) =>
		typeof a?.[k] === 'number' ? (a[k] as number) : undefined;

	// The queues of every node, the fullest first.
	const QUEUES: View = {
		kind: 'listing',
		listing: 'queues',
		node: ALL,
		query: 'sort=messageCount&order=desc&size=500'
	};
	$effect(() => live.watch(QUEUES));
	interface Spread {
		name: string;
		address: string;
		total: number;
		consumers: number;
		on: Record<string, { messages: number; consumers: number }>;
	}
	const spread = $derived.by(() => {
		const page = listingPage<'queues'>(live.value(QUEUES));
		if (!page) return null;
		const byName = new Map<string, Spread>();
		for (const r of page.data) {
			if (r.internalQueue === 'true' || !r.node) continue;
			const s = byName.get(r.name) ?? {
				name: r.name,
				address: r.address,
				total: 0,
				consumers: 0,
				on: {}
			};
			const messages = Number(r.messageCount) || 0;
			const consumers = Number(r.consumerCount) || 0;
			s.total += messages;
			s.consumers += consumers;
			s.on[r.node] = { messages, consumers };
			byName.set(r.name, s);
		}
		return [...byName.values()].sort((a, b) => b.total - a.total || a.name.localeCompare(b.name));
	});
	const errors = $derived(listingPage<'queues'>(live.value(QUEUES))?.errors ?? []);

	// This broker's cluster connections.
	interface Connection {
		resource: string;
		values: Record<string, unknown>;
	}
	let connections = $state<Connection[] | null>(null);
	$effect(() => {
		const node = here;
		resources
			.list(node)
			.then((all) =>
				Promise.all(
					all
						.filter((r) => r.type === 'clusterconnection')
						.map(async (r) => {
							const a = await resources.attributes(
								r.resource,
								[
									'Name',
									'Address',
									'Started',
									'MessageLoadBalancingType',
									'MaxHops',
									'Nodes',
									'MessagesPendingAcknowledgement',
									'MessagesAcknowledged'
								],
								node
							);
							return {
								resource: r.resource,
								values: Object.fromEntries(Object.entries(a).map(([k, v]) => [k, v.value]))
							};
						})
				)
			)
			.then((c) => {
				if (node === here) connections = c;
			})
			.catch(() => (connections = []));
	});
</script>

<svelte:head>
	<title>Nodes · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<h1 class="text-xl font-semibold">Nodes</h1>

	<section class="grid gap-3 md:grid-cols-2 xl:grid-cols-3" aria-label="Nodes">
		{#each nodes.members as n (n.id)}
			{@const s = status(n)}
			<Card.Root class="gap-3 py-4">
				<Card.Header class="px-4">
					<Card.Title class="flex flex-wrap items-center gap-2">
						<a class="hover:underline" href={overviewHref(n.id)}>{n.name}</a>
						{#if n.local}<Badge variant="outline">This broker</Badge>{/if}
						{#if s.error}
							<Badge variant="destructive">Not reachable</Badge>
						{:else if s.attrs?.Backup === true}
							<Badge variant="secondary">Backup</Badge>
						{:else if s.attrs}
							<Badge>Live</Badge>
						{/if}
					</Card.Title>
					<Card.Description class="font-mono text-xs break-all">
						{n.nodeID ?? ''}{n.primary ? ` · ${n.primary}` : ''}
					</Card.Description>
				</Card.Header>
				<Card.Content class="px-4">
					{#if s.error}
						<p class="text-sm text-destructive">{s.error}</p>
					{:else if !s.attrs}
						<Skeleton class="h-16 w-full" />
					{:else}
						<dl class="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
							<dt class="text-muted-foreground">Version</dt>
							<dd>{s.attrs.Version ?? '–'}</dd>
							<dt class="text-muted-foreground">Up</dt>
							<dd>
								{num(s.attrs, 'UptimeMillis') !== undefined
									? formatDuration(num(s.attrs, 'UptimeMillis')!)
									: '–'}
							</dd>
							<dt class="text-muted-foreground">Connections</dt>
							<dd class="tabular-nums">{formatNumber(num(s.attrs, 'ConnectionCount') ?? 0)}</dd>
							<dt class="text-muted-foreground">Consumers</dt>
							<dd class="tabular-nums">{formatNumber(num(s.attrs, 'TotalConsumerCount') ?? 0)}</dd>
							<dt class="text-muted-foreground">Messages</dt>
							<dd class="tabular-nums">{formatNumber(num(s.attrs, 'TotalMessageCount') ?? 0)}</dd>
							<dt class="text-muted-foreground">Address memory</dt>
							<dd class="tabular-nums">
								{num(s.attrs, 'AddressMemoryUsagePercentage') !== undefined
									? `${num(s.attrs, 'AddressMemoryUsagePercentage')} %`
									: '–'}
							</dd>
						</dl>
					{/if}
				</Card.Content>
			</Card.Root>
		{/each}
	</section>

	<section class="flex flex-col gap-2" aria-labelledby="spread">
		<h2 id="spread" class="font-semibold">Queues across the nodes</h2>
		<p class="text-sm text-muted-foreground">
			Messages and consumers of each queue on each node, the fullest first.
		</p>
		{#each errors as e (e.node)}
			<p class="text-sm text-destructive">{nodes.nameOf(e.node)} could not answer: {e.error}</p>
		{/each}
		{#if spread === null}
			<Skeleton class="h-24 w-full" />
		{:else if spread.length === 0}
			<p class="text-sm text-muted-foreground">No queues.</p>
		{:else}
			<div class="overflow-x-auto rounded-md border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Queue</Table.Head>
							<Table.Head>Address</Table.Head>
							<Table.Head class="text-right">Messages</Table.Head>
							<Table.Head class="text-right">Consumers</Table.Head>
							{#each nodes.members as n (n.id)}
								<Table.Head class="text-right">{n.name}</Table.Head>
							{/each}
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each spread.slice(0, 100) as q (q.name)}
							<Table.Row>
								<Table.Cell class="font-medium">{q.name}</Table.Cell>
								<Table.Cell class="text-muted-foreground">{q.address}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{formatNumber(q.total)}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{formatNumber(q.consumers)}</Table.Cell>
								{#each nodes.members as n (n.id)}
									{@const at = n.nodeID ? q.on[n.nodeID] : undefined}
									<Table.Cell class="text-right tabular-nums">
										{#if at}
											<a class="hover:underline" href={queueHref(n.id, q.name)}>
												{formatNumber(at.messages)}
											</a>
											<span class="text-muted-foreground">
												· {at.consumers} consumer{at.consumers === 1 ? '' : 's'}</span
											>
										{:else}
											<span class="text-muted-foreground">–</span>
										{/if}
									</Table.Cell>
								{/each}
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/if}
	</section>

	<section class="flex flex-col gap-2" aria-labelledby="connections">
		<h2 id="connections" class="font-semibold">
			Cluster connections of {nodes.find(here)?.name ?? 'this broker'}
		</h2>
		{#if connections === null}
			<Skeleton class="h-16 w-full" />
		{:else if connections.length === 0}
			<p class="text-sm text-muted-foreground">None.</p>
		{:else}
			<div class="overflow-x-auto rounded-md border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Name</Table.Head>
							<Table.Head>Address</Table.Head>
							<Table.Head>Load balancing</Table.Head>
							<Table.Head class="text-right">Max hops</Table.Head>
							<Table.Head>Bridged to</Table.Head>
							<Table.Head class="text-right">Pending</Table.Head>
							<Table.Head class="text-right">Acknowledged</Table.Head>
							<Table.Head>State</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each connections as c (c.resource)}
							<Table.Row>
								<Table.Cell>
									<a class="hover:underline" href={explorerHref(here, c.resource)}
										>{display(c.values.Name)}</a
									>
								</Table.Cell>
								<Table.Cell>{display(c.values.Address) || '(every address)'}</Table.Cell>
								<Table.Cell>{display(c.values.MessageLoadBalancingType)}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{display(c.values.MaxHops)}</Table.Cell>
								<Table.Cell class="font-mono text-xs">
									{Object.values((c.values.Nodes as Record<string, string>) ?? {}).join(', ') ||
										'–'}
								</Table.Cell>
								<Table.Cell class="text-right tabular-nums">
									{display(c.values.MessagesPendingAcknowledgement)}
								</Table.Cell>
								<Table.Cell class="text-right tabular-nums">
									{display(c.values.MessagesAcknowledged)}
								</Table.Cell>
								<Table.Cell>
									{#if c.values.Started === true}<Badge>Started</Badge>{:else}<Badge
											variant="secondary">Stopped</Badge
										>{/if}
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/if}
	</section>
</div>
