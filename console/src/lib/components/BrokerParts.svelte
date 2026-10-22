<!--
	What Artemis's status page lists beside the broker's status: its
	acceptors, its cluster connections, its lock coordinators, and how many
	live and backup brokers the topology has.
-->
<script lang="ts">
	import * as Card from '#lib/components/ui/card/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { resources } from '#lib/api/index.js';
	import { broker, type TopologyMember } from '#lib/broker.js';
	import { display } from '#lib/explorer.js';
	import { formatNumber } from '#lib/format.js';
	import { explorerHref } from '#lib/nav.js';

	let { node }: { node: string } = $props();

	interface Part {
		resource: string;
		values: Record<string, unknown>;
	}

	let acceptors = $state<Part[] | null>(null);
	let clusters = $state<Part[] | null>(null);
	let locks = $state<Record<string, unknown>[] | null>(null);
	let topology = $state<TopologyMember[] | null>(null);

	async function read(resource: string, names: string[]): Promise<Part> {
		const a = await resources.attributes(resource, names);
		return {
			resource,
			values: Object.fromEntries(Object.entries(a).map(([k, v]) => [k, v.value]))
		};
	}

	$effect(() => {
		resources
			.list()
			.then(async (all) => {
				const of = (type: string) => all.filter((r) => r.type === type).map((r) => r.resource);
				acceptors = await Promise.all(
					of('acceptor').map((r) => read(r, ['Name', 'Started', 'Parameters']))
				);
				clusters = await Promise.all(
					of('clusterconnection').map((r) =>
						read(r, ['Name', 'Address', 'Started', 'MessageLoadBalancingType', 'MaxHops', 'Nodes'])
					)
				);
			})
			.catch(() => {
				acceptors = [];
				clusters = [];
			});
		resources
			.invoke<string>('broker', 'listLockCoordinatorsAsJSON()')
			.then((j) => (locks = JSON.parse(j)))
			.catch(() => (locks = null));
		broker
			.topology()
			.then((t) => (topology = t))
			.catch(() => (topology = null));
	});

	const param = (p: unknown, k: string) =>
		p && typeof p === 'object' ? ((p as Record<string, string>)[k] ?? '') : '';
	const lives = $derived(topology?.length ?? 0);
	const backups = $derived(topology?.filter((m) => m.backup).length ?? 0);
	const lockColumns = $derived(locks?.length ? Object.keys(locks[0]) : []);
</script>

<div class="grid gap-4 xl:grid-cols-2">
	<Card.Root>
		<Card.Header><Card.Title>Acceptors</Card.Title></Card.Header>
		<Card.Content class="overflow-x-auto">
			{#if acceptors === null}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if acceptors.length === 0}
				<p class="text-sm text-muted-foreground">None the user may see.</p>
			{:else}
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Name</Table.Head>
							<Table.Head>Listens on</Table.Head>
							<Table.Head>Protocols</Table.Head>
							<Table.Head>State</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each acceptors as a (a.resource)}
							<Table.Row>
								<Table.Cell>
									<a
										class="text-primary underline-offset-4 hover:underline"
										href={explorerHref(node, a.resource)}>{a.values.Name ?? a.resource}</a
									>
								</Table.Cell>
								<Table.Cell class="font-mono text-xs">
									{param(a.values.Parameters, 'host')}:{param(a.values.Parameters, 'port')}
								</Table.Cell>
								<Table.Cell class="text-xs"
									>{param(a.values.Parameters, 'protocols') || 'all'}</Table.Cell
								>
								<Table.Cell>
									{#if a.values.Started === true}<Badge variant="secondary">started</Badge>
									{:else if a.values.Started === false}<Badge variant="destructive">stopped</Badge
										>{/if}
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			{/if}
		</Card.Content>
	</Card.Root>

	<Card.Root>
		<Card.Header>
			<Card.Title>Cluster</Card.Title>
			{#if topology}
				<Card.Description>
					{formatNumber(lives)} live, {formatNumber(backups)} backup{backups === 1 ? '' : 's'} in the
					topology
				</Card.Description>
			{/if}
		</Card.Header>
		<Card.Content class="grid gap-4 overflow-x-auto">
			{#if clusters === null}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if clusters.length === 0}
				<p class="text-sm text-muted-foreground">No cluster connection.</p>
			{:else}
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Cluster connection</Table.Head>
							<Table.Head>Address</Table.Head>
							<Table.Head>Load balancing</Table.Head>
							<Table.Head class="text-right">Nodes</Table.Head>
							<Table.Head>State</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each clusters as c (c.resource)}
							<Table.Row>
								<Table.Cell>
									<a
										class="text-primary underline-offset-4 hover:underline"
										href={explorerHref(node, c.resource)}>{c.values.Name ?? c.resource}</a
									>
								</Table.Cell>
								<Table.Cell class="font-mono text-xs">{c.values.Address ?? ''}</Table.Cell>
								<Table.Cell class="text-xs">{c.values.MessageLoadBalancingType ?? ''}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">
									{c.values.Nodes && typeof c.values.Nodes === 'object'
										? Object.keys(c.values.Nodes).length
										: ''}
								</Table.Cell>
								<Table.Cell>
									{#if c.values.Started === true}<Badge variant="secondary">started</Badge>
									{:else if c.values.Started === false}<Badge variant="destructive">stopped</Badge
										>{/if}
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			{/if}
			{#if locks && locks.length}
				<Table.Root>
					<Table.Header>
						<Table.Row>
							{#each lockColumns as k (k)}<Table.Head>{k}</Table.Head>{/each}
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each locks as l, i (i)}
							<Table.Row>
								{#each lockColumns as k (k)}<Table.Cell class="text-xs">{display(l[k])}</Table.Cell
									>{/each}
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			{:else if locks}
				<p class="text-sm text-muted-foreground">No lock coordinator.</p>
			{/if}
		</Card.Content>
	</Card.Root>
</div>
