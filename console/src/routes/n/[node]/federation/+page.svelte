<!--
	A node's links to other brokers: its broker connections (AMQP, with their
	bridges, federation and mirrors), its core bridges and its cluster
	connections, each with its state and counters, read live, and started or
	stopped.
-->
<script lang="ts">
	import * as Table from '#lib/components/ui/table/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import { toast } from 'svelte-sonner';
	import { resources, type View } from '#lib/api/index.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { display } from '#lib/explorer.js';
	import { explorerHref } from '#lib/nav.js';
	import { live, attributeValues } from '#lib/state/live.svelte.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	interface Kind {
		type: string;
		title: string;
		/** The attributes shown, the first the name. */
		columns: { key: string; label: string; numeric?: boolean }[];
		none: string;
	}
	const KINDS: Kind[] = [
		{
			type: 'brokerconnection',
			title: 'Broker connections',
			columns: [
				{ key: 'Name', label: 'Name' },
				{ key: 'Protocol', label: 'Protocol' },
				{ key: 'Uri', label: 'URI' },
				{ key: 'User', label: 'User' },
				{ key: 'Connected', label: 'Connected' }
			],
			none: 'No broker connection: an <amqp-connection> in broker.xml makes one.'
		},
		{
			type: 'bridge',
			title: 'Core bridges',
			columns: [
				{ key: 'Name', label: 'Name' },
				{ key: 'QueueName', label: 'Queue' },
				{ key: 'ForwardingAddress', label: 'Forwarding address' },
				{ key: 'StaticConnectors', label: 'Connectors' },
				{ key: 'Connected', label: 'Connected' },
				{ key: 'MessagesPendingAcknowledgement', label: 'Pending', numeric: true },
				{ key: 'MessagesAcknowledged', label: 'Acknowledged', numeric: true }
			],
			none: 'No core bridge.'
		},
		{
			type: 'clusterconnection',
			title: 'Cluster connections',
			columns: [
				{ key: 'Name', label: 'Name' },
				{ key: 'Address', label: 'Address' },
				{ key: 'MessageLoadBalancingType', label: 'Load balancing' },
				{ key: 'Topology', label: 'Topology' },
				{ key: 'MessagesPendingAcknowledgement', label: 'Pending', numeric: true },
				{ key: 'MessagesAcknowledged', label: 'Acknowledged', numeric: true }
			],
			none: 'No cluster connection.'
		}
	];

	let listed = $state<Record<string, string[]> | null>(null);
	let error = $state<string | null>(null);
	let may = $state<Record<string, Allowed>>({});
	let tick = $state(0);

	$effect(() => {
		void tick;
		resources
			.list()
			.then((all) => {
				listed = Object.fromEntries(
					KINDS.map((k) => [k.type, all.filter((r) => r.type === k.type).map((r) => r.resource)])
				);
				error = null;
				for (const r of Object.values(listed).flat()) {
					allowed(r, ['start', 'stop']).then((a) => (may[r] = a));
				}
			})
			.catch((e: Error) => (error = e.message));
	});

	// Each one's attributes, as the stream reads them.
	const views = $derived(
		listed
			? KINDS.flatMap((k) =>
					(listed![k.type] ?? []).map((resource): [string, View] => [
						resource,
						{
							kind: 'attributes',
							resource,
							names: [...k.columns.map((c) => c.key), 'Started']
						}
					])
				)
			: []
	);
	$effect(() => {
		const stops = views.map(([, v]) => live.watch(v));
		return () => stops.forEach((s) => s());
	});
	const values = (resource: string) => {
		const v = views.find(([r]) => r === resource)?.[1];
		return v ? attributeValues(live.value(v)) : null;
	};

	async function toggle(resource: string, start: boolean) {
		const what = start ? 'Started' : 'Stopped';
		try {
			await resources.invoke(resource, start ? 'start()' : 'stop()');
			toast.success(`${what} ${resource}`);
			live.refresh();
		} catch (e) {
			toast.error(`${resource} could not be ${what.toLowerCase()}`, {
				description: (e as Error).message
			});
		}
	}

	function cell(v: unknown): string {
		if (Array.isArray(v)) return v.map((x) => display(x)).join(', ');
		if (v === true) return 'Yes';
		if (v === false) return 'No';
		return display(v) || '–';
	}
</script>

<svelte:head>
	<title>Bridges and connections · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<h1 class="text-xl font-semibold">Bridges and connections</h1>
	{#if error}
		<p class="text-sm text-destructive">{error}</p>
	{/if}

	{#each KINDS as k (k.type)}
		<section class="flex flex-col gap-2" aria-labelledby={k.type}>
			<h2 id={k.type} class="font-semibold">{k.title}</h2>
			{#if listed === null}
				{#if !error}<Skeleton class="h-12 w-full" />{/if}
			{:else if !listed[k.type]?.length}
				<p class="text-sm text-muted-foreground">{k.none}</p>
			{:else}
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								{#each k.columns as c (c.key)}
									<Table.Head class={c.numeric ? 'text-right' : ''}>{c.label}</Table.Head>
								{/each}
								<Table.Head>State</Table.Head>
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each listed[k.type] as resource (resource)}
								{@const v = values(resource)}
								<Table.Row>
									{#each k.columns as c, i (c.key)}
										<Table.Cell
											class="{c.numeric ? 'text-right tabular-nums' : ''} {c.key === 'Topology'
												? 'max-w-80 truncate font-mono text-xs'
												: ''}"
										>
											{#if !v}
												<Skeleton class="h-4 w-16" />
											{:else if i === 0}
												<a class="hover:underline" href={explorerHref(params.node, resource)}
													>{cell(v[c.key])}</a
												>
											{:else}
												{cell(v[c.key])}
											{/if}
										</Table.Cell>
									{/each}
									<Table.Cell>
										{#if v?.Started === true}<Badge>Started</Badge>{:else if v}<Badge
												variant="secondary">Stopped</Badge
											>{/if}
									</Table.Cell>
									<Table.Cell class="text-right">
										{#if v?.Started === true}
											<Button
												size="xs"
												variant="outline"
												disabled={may[resource]?.stop === false}
												onclick={() => toggle(resource, false)}>Stop</Button
											>
										{:else if v}
											<Button
												size="xs"
												variant="outline"
												disabled={may[resource]?.start === false}
												onclick={() => toggle(resource, true)}>Start</Button
											>
										{/if}
									</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</section>
	{/each}
</div>
