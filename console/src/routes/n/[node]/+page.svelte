<script lang="ts">
	import * as Card from '#lib/components/ui/card/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import { formatBytes, formatDuration, formatNumber, formatRate } from '#lib/format.js';
	import { listingHref } from '#lib/nav.js';
	import BrokerParts from '#lib/components/BrokerParts.svelte';
	import LiveChart from '#lib/components/LiveChart.svelte';
	import { live } from '#lib/state/live.svelte.js';
	import type { View } from '#lib/api/index.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	// The status, as the live stream reads it every tick.
	const STATUS: View = { kind: 'broker' };
	$effect(() => live.watch(STATUS));
	const status = $derived(live.value(STATUS));
	const broker = $derived(
		status?.value as { attributes: Record<string, unknown>; denied: string[] } | undefined
	);
	const attributes = $derived(broker?.attributes ?? null);
	const denied = $derived(broker?.denied ?? []);
	const error = $derived(status?.error ?? null);

	const a = (k: string) => attributes?.[k];
	const yes = (k: string) => a(k) === true;
	const num = (k: string) => (typeof a(k) === 'number' ? (a(k) as number) : undefined);

	const memoryPercent = $derived(num('AddressMemoryUsagePercentage'));
	const disk = $derived(num('DiskStoreUsage'));

	const counters = $derived([
		{
			label: 'Connections',
			value: num('ConnectionCount'),
			href: listingHref(params.node, 'connections')
		},
		{
			label: 'Consumers',
			value: num('TotalConsumerCount'),
			href: listingHref(params.node, 'consumers')
		},
		{
			label: 'Messages',
			value: num('TotalMessageCount'),
			href: listingHref(params.node, 'queues')
		},
		{ label: 'Messages added', value: num('TotalMessagesAdded') },
		{ label: 'Messages acknowledged', value: num('TotalMessagesAcknowledged') },
		{ label: 'Connections since start', value: num('TotalConnectionCount') }
	]);

	const facts = $derived([
		['Node ID', a('NodeID')],
		['Version', a('Version')],
		['HA policy', a('HAPolicy')],
		['Journal', a('JournalType')],
		[
			'Persistence',
			a('PersistenceEnabled') === undefined ? undefined : yes('PersistenceEnabled') ? 'On' : 'Off'
		],
		[
			'Security',
			a('SecurityEnabled') === undefined ? undefined : yes('SecurityEnabled') ? 'On' : 'Off'
		],
		['Clustered', a('Clustered') === undefined ? undefined : yes('Clustered') ? 'Yes' : 'No']
	] as [string, unknown][]);
</script>

<svelte:head>
	<title>Overview · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<div class="flex flex-wrap items-center gap-3">
		<h1 class="text-xl font-semibold">{(a('Name') as string) ?? 'Overview'}</h1>
		{#if attributes}
			{#if yes('Backup')}
				<Badge variant="secondary">Backup{yes('ReplicaSync') ? ', in sync' : ''}</Badge>
			{:else if yes('Active')}
				<Badge>Live</Badge>
			{:else if a('Started') === false}
				<Badge variant="destructive">Stopped</Badge>
			{/if}
			{#if num('UptimeMillis') !== undefined}
				<span class="text-sm text-muted-foreground">
					up {formatDuration(num('UptimeMillis')!)}
				</span>
			{/if}
		{/if}
	</div>

	{#if error}
		<Alert.Root variant="destructive"><Alert.Description>{error}</Alert.Description></Alert.Root>
	{/if}

	<section class="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6" aria-label="Counters">
		{#each counters as c (c.label)}
			<Card.Root class="gap-1 py-4">
				<Card.Header class="px-4">
					<Card.Description>{c.label}</Card.Description>
				</Card.Header>
				<Card.Content class="px-4">
					{#if !attributes}
						<Skeleton class="h-7 w-16" />
					{:else if c.value === undefined}
						<span class="text-sm text-muted-foreground">Not permitted</span>
					{:else if c.href}
						<a class="text-2xl font-semibold tabular-nums hover:underline" href={c.href}>
							{formatNumber(c.value)}
						</a>
					{:else}
						<span class="text-2xl font-semibold tabular-nums">{formatNumber(c.value)}</span>
					{/if}
				</Card.Content>
			</Card.Root>
		{/each}
	</section>

	<div class="grid gap-3 lg:grid-cols-2">
		<Card.Root>
			<Card.Header>
				<Card.Title>Memory and disk</Card.Title>
			</Card.Header>
			<Card.Content class="grid gap-4">
				{@render meter(
					'Address memory',
					memoryPercent,
					num('AddressMemoryUsage') !== undefined
						? `${formatBytes(num('AddressMemoryUsage'))}${(num('GlobalMaxSize') ?? -1) > 0 ? ` of ${formatBytes(num('GlobalMaxSize'))}` : ''}`
						: undefined
				)}
				{@render meter(
					'Disk',
					disk === undefined ? undefined : disk * 100,
					num('MaxDiskUsage') !== undefined
						? `blocks producers at ${num('MaxDiskUsage')}%`
						: undefined
				)}
			</Card.Content>
		</Card.Root>
		<Card.Root>
			<Card.Header>
				<Card.Title>Broker</Card.Title>
			</Card.Header>
			<Card.Content>
				<dl class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5 text-sm">
					{#each facts as [k, v] (k)}
						<dt class="text-muted-foreground">{k}</dt>
						<dd class="truncate font-medium" title={v === undefined ? '' : String(v)}>
							{#if !attributes}<Skeleton class="h-4 w-32" />{:else}{v ?? '—'}{/if}
						</dd>
					{/each}
				</dl>
			</Card.Content>
		</Card.Root>
	</div>

	<section class="grid gap-3 lg:grid-cols-2" aria-label="Charts">
		{@render chart('Connections and consumers', 'broker', [
			{ name: 'ConnectionCount', label: 'Connections' },
			{ name: 'TotalConsumerCount', label: 'Consumers' }
		])}
		{@render chart('Messages', 'broker', [{ name: 'TotalMessageCount', label: 'Messages' }])}
		{@render chart(
			'Messages per second',
			'broker',
			[
				{ name: 'TotalMessagesAdded', label: 'Added' },
				{ name: 'TotalMessagesAcknowledged', label: 'Acknowledged' }
			],
			true
		)}
		{@render chart(
			'Address memory',
			'broker',
			[{ name: 'AddressMemoryUsage', label: 'Address memory' }],
			false,
			formatBytes
		)}
	</section>

	<BrokerParts node={params.node} />

	{#if denied.length}
		<p class="text-xs text-muted-foreground">
			Your role may not read: {denied.join(', ')}.
		</p>
	{/if}
</div>

{#snippet chart(
	title: string,
	resource: string,
	series: { name: string; label: string }[],
	rate = false,
	format: (v: number) => string = rate ? formatRate : formatNumber
)}
	<Card.Root class="gap-2 py-4">
		<Card.Header class="px-4">
			<Card.Title class="text-sm">{title}</Card.Title>
		</Card.Header>
		<Card.Content class="px-4">
			<LiveChart {resource} {series} {rate} {format} height={140} />
		</Card.Content>
	</Card.Root>
{/snippet}

{#snippet meter(name: string, percent: number | undefined, detail: string | undefined)}
	<div class="grid gap-1.5">
		<div class="flex items-baseline justify-between text-sm">
			<span class="font-medium">{name}</span>
			<span class="text-muted-foreground tabular-nums">
				{percent === undefined ? '—' : `${percent.toFixed(percent < 10 ? 1 : 0)}%`}
			</span>
		</div>
		<div
			class="h-2 overflow-hidden rounded-full bg-muted"
			role="meter"
			aria-label={name}
			aria-valuemin={0}
			aria-valuemax={100}
			aria-valuenow={percent ?? 0}
		>
			<div
				class="h-full rounded-full bg-primary transition-[width] duration-500"
				class:bg-warning={(percent ?? 0) >= 75}
				class:bg-destructive={(percent ?? 0) >= 90}
				style:width="{Math.min(100, Math.max(0, percent ?? 0))}%"
			></div>
		</div>
		{#if detail}<span class="text-xs text-muted-foreground">{detail}</span>{/if}
	</div>
{/snippet}
