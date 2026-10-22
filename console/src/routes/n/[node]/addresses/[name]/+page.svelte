<script lang="ts">
	import { goto } from '$app/navigation';
	import { toast } from 'svelte-sonner';
	import EllipsisIcon from '@lucide/svelte/icons/ellipsis';
	import SendIcon from '@lucide/svelte/icons/send';
	import * as Card from '#lib/components/ui/card/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import MiniListing from '#lib/components/MiniListing.svelte';
	import AttributeTable from '#lib/components/AttributeTable.svelte';
	import SendMessage from '#lib/components/SendMessage.svelte';
	import CreateQueue from '#lib/components/CreateQueue.svelte';
	import { resources, type Resource } from '#lib/api/index.js';
	import { address as ops, addressResource, broker } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { formatBytes, formatNumber } from '#lib/format.js';
	import { explorerHref, listingHref } from '#lib/nav.js';
	import { session } from '#lib/state/session.svelte.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	const name = $derived(params.name);
	const resource = $derived(addressResource(name));

	const STATS = [
		'QueueCount',
		'MessageCount',
		'RoutedMessageCount',
		'UnRoutedMessageCount',
		'AddressSize',
		'NumberOfPages',
		'AddressLimitPercent',
		'RoutingTypes',
		'Paused',
		'Paging',
		'Internal',
		'Temporary',
		'AutoCreated'
	];

	let stats = $state<Record<string, unknown> | null>(null);
	let missing = $state<string | null>(null);
	let detail = $state<Resource | null>(null);
	let may = $state<Allowed>({});
	let tick = $state(0);
	let tab = $state('queues');
	let filter = $state('');

	$effect(() => {
		const r = resource;
		void tick;
		let stale = false;
		const read = () =>
			resources
				.attributes(r, STATS)
				.then((a) => {
					if (stale) return;
					stats = Object.fromEntries(Object.entries(a).map(([k, v]) => [k, v.value]));
					missing = null;
				})
				.catch((e: Error) => !stale && (missing = e.message));
		read();
		const timer = setInterval(read, 5000);
		return () => {
			stale = true;
			clearInterval(timer);
		};
	});

	$effect(() => {
		allowed(resource, ['sendMessage', 'pause', 'resume']).then((a) => (may = a));
	});

	$effect(() => {
		if (tab !== 'attributes') return;
		const r = resource;
		void tick;
		resources
			.get(r)
			.then((d) => (detail = d))
			.catch(() => (detail = null));
	});

	const s = (k: string) => stats?.[k];
	const paused = $derived(s('Paused') === true);
	const routing = $derived(Array.isArray(s('RoutingTypes')) ? (s('RoutingTypes') as string[]) : []);

	const counters = $derived([
		['Queues', s('QueueCount'), formatNumber],
		['Messages', s('MessageCount'), formatNumber],
		['Routed', s('RoutedMessageCount'), formatNumber],
		['Not routed', s('UnRoutedMessageCount'), formatNumber],
		['Size', s('AddressSize'), formatBytes],
		['Pages', s('NumberOfPages'), formatNumber]
	] as [string, unknown, (v: unknown) => string][]);

	let sending = $state(false);
	let creating = $state(false);
	let deleting = $state(false);

	async function act(what: string, run: () => Promise<unknown>) {
		try {
			await run();
			toast.success(what);
		} catch (e) {
			toast.error(`${what} failed`, { description: (e as Error).message });
		}
		tick++;
	}

	async function remove() {
		deleting = false;
		try {
			await broker.deleteAddress(name, false);
			toast.success(`Deleted address ${name}`);
			goto(listingHref(params.node, 'addresses'));
		} catch (e) {
			toast.error('The address cannot be deleted', { description: (e as Error).message });
		}
	}
</script>

<svelte:head>
	<title>{name} · Addresses · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<div class="flex flex-wrap items-start justify-between gap-3">
		<div class="grid gap-1">
			<p class="text-sm text-muted-foreground">
				<a class="hover:underline" href={listingHref(params.node, 'addresses')}>Addresses</a>
			</p>
			<div class="flex flex-wrap items-center gap-2">
				<h1 class="font-mono text-xl font-semibold break-all">{name}</h1>
				{#each routing as r (r)}<Badge variant="outline">{r.toLowerCase()}</Badge>{/each}
				{#if paused}<Badge variant="secondary">paused</Badge>{/if}
				{#if s('Paging') === true}<Badge variant="secondary">paging</Badge>{/if}
				{#if s('Internal') === true}<Badge variant="outline">internal</Badge>{/if}
				{#if s('Temporary') === true}<Badge variant="outline">temporary</Badge>{/if}
				{#if s('AutoCreated') === true}<Badge variant="outline">auto-created</Badge>{/if}
			</div>
		</div>
		<div class="flex items-center gap-2">
			{#if may.sendMessage}
				<Button size="sm" onclick={() => (sending = true)}><SendIcon /> Send</Button>
			{/if}
			{#if session.may('createQueue')}
				<Button size="sm" variant="outline" onclick={() => (creating = true)}>Create queue</Button>
			{/if}
			<DropdownMenu.Root>
				<DropdownMenu.Trigger>
					{#snippet child({ props })}
						<Button {...props} size="sm" variant="outline" aria-label="More actions">
							<EllipsisIcon />
						</Button>
					{/snippet}
				</DropdownMenu.Trigger>
				<DropdownMenu.Content align="end">
					{#if paused}
						<DropdownMenu.Item
							disabled={!may.resume}
							onSelect={() => act('Resumed', () => ops.resume(name))}
						>
							Resume
						</DropdownMenu.Item>
					{:else}
						<DropdownMenu.Item
							disabled={!may.pause}
							onSelect={() => act('Paused', () => ops.pause(name))}
						>
							Pause
						</DropdownMenu.Item>
					{/if}
					<DropdownMenu.Item onSelect={() => goto(explorerHref(params.node, resource))}>
						Open in the explorer
					</DropdownMenu.Item>
					<DropdownMenu.Separator />
					<DropdownMenu.Item
						variant="destructive"
						disabled={!session.may('deleteAddress')}
						onSelect={() => (deleting = true)}
					>
						Delete
					</DropdownMenu.Item>
				</DropdownMenu.Content>
			</DropdownMenu.Root>
		</div>
	</div>

	{#if missing}
		<Alert.Root variant="destructive">
			<Alert.Title>The address cannot be read</Alert.Title>
			<Alert.Description>{missing}</Alert.Description>
		</Alert.Root>
	{/if}

	<section class="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-6" aria-label="Counters">
		{#each counters as [label, value, format] (label)}
			<Card.Root class="gap-1 py-3">
				<Card.Header class="px-3"><Card.Description>{label}</Card.Description></Card.Header>
				<Card.Content class="px-3">
					{#if !stats}
						<Skeleton class="h-6 w-12" />
					{:else if typeof value === 'number'}
						<span class="text-xl font-semibold tabular-nums">{format(value)}</span>
					{:else}
						<span class="text-sm text-muted-foreground">Not permitted</span>
					{/if}
				</Card.Content>
			</Card.Root>
		{/each}
	</section>

	<Tabs.Root bind:value={tab}>
		<Tabs.List>
			<Tabs.Trigger value="queues">Queues</Tabs.Trigger>
			<Tabs.Trigger value="producers">Producers</Tabs.Trigger>
			<Tabs.Trigger value="attributes">Attributes</Tabs.Trigger>
		</Tabs.List>
		<Tabs.Content value="queues" class="mt-3 grid gap-2">
			<MiniListing
				node={params.node}
				kind="queues"
				filter={{ field: 'address', operation: 'EQUALS', value: name }}
				columns={[
					'name',
					'routingType',
					'filter',
					'messageCount',
					'consumerCount',
					'deliveringCount',
					'messagesAdded',
					'durable',
					'paused'
				]}
				{tick}
			/>
		</Tabs.Content>
		<Tabs.Content value="producers" class="mt-3 grid gap-2">
			<MiniListing
				node={params.node}
				kind="producers"
				filter={{ field: 'address', operation: 'EQUALS', value: name }}
				columns={['id', 'name', 'session', 'validatedUser', 'protocol', 'remoteAddress', 'msgSent']}
				{tick}
			/>
		</Tabs.Content>
		<Tabs.Content value="attributes" class="mt-3 grid gap-3">
			<Input aria-label="Find an attribute" placeholder="Find an attribute" bind:value={filter} />
			{#if detail}
				<AttributeTable
					{resource}
					attributes={detail.attributes}
					{filter}
					onwritten={() => tick++}
				/>
			{:else}
				<Skeleton class="h-40 w-full" />
			{/if}
		</Tabs.Content>
	</Tabs.Root>
</div>

<SendMessage bind:open={sending} {resource} target={`address ${name}`} onsent={() => tick++} />
<CreateQueue bind:open={creating} address={name} oncreated={() => tick++} />

<AlertDialog.Root open={deleting} onOpenChange={(o) => (deleting = o)}>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>Delete the address?</AlertDialog.Title>
			<AlertDialog.Description>
				An address with queues bound to it is not deleted: delete its queues first.
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
			<AlertDialog.Action onclick={remove}>Delete</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
