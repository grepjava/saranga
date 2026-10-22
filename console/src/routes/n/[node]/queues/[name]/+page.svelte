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
	import MessageBrowser from '#lib/components/MessageBrowser.svelte';
	import MiniListing from '#lib/components/MiniListing.svelte';
	import AttributeTable from '#lib/components/AttributeTable.svelte';
	import SendMessage from '#lib/components/SendMessage.svelte';
	import { resources, type Resource } from '#lib/api/index.js';
	import { broker, queue as ops, queueResource } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { formatNumber } from '#lib/format.js';
	import { addressHref, explorerHref, listingHref } from '#lib/nav.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { session } from '#lib/state/session.svelte.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	const name = $derived(params.name);
	const resource = $derived(queueResource(name));

	const STATS = [
		'MessageCount',
		'DeliveringCount',
		'ScheduledCount',
		'ConsumerCount',
		'MessagesAdded',
		'MessagesAcknowledged',
		'MessagesExpired',
		'MessagesKilled',
		'Paused',
		'Enabled',
		'Address',
		'RoutingType',
		'Durable',
		'Temporary',
		'Filter',
		'DeadLetterAddress',
		'ExpiryAddress',
		'MaxConsumers'
	];

	let stats = $state<Record<string, unknown> | null>(null);
	let missing = $state<string | null>(null);
	let detail = $state<Resource | null>(null);
	let may = $state<Allowed>({});
	let tick = $state(0);
	let tab = $state('messages');
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
		allowed(resource, [
			'removeAllMessages',
			'pause',
			'resume',
			'enable',
			'disable',
			'resetMessageCounter',
			'retryMessages',
			'sendMessage'
		]).then((a) => (may = a));
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
	const enabled = $derived(s('Enabled') !== false);
	const retryable = $derived(prefs.isRetryQueue(name));

	const counters = $derived([
		['Messages', s('MessageCount')],
		['Delivering', s('DeliveringCount')],
		['Scheduled', s('ScheduledCount')],
		['Consumers', s('ConsumerCount')],
		['Added', s('MessagesAdded')],
		['Acknowledged', s('MessagesAcknowledged')],
		['Expired', s('MessagesExpired')],
		['Killed', s('MessagesKilled')]
	] as [string, unknown][]);

	let sending = $state(false);
	let confirm = $state<'purge' | 'delete' | 'retry' | null>(null);

	async function act(what: string, run: () => Promise<unknown>) {
		try {
			const r = await run();
			toast.success(typeof r === 'number' ? `${what}: ${formatNumber(r)} messages` : what);
		} catch (e) {
			toast.error(`${what} failed`, { description: (e as Error).message });
		}
		tick++;
	}

	async function confirmed() {
		const c = confirm;
		confirm = null;
		if (c === 'purge') await act('Purged', () => ops.removeAll(name));
		else if (c === 'retry') await act('Retried', () => ops.retryAll(name));
		else if (c === 'delete') {
			try {
				await broker.destroyQueue(name, true, false);
				toast.success(`Deleted queue ${name}`);
				goto(listingHref(params.node, 'queues'));
			} catch (e) {
				toast.error('The queue cannot be deleted', { description: (e as Error).message });
			}
		}
	}

	const CONFIRM = {
		purge: {
			title: 'Purge the queue?',
			text: 'Every message on it is deleted for good.',
			action: 'Purge'
		},
		retry: {
			title: 'Retry every message?',
			text: 'Each goes back to the queue it came from.',
			action: 'Retry'
		},
		delete: {
			title: 'Delete the queue?',
			text: 'Its messages are gone for good, and its consumers are closed.',
			action: 'Delete'
		}
	};
</script>

<svelte:head>
	<title>{name} · Queues · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<div class="flex flex-wrap items-start justify-between gap-3">
		<div class="grid gap-1">
			<p class="text-sm text-muted-foreground">
				<a class="hover:underline" href={listingHref(params.node, 'queues')}>Queues</a>
			</p>
			<div class="flex flex-wrap items-center gap-2">
				<h1 class="font-mono text-xl font-semibold break-all">{name}</h1>
				{#if stats}
					<Badge variant="outline">{String(s('RoutingType')).toLowerCase()}</Badge>
					{#if s('Durable') === false}<Badge variant="outline">not durable</Badge>{/if}
					{#if s('Temporary') === true}<Badge variant="outline">temporary</Badge>{/if}
					{#if paused}<Badge variant="secondary">paused</Badge>{/if}
					{#if !enabled}<Badge variant="destructive">disabled</Badge>{/if}
					{#if retryable}<Badge variant="secondary">dead letters</Badge>{/if}
				{/if}
			</div>
			{#if stats}
				<p class="text-sm text-muted-foreground">
					On address
					<a
						class="font-mono text-primary underline-offset-4 hover:underline"
						href={addressHref(params.node, String(s('Address')))}>{s('Address')}</a
					>{#if s('Filter')}, filter <code class="font-mono">{s('Filter')}</code>{/if}
				</p>
			{/if}
		</div>
		<div class="flex items-center gap-2">
			{#if may.sendMessage}
				<Button size="sm" onclick={() => (sending = true)}><SendIcon /> Send</Button>
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
					{#if retryable}
						<DropdownMenu.Item disabled={!may.retryMessages} onSelect={() => (confirm = 'retry')}>
							Retry every message
						</DropdownMenu.Item>
					{/if}
					<DropdownMenu.Item disabled={!may.removeAllMessages} onSelect={() => (confirm = 'purge')}>
						Purge
					</DropdownMenu.Item>
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
					{#if enabled}
						<DropdownMenu.Item
							disabled={!may.disable}
							onSelect={() => act('Disabled', () => ops.disable(name))}
						>
							Disable
						</DropdownMenu.Item>
					{:else}
						<DropdownMenu.Item
							disabled={!may.enable}
							onSelect={() => act('Enabled', () => ops.enable(name))}
						>
							Enable
						</DropdownMenu.Item>
					{/if}
					<DropdownMenu.Item
						disabled={!may.resetMessageCounter}
						onSelect={() => act('Reset the message counter', () => ops.resetCounters(name))}
					>
						Reset the message counter
					</DropdownMenu.Item>
					<DropdownMenu.Item onSelect={() => goto(explorerHref(params.node, resource))}>
						Open in the explorer
					</DropdownMenu.Item>
					<DropdownMenu.Separator />
					<DropdownMenu.Item
						variant="destructive"
						disabled={!session.may('destroyQueue')}
						onSelect={() => (confirm = 'delete')}
					>
						Delete
					</DropdownMenu.Item>
				</DropdownMenu.Content>
			</DropdownMenu.Root>
		</div>
	</div>

	{#if missing}
		<Alert.Root variant="destructive">
			<Alert.Title>The queue cannot be read</Alert.Title>
			<Alert.Description>{missing}</Alert.Description>
		</Alert.Root>
	{/if}

	<section class="grid grid-cols-2 gap-3 sm:grid-cols-4 xl:grid-cols-8" aria-label="Counters">
		{#each counters as [label, value] (label)}
			<Card.Root class="gap-1 py-3">
				<Card.Header class="px-3"><Card.Description>{label}</Card.Description></Card.Header>
				<Card.Content class="px-3">
					{#if !stats}
						<Skeleton class="h-6 w-12" />
					{:else if typeof value === 'number'}
						<span class="text-xl font-semibold tabular-nums">{formatNumber(value)}</span>
					{:else}
						<span class="text-sm text-muted-foreground">Not permitted</span>
					{/if}
				</Card.Content>
			</Card.Root>
		{/each}
	</section>

	<Tabs.Root bind:value={tab}>
		<Tabs.List>
			<Tabs.Trigger value="messages">Messages</Tabs.Trigger>
			<Tabs.Trigger value="consumers">Consumers</Tabs.Trigger>
			<Tabs.Trigger value="attributes">Attributes</Tabs.Trigger>
		</Tabs.List>
		<Tabs.Content value="messages" class="mt-3">
			<MessageBrowser queue={name} refresh={tick} onchange={() => tick++} />
		</Tabs.Content>
		<Tabs.Content value="consumers" class="mt-3 grid gap-2">
			<MiniListing
				node={params.node}
				kind="consumers"
				filter={{ field: 'queue', operation: 'EQUALS', value: name }}
				columns={[
					'id',
					'session',
					'validatedUser',
					'protocol',
					'remoteAddress',
					'filter',
					'messagesInTransit',
					'messagesDelivered',
					'messagesAcknowledged',
					'status'
				]}
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

<SendMessage bind:open={sending} {resource} target={`queue ${name}`} onsent={() => tick++} />

<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
	<AlertDialog.Content>
		{#if confirm}
			<AlertDialog.Header>
				<AlertDialog.Title>{CONFIRM[confirm].title}</AlertDialog.Title>
				<AlertDialog.Description>{CONFIRM[confirm].text}</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action onclick={confirmed}>{CONFIRM[confirm].action}</AlertDialog.Action>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
