<!--
	Creates a queue (`createQueue(String queueConfiguration, boolean)`), with
	the settings Artemis's console offers.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import * as Collapsible from '#lib/components/ui/collapsible/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { broker, type QueueSettings } from '#lib/broker.js';

	let {
		open = $bindable(false),
		address: fixedAddress,
		oncreated
	}: {
		open?: boolean;
		/** The address it is bound to, when it is given. */
		address?: string;
		oncreated?: (name: string) => void;
	} = $props();

	const fresh = (): QueueSettings => ({
		name: '',
		address: fixedAddress ?? '',
		routingType: 'ANYCAST',
		durable: true,
		filter: '',
		maxConsumers: -1,
		purgeOnNoConsumers: false,
		exclusive: false,
		lastValue: false,
		lastValueKey: '',
		nonDestructive: false,
		groupRebalance: false,
		groupBuckets: -1,
		consumersBeforeDispatch: 0,
		delayBeforeDispatch: -1,
		autoCreateAddress: true
	});

	let s = $state<QueueSettings>(fresh());
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		if (!open) return;
		s = fresh();
		error = null;
	});

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		busy = true;
		error = null;
		const settings = { ...s, name: s.name.trim(), address: s.address.trim() || s.name.trim() };
		try {
			await broker.createQueue(settings);
			toast.success(`Created queue ${settings.name}`, {
				description: `On address ${settings.address}`
			});
			oncreated?.(settings.name);
			open = false;
		} catch (err) {
			error = (err as Error).message;
		} finally {
			busy = false;
		}
	}

	const select =
		'h-8 w-full rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30';
</script>

{#snippet number(
	id: string,
	label: string,
	key: 'maxConsumers' | 'groupBuckets' | 'consumersBeforeDispatch' | 'delayBeforeDispatch'
)}
	<div class="grid gap-1.5">
		<Label for={id}>{label}</Label>
		<Input
			{id}
			type="number"
			value={s[key]}
			oninput={(e) => (s[key] = Number(e.currentTarget.value))}
		/>
	</div>
{/snippet}

<Dialog.Root bind:open>
	<Dialog.Content class="max-h-[90vh] overflow-y-auto sm:max-w-lg">
		<Dialog.Header>
			<Dialog.Title>Create a queue</Dialog.Title>
			<Dialog.Description>
				{fixedAddress
					? `Bound to address ${fixedAddress}.`
					: 'On an address, created if it is not there.'}
			</Dialog.Description>
		</Dialog.Header>
		<form id="create-queue" class="grid gap-4" onsubmit={submit}>
			<div class="grid gap-3 sm:grid-cols-2">
				<div class="grid gap-1.5">
					<Label for="queue-name">Name</Label>
					<Input id="queue-name" required autocomplete="off" bind:value={s.name} />
				</div>
				<div class="grid gap-1.5">
					<Label for="queue-address">Address</Label>
					<Input
						id="queue-address"
						autocomplete="off"
						placeholder={s.name || 'The queue’s name'}
						disabled={fixedAddress !== undefined}
						bind:value={s.address}
					/>
				</div>
				<div class="grid gap-1.5">
					<Label for="queue-routing">Routing</Label>
					<select id="queue-routing" class={select} bind:value={s.routingType}>
						<option value="ANYCAST">Anycast</option>
						<option value="MULTICAST">Multicast</option>
					</select>
				</div>
				<div class="grid gap-1.5">
					<Label for="queue-filter">Filter</Label>
					<Input
						id="queue-filter"
						autocomplete="off"
						placeholder="color = 'red'"
						class="font-mono"
						bind:value={s.filter}
					/>
				</div>
			</div>
			<div class="flex flex-wrap gap-x-6 gap-y-3">
				<Label class="font-normal"><Checkbox bind:checked={s.durable} /> Durable</Label>
				<Label class="font-normal">
					<Checkbox bind:checked={s.purgeOnNoConsumers} /> Purge when the last consumer goes
				</Label>
				{#if fixedAddress === undefined}
					<Label class="font-normal">
						<Checkbox bind:checked={s.autoCreateAddress} /> Create the address
					</Label>
				{/if}
			</div>
			<Collapsible.Root>
				<Collapsible.Trigger
					class="group flex items-center gap-1 text-sm font-medium text-muted-foreground hover:text-foreground"
				>
					<ChevronRightIcon class="size-4 transition-transform group-data-[state=open]:rotate-90" />
					More settings
				</Collapsible.Trigger>
				<Collapsible.Content class="mt-3 grid gap-4">
					<div class="grid gap-3 sm:grid-cols-2">
						{@render number('queue-max-consumers', 'Most consumers (-1: any)', 'maxConsumers')}
						{@render number('queue-group-buckets', 'Group buckets (-1: none)', 'groupBuckets')}
						{@render number(
							'queue-consumers-before-dispatch',
							'Consumers before dispatch',
							'consumersBeforeDispatch'
						)}
						{@render number(
							'queue-delay-before-dispatch',
							'Delay before dispatch (ms)',
							'delayBeforeDispatch'
						)}
						<div class="grid gap-1.5">
							<Label for="queue-last-value-key">Last-value key</Label>
							<Input
								id="queue-last-value-key"
								autocomplete="off"
								disabled={!s.lastValue}
								bind:value={s.lastValueKey}
							/>
						</div>
					</div>
					<div class="flex flex-wrap gap-x-6 gap-y-3">
						<Label class="font-normal"><Checkbox bind:checked={s.exclusive} /> Exclusive</Label>
						<Label class="font-normal"><Checkbox bind:checked={s.lastValue} /> Last value</Label>
						<Label class="font-normal">
							<Checkbox bind:checked={s.nonDestructive} /> Non-destructive
						</Label>
						<Label class="font-normal">
							<Checkbox bind:checked={s.groupRebalance} /> Rebalance groups
						</Label>
					</div>
				</Collapsible.Content>
			</Collapsible.Root>
			{#if error}<p class="text-sm text-destructive" role="alert">{error}</p>{/if}
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button type="submit" form="create-queue" disabled={busy || !s.name.trim()}>Create</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
