<!--
	A queue's messages that browsing does not show: those scheduled for later
	(`listScheduledMessagesAsJSON`), which can be delivered now, and those out
	with its consumers (`listDeliveringMessagesAsJSON`), by consumer.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import { pending, queueResource } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';

	let {
		queue,
		kind,
		tick = 0,
		onchange
	}: {
		queue: string;
		kind: 'scheduled' | 'delivering';
		tick?: number;
		onchange?: () => void;
	} = $props();

	type Row = Record<string, unknown>;

	let groups = $state<{ consumer: string | null; messages: Row[] }[] | null>(null);
	let error = $state<string | null>(null);
	let may = $state<Allowed>({});
	let again = $state(0);

	$effect(() => {
		allowed(queueResource(queue), ['deliverScheduledMessage', 'deliverScheduledMessages']).then(
			(a) => (may = a)
		);
	});
	$effect(() => {
		const q = queue;
		void tick;
		void again;
		(kind === 'scheduled'
			? pending.scheduled(q).then((m) => [{ consumer: null, messages: m }])
			: pending
					.delivering(q)
					.then((by) => Object.entries(by).map(([consumer, messages]) => ({ consumer, messages })))
		)
			.then((g) => ((groups = g), (error = null)))
			.catch((e: Error) => (error = e.message));
	});

	const SCHEDULED = '_AMQ_SCHED_DELIVERY';
	const time = (v: unknown) => (typeof v === 'number' && v > 0 ? new Date(v).toLocaleString() : '');

	async function deliver(id?: string) {
		try {
			await (id ? pending.deliverScheduled(queue, id) : pending.deliverAllScheduled(queue, ''));
			toast.success(id ? `Delivered message ${id} now` : 'Delivered every scheduled message now');
		} catch (e) {
			toast.error('They could not be delivered', { description: (e as Error).message });
		}
		again++;
		onchange?.();
	}

	const total = $derived(groups?.reduce((n, g) => n + g.messages.length, 0) ?? 0);
</script>

{#if error}
	<p class="text-sm text-destructive" role="alert">{error}</p>
{:else if !groups}
	<Skeleton class="h-24 w-full" />
{:else if !total}
	<p class="text-sm text-muted-foreground">
		{kind === 'scheduled'
			? 'No message is scheduled for later.'
			: 'No message is out with a consumer.'}
	</p>
{:else}
	<div class="flex flex-col gap-3">
		{#if kind === 'scheduled'}
			<div class="flex justify-end">
				<Button
					size="sm"
					variant="outline"
					disabled={may.deliverScheduledMessages === false}
					onclick={() => deliver()}>Deliver all now</Button
				>
			</div>
		{/if}
		{#each groups as g (g.consumer)}
			{#if g.consumer}<h3 class="font-mono text-sm">{g.consumer}</h3>{/if}
			<div class="overflow-x-auto rounded-md border">
				<Table.Root aria-label={g.consumer ?? 'Scheduled messages'}>
					<Table.Header>
						<Table.Row>
							<Table.Head>ID</Table.Head>
							<Table.Head class="text-right">Priority</Table.Head>
							<Table.Head>Sent</Table.Head>
							{#if kind === 'scheduled'}<Table.Head>Delivered at</Table.Head>{/if}
							<Table.Head>Durable</Table.Head>
							{#if kind === 'scheduled'}
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							{/if}
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each g.messages as m (String(m.messageID))}
							<Table.Row>
								<Table.Cell class="font-mono">{String(m.messageID)}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{String(m.priority ?? '')}</Table.Cell>
								<Table.Cell class="whitespace-nowrap">{time(m.timestamp)}</Table.Cell>
								{#if kind === 'scheduled'}
									<Table.Cell class="whitespace-nowrap">{time(m[SCHEDULED])}</Table.Cell>
								{/if}
								<Table.Cell>{m.durable ? 'yes' : 'no'}</Table.Cell>
								{#if kind === 'scheduled'}
									<Table.Cell class="text-right">
										<Button
											size="xs"
											variant="outline"
											disabled={may.deliverScheduledMessage === false}
											onclick={() => deliver(String(m.messageID))}>Deliver now</Button
										>
									</Table.Cell>
								{/if}
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/each}
	</div>
{/if}
