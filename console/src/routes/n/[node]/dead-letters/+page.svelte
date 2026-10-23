<!--
	The dead-letter workbench: a dead-letter or expiry queue's messages,
	grouped by the queue or the address they came from, or by why they are
	there, each group retried, moved, exported or deleted at once by the
	filter that selects it.
-->
<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { toast } from 'svelte-sonner';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import QueuePicker from '#lib/components/QueuePicker.svelte';
	import { admin, resources } from '#lib/api/index.js';
	import { matching, queue as ops, queueResource, type Message } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { GROUP_BY, groups, type Group, type GroupBy } from '#lib/deadLetters.js';
	import { messageCount, messagesFile, saveText } from '#lib/files.js';
	import { formatNumber } from '#lib/format.js';
	import { deadLettersHref, hashQuery, queueHref } from '#lib/nav.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { cn } from '#lib/utils.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	/** The most messages read to group. */
	const MAX_READ = 5_000;
	const PAGE = 500;

	const query = $derived(hashQuery(page.url));
	const chosen = $derived(query.get('queue') ?? '');
	const by = $derived<GroupBy>(GROUP_BY.find((g) => g.value === query.get('by'))?.value ?? 'queue');
	function set(queue: string, groupBy: GroupBy) {
		const q = new URLSearchParams();
		if (queue) q.set('queue', queue);
		if (groupBy !== 'queue') q.set('by', groupBy);
		goto(deadLettersHref(params.node, q), { replaceState: true });
	}

	// The queues whose messages can be retried, as the preferences name them.
	let queues = $state<string[] | null>(null);
	$effect(() => {
		resources
			.list()
			.then((all) => {
				queues = all
					.filter((r) => r.type === 'queue')
					.map((r) => r.resource.slice('queue.'.length))
					.filter((n) => prefs.isRetryQueue(n))
					.sort();
				if (!chosen && queues.length) set(queues[0], by);
			})
			.catch(() => (queues = []));
	});

	let messages = $state<Message[] | null>(null);
	let total = $state(0);
	let loading = $state(false);
	let error = $state<string | null>(null);
	let tick = $state(0);
	let may = $state<Allowed>({});

	$effect(() => {
		const q = chosen;
		if (!q) return;
		allowed(queueResource(q), ['browse', 'retryMessages', 'moveMessages', 'removeMessages']).then(
			(a) => (may = a)
		);
	});

	$effect(() => {
		const q = chosen;
		void tick;
		if (!q) return;
		let stale = false;
		loading = true;
		(async () => {
			const count = await ops.count(q, '');
			const read: Message[] = [];
			for (let p = 1; read.length < Math.min(count, MAX_READ); p++) {
				const got = await ops.browse(q, p, PAGE, '');
				read.push(...got);
				if (got.length < PAGE || stale) break;
			}
			return { count, read: read.slice(0, MAX_READ) };
		})()
			.then(({ count, read }) => {
				if (stale) return;
				messages = read;
				total = count;
				error = null;
			})
			.catch((e: Error) => {
				if (!stale) error = e.message;
			})
			.finally(() => {
				if (!stale) loading = false;
			});
		return () => {
			stale = true;
		};
	});

	const grouped = $derived(messages ? groups(messages, by) : []);

	async function retry(g: Group | null) {
		try {
			const n = g ? await matching.retry(chosen, g.filter) : await ops.retryAll(chosen);
			toast.success(`Retried ${formatNumber(n)} message${n === 1 ? '' : 's'}`, {
				description: g ? g.label : undefined
			});
		} catch (e) {
			toast.error('They could not be retried', { description: (e as Error).message });
		}
		tick++;
	}

	let moving = $state<Group | null>(null);
	async function move(to: string) {
		const g = moving;
		moving = null;
		if (!g) return;
		try {
			const n = await matching.move(chosen, g.filter, to);
			toast.success(`Moved ${formatNumber(n)} message${n === 1 ? '' : 's'} to ${to}`);
		} catch (e) {
			toast.error('They could not be moved', { description: (e as Error).message });
		}
		tick++;
	}

	let removing = $state<Group | null>(null);
	async function remove() {
		const g = removing;
		removing = null;
		if (!g) return;
		try {
			const n = await matching.remove(chosen, g.filter);
			toast.success(`Deleted ${formatNumber(n)} message${n === 1 ? '' : 's'}`);
		} catch (e) {
			toast.error('They could not be deleted', { description: (e as Error).message });
		}
		tick++;
	}

	async function exportGroup(g: Group | null) {
		try {
			const text = await admin.exportMessages(chosen, g?.filter);
			saveText(messagesFile(chosen), text);
			const n = messageCount(text);
			toast.success(`Exported ${formatNumber(n)} message${n === 1 ? '' : 's'}`);
		} catch (e) {
			toast.error('They could not be exported', { description: (e as Error).message });
		}
	}

	const select = 'h-8 rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30';
</script>

<svelte:head>
	<title>Dead letters · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	<h1 class="text-xl font-semibold">Dead letters</h1>

	<div class="flex flex-wrap items-end gap-3">
		<div class="grid gap-1.5">
			<Label for="dead-letter-queue">Queue</Label>
			{#if queues === null}
				<Skeleton class="h-8 w-48" />
			{:else}
				<select
					id="dead-letter-queue"
					class={select}
					value={chosen}
					onchange={(e) => set(e.currentTarget.value, by)}
				>
					{#if chosen && !queues.includes(chosen)}<option value={chosen}>{chosen}</option>{/if}
					{#each queues as q (q)}<option value={q}>{q}</option>{/each}
				</select>
			{/if}
		</div>
		<div class="grid gap-1.5">
			<Label for="group-by">Group by</Label>
			<select
				id="group-by"
				class={select}
				value={by}
				onchange={(e) => set(chosen, e.currentTarget.value as GroupBy)}
			>
				{#each GROUP_BY as g (g.value)}<option value={g.value}>{g.label}</option>{/each}
			</select>
		</div>
		<Button
			variant="ghost"
			size="icon-sm"
			aria-label="Refresh"
			disabled={!chosen}
			onclick={() => tick++}
		>
			<RefreshCwIcon class={cn(loading && 'animate-spin')} />
		</Button>
		{#if chosen}
			<div class="ml-auto flex gap-2">
				<Button size="sm" variant="outline" disabled={!total} onclick={() => exportGroup(null)}
					>Export all</Button
				>
				<Button
					size="sm"
					disabled={!total || may.retryMessages === false}
					onclick={() => retry(null)}>Retry all</Button
				>
			</div>
		{/if}
	</div>

	{#if queues?.length === 0 && !chosen}
		<p class="text-sm text-muted-foreground">
			No queue matches the dead-letter or expiry pattern of the preferences.
		</p>
	{/if}
	{#if error}<p class="text-sm text-destructive" role="alert">{error}</p>{/if}

	{#if chosen && messages}
		<p class="text-sm text-muted-foreground">
			{formatNumber(total)} message{total === 1 ? '' : 's'} on
			<a class="hover:underline" href={queueHref(params.node, chosen)}>{chosen}</a>{total >
			messages.length
				? `; the groups are of the first ${formatNumber(messages.length)} browsed, the actions apply to all`
				: ''}.
		</p>
		<div class="overflow-x-auto rounded-md border">
			<Table.Root aria-label="Groups">
				<Table.Header>
					<Table.Row>
						<Table.Head>{GROUP_BY.find((g) => g.value === by)?.label}</Table.Head>
						<Table.Head class="text-right">Messages</Table.Head>
						<Table.Head>First sent</Table.Head>
						<Table.Head>Last sent</Table.Head>
						<Table.Head><span class="sr-only">Actions</span></Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each grouped as g (g.filter)}
						<Table.Row>
							<Table.Cell class={cn(g.key !== null && by !== 'reason' && 'font-mono')}>
								{g.label}
							</Table.Cell>
							<Table.Cell class="text-right tabular-nums">{formatNumber(g.count)}</Table.Cell>
							<Table.Cell class="whitespace-nowrap">
								{g.first ? new Date(g.first).toLocaleString() : ''}
							</Table.Cell>
							<Table.Cell class="whitespace-nowrap">
								{g.last ? new Date(g.last).toLocaleString() : ''}
							</Table.Cell>
							<Table.Cell class="text-right whitespace-nowrap">
								<Button
									size="xs"
									variant="outline"
									disabled={may.retryMessages === false}
									onclick={() => retry(g)}>Retry</Button
								>
								<Button
									size="xs"
									variant="outline"
									disabled={may.moveMessages === false}
									onclick={() => (moving = g)}>Move</Button
								>
								<Button size="xs" variant="outline" onclick={() => exportGroup(g)}>Export</Button>
								<Button
									size="xs"
									variant="destructive"
									disabled={may.removeMessages === false}
									onclick={() => (removing = g)}>Delete</Button
								>
							</Table.Cell>
						</Table.Row>
					{:else}
						<Table.Row>
							<Table.Cell colspan={5} class="py-8 text-center text-muted-foreground">
								{loading ? 'Loading…' : 'No message: nothing has failed.'}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	{:else if chosen && !error}
		<Skeleton class="h-32 w-full" />
	{/if}
</div>

<QueuePicker
	title="Move to a queue"
	description={`The messages of ${moving?.label ?? ''} on ${chosen}.`}
	action="Move"
	exclude={chosen}
	onpick={move}
	bind:open={() => moving !== null, (o) => !o && (moving = null)}
/>

<AlertDialog.Root open={removing !== null} onOpenChange={(o) => !o && (removing = null)}>
	<AlertDialog.Content>
		{#if removing}
			<AlertDialog.Header>
				<AlertDialog.Title>Delete the messages of {removing.label}?</AlertDialog.Title>
				<AlertDialog.Description>
					Every message on {chosen} that <code>{removing.filter}</code> selects is acknowledged and gone
					for good.
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action
					class="bg-destructive text-white hover:bg-destructive/90"
					onclick={remove}>Delete</AlertDialog.Action
				>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
