<!--
	A queue's messages, as Artemis's console browses them (`browse(page, size,
	filter)` and `countMessages(filter)`): a page at a time, filtered by a
	selector, with the actions the guard allows on a selection or on one
	message.
-->
<script lang="ts">
	import { SvelteSet } from 'svelte/reactivity';
	import { toast } from 'svelte-sonner';
	import ChevronLeftIcon from '@lucide/svelte/icons/chevron-left';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import * as Sheet from '#lib/components/ui/sheet/index.js';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import MessageView from './MessageView.svelte';
	import QueuePicker from './QueuePicker.svelte';
	import SendMessage, { type Draft } from './SendMessage.svelte';
	import {
		MESSAGE_TYPES,
		addressResource,
		originalQueue,
		queue as ops,
		queueResource,
		type Message
	} from '#lib/broker.js';
	import { bodyText } from '#lib/body.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { formatBytes, formatNumber } from '#lib/format.js';
	import { PAGE_SIZES, pages } from '#lib/query.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { cn } from '#lib/utils.js';

	let {
		queue,
		refresh: outer = 0,
		onchange
	}: {
		queue: string;
		/** Changed to read the messages again, as after a send from the page. */
		refresh?: number;
		/** Messages were moved, removed or sent: the queue's counts changed. */
		onchange?: () => void;
	} = $props();

	const METHODS = [
		'browse',
		'countMessages',
		'removeMessage',
		'moveMessage',
		'copyMessage',
		'retryMessage',
		'expireMessage',
		'sendMessageToDeadLetterAddress'
	] as const;

	let may = $state<Allowed>({});
	let messages = $state<Message[]>([]);
	let count = $state(0);
	let page = $state(1);
	let size = $state(Math.min(prefs.pageSize, 200));
	let filter = $state('');
	let typed = $state('');
	let loading = $state(false);
	let error = $state<string | null>(null);
	let tick = $state(0);
	const selected = new SvelteSet<string>();
	let viewing = $state<Message | null>(null);

	const retryable = $derived(prefs.isRetryQueue(queue));
	const last = $derived(pages(count, size));

	$effect(() => {
		const q = queue;
		allowed(queueResource(q), METHODS).then((a) => (may = a));
	});

	$effect(() => {
		const q = queue;
		const p = page;
		const s = size;
		const f = filter;
		void tick;
		void outer;
		let stale = false;
		loading = true;
		Promise.all([ops.browse(q, p, s, f), ops.count(q, f)])
			.then(([m, c]) => {
				if (stale) return;
				messages = m;
				count = c;
				error = null;
				const shown = new Set(m.map((x) => x.messageID));
				for (const id of selected) if (!shown.has(id)) selected.delete(id);
			})
			.catch((e: Error) => {
				if (stale) return;
				messages = [];
				error = e.message;
			})
			.finally(() => {
				if (!stale) loading = false;
			});
		return () => {
			stale = true;
		};
	});

	function refresh() {
		tick++;
		onchange?.();
	}

	type Action = 'remove' | 'move' | 'copy' | 'retry' | 'expire' | 'toDeadLetter';

	const DONE: Record<Action, string> = {
		remove: 'Deleted',
		move: 'Moved',
		copy: 'Copied',
		retry: 'Retried',
		expire: 'Expired',
		toDeadLetter: 'Sent to the dead-letter address'
	};

	async function run(action: Action, ids: string[], to?: string) {
		const call = (id: string) =>
			action === 'move' || action === 'copy' ? ops[action](queue, id, to!) : ops[action](queue, id);
		const results = await Promise.allSettled(ids.map(call));
		// The operations answer false for a message no longer there.
		const done = results.filter((r) => r.status === 'fulfilled' && r.value !== false).length;
		const failed = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
		const gone = results.length - done - failed.length;
		const n = (k: number) => `${k} message${k === 1 ? '' : 's'}`;
		if (done) toast.success(`${DONE[action]} ${n(done)}`, to ? { description: `To ${to}` } : {});
		if (gone) toast.warning(`${n(gone)} no longer on the queue`);
		if (failed.length)
			toast.error(`${n(failed.length)} could not be ${DONE[action].toLowerCase()}`, {
				description: (failed[0].reason as Error).message
			});
		selected.clear();
		if (action !== 'copy') viewing = null;
		refresh();
	}

	let picking = $state<{ action: 'move' | 'copy'; ids: string[] } | null>(null);
	let confirming = $state<{ action: 'remove' | 'expire'; ids: string[] } | null>(null);
	let resend = $state<{ address: string; draft: Draft } | null>(null);
	let sending = $state(false);

	function resendOf(m: Message) {
		const headers = Object.entries(m.StringProperties ?? {})
			.filter(([k]) => !k.startsWith('_AMQ') && !k.startsWith('__AMQ'))
			.map(([name, value]) => ({ name, value }));
		resend = {
			address: m.address,
			draft: { headers, body: typeof m.text === 'string' ? m.text : '', durable: m.durable }
		};
		sending = true;
	}

	const allShown = $derived(
		messages.length > 0 && messages.every((m) => selected.has(m.messageID))
	);
	const someShown = $derived(messages.some((m) => selected.has(m.messageID)));
	const preview = (m: Message) => {
		const t = bodyText(m, 'text').text;
		return t.length > 80 ? `${t.slice(0, 80)}…` : t;
	};
</script>

{#snippet actions(ids: string[], compact: boolean)}
	{@const size = compact ? 'xs' : 'sm'}
	{#if may.moveMessage}
		<Button {size} variant="outline" onclick={() => (picking = { action: 'move', ids })}
			>Move</Button
		>
	{/if}
	{#if may.copyMessage}
		<Button {size} variant="outline" onclick={() => (picking = { action: 'copy', ids })}
			>Copy</Button
		>
	{/if}
	{#if retryable && may.retryMessage}
		<Button {size} variant="outline" onclick={() => run('retry', ids)}>Retry</Button>
	{/if}
	{#if may.expireMessage}
		<Button {size} variant="outline" onclick={() => (confirming = { action: 'expire', ids })}>
			Expire
		</Button>
	{/if}
	{#if !retryable && may.sendMessageToDeadLetterAddress}
		<Button {size} variant="outline" onclick={() => run('toDeadLetter', ids)}
			>To dead letters</Button
		>
	{/if}
	{#if may.removeMessage}
		<Button {size} variant="destructive" onclick={() => (confirming = { action: 'remove', ids })}>
			Delete
		</Button>
	{/if}
{/snippet}

<div class="flex flex-col gap-3">
	<div class="flex flex-wrap items-center gap-2">
		<form
			class="flex min-w-64 flex-1 gap-2"
			onsubmit={(e) => {
				e.preventDefault();
				filter = typed.trim();
				page = 1;
			}}
		>
			<Input
				aria-label="Filter"
				placeholder="A filter, such as priority > 4 AND color = 'red'"
				class="font-mono"
				bind:value={typed}
			/>
			<Button type="submit" variant="outline" size="sm">Filter</Button>
		</form>
		<Button variant="ghost" size="icon-sm" aria-label="Refresh" onclick={refresh}>
			<RefreshCwIcon class={cn(loading && 'animate-spin')} />
		</Button>
	</div>

	{#if selected.size}
		<div class="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/40 px-3 py-2">
			<span class="mr-2 text-sm">{selected.size} selected</span>
			{@render actions([...selected], true)}
		</div>
	{/if}

	{#if error}
		<Alert.Root variant="destructive">
			<Alert.Title>The messages cannot be browsed</Alert.Title>
			<Alert.Description>{error}</Alert.Description>
		</Alert.Root>
	{/if}

	<div class="overflow-x-auto rounded-lg border">
		<Table.Root aria-label="Messages">
			<Table.Header>
				<Table.Row>
					<Table.Head class="w-8">
						<Checkbox
							aria-label="Select the page"
							checked={allShown}
							indeterminate={someShown && !allShown}
							onCheckedChange={(c) =>
								messages.forEach((m) =>
									c ? selected.add(m.messageID) : selected.delete(m.messageID)
								)}
						/>
					</Table.Head>
					<Table.Head>ID</Table.Head>
					<Table.Head>Type</Table.Head>
					<Table.Head class="text-right">Priority</Table.Head>
					<Table.Head>Sent</Table.Head>
					<Table.Head>Expires</Table.Head>
					<Table.Head>Durable</Table.Head>
					<Table.Head class="text-right">Size</Table.Head>
					{#if retryable}<Table.Head>From</Table.Head>{/if}
					<Table.Head>Body</Table.Head>
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#each messages as m (m.messageID)}
					<Table.Row data-state={selected.has(m.messageID) ? 'selected' : undefined}>
						<Table.Cell>
							<Checkbox
								aria-label="Select message {m.messageID}"
								checked={selected.has(m.messageID)}
								onCheckedChange={(c) =>
									c ? selected.add(m.messageID) : selected.delete(m.messageID)}
							/>
						</Table.Cell>
						<Table.Cell>
							<button
								class="font-mono text-primary underline-offset-4 hover:underline"
								onclick={() => (viewing = m)}
							>
								{m.messageID}
							</button>
						</Table.Cell>
						<Table.Cell>{MESSAGE_TYPES[m.type] ?? m.type}</Table.Cell>
						<Table.Cell class="text-right tabular-nums">{m.priority}</Table.Cell>
						<Table.Cell class="whitespace-nowrap">
							{m.timestamp ? new Date(m.timestamp).toLocaleString() : ''}
						</Table.Cell>
						<Table.Cell class="whitespace-nowrap">
							{m.expiration ? new Date(m.expiration).toLocaleString() : 'never'}
						</Table.Cell>
						<Table.Cell>{m.durable ? 'yes' : 'no'}</Table.Cell>
						<Table.Cell class="text-right tabular-nums">{formatBytes(m.persistentSize)}</Table.Cell>
						{#if retryable}<Table.Cell class="font-mono">{originalQueue(m) ?? ''}</Table.Cell>{/if}
						<Table.Cell class="max-w-80 truncate font-mono text-muted-foreground">
							{preview(m)}
						</Table.Cell>
					</Table.Row>
				{:else}
					<Table.Row>
						<Table.Cell colspan={10} class="py-8 text-center text-muted-foreground">
							{#if loading}Loading…{:else if count > 0 && page === 1}None to browse: the queue's
								messages are out with its consumers.{:else if filter}No message matches.{:else if count > 0}Nothing
								on this page.{:else}The queue is empty.{/if}
						</Table.Cell>
					</Table.Row>
				{/each}
			</Table.Body>
		</Table.Root>
	</div>

	<div class="flex flex-wrap items-center justify-between gap-3 text-sm">
		<span class="text-muted-foreground">
			{formatNumber(count)} message{count === 1 ? '' : 's'}{filter ? ' match' : ''}
		</span>
		<div class="flex items-center gap-2">
			<select
				aria-label="Messages per page"
				class="h-8 rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30"
				value={size}
				onchange={(e) => {
					size = Number(e.currentTarget.value);
					page = 1;
				}}
			>
				{#each PAGE_SIZES.filter((n) => n <= 1000) as n (n)}<option value={n}>{n}</option>{/each}
			</select>
			<Button
				variant="outline"
				size="icon-sm"
				aria-label="Previous page"
				disabled={page <= 1}
				onclick={() => page--}
			>
				<ChevronLeftIcon />
			</Button>
			<span class="tabular-nums">Page {page} of {last}</span>
			<Button
				variant="outline"
				size="icon-sm"
				aria-label="Next page"
				disabled={page >= last}
				onclick={() => page++}
			>
				<ChevronRightIcon />
			</Button>
		</div>
	</div>
</div>

<Sheet.Root open={viewing !== null} onOpenChange={(o) => !o && (viewing = null)}>
	<Sheet.Content class="w-full overflow-y-auto sm:max-w-2xl">
		{#if viewing}
			<Sheet.Header>
				<Sheet.Title>Message {viewing.messageID}</Sheet.Title>
				<Sheet.Description>On queue {queue}</Sheet.Description>
			</Sheet.Header>
			<div class="flex flex-wrap gap-2 px-4">
				{@render actions([viewing.messageID], false)}
				<Button size="sm" variant="outline" onclick={() => viewing && resendOf(viewing)}>
					Resend
				</Button>
			</div>
			<div class="p-4"><MessageView message={viewing} /></div>
		{/if}
	</Sheet.Content>
</Sheet.Root>

<QueuePicker
	title={picking?.action === 'copy' ? 'Copy to a queue' : 'Move to a queue'}
	description={`${picking?.ids.length ?? 0} message${picking?.ids.length === 1 ? '' : 's'} from ${queue}.`}
	action={picking?.action === 'copy' ? 'Copy' : 'Move'}
	exclude={queue}
	onpick={(to) => picking && run(picking.action, picking.ids, to)}
	bind:open={() => picking !== null, (o) => !o && (picking = null)}
/>

<AlertDialog.Root open={confirming !== null} onOpenChange={(o) => !o && (confirming = null)}>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>
				{confirming?.action === 'expire' ? 'Expire' : 'Delete'}
				{confirming?.ids.length === 1 ? 'the message' : `${confirming?.ids.length} messages`}?
			</AlertDialog.Title>
			<AlertDialog.Description>
				{confirming?.action === 'expire'
					? 'They go to the expiry address, or are dropped when there is none.'
					: 'They are acknowledged and gone for good.'}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
			<AlertDialog.Action
				onclick={() => {
					const c = confirming;
					confirming = null;
					if (c) run(c.action, c.ids);
				}}
			>
				{confirming?.action === 'expire' ? 'Expire' : 'Delete'}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>

{#if resend}
	<SendMessage
		bind:open={sending}
		resource={addressResource(resend.address)}
		target={`address ${resend.address}`}
		draft={resend.draft}
		onsent={refresh}
	/>
{/if}
