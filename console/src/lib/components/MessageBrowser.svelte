<!--
	A queue's messages, as Artemis's console browses them (`browse(page, size,
	filter)` and `countMessages(filter)`): a page at a time, filtered by a
	selector, with the actions the guard allows on a selection or on one
	message, and on every message the filter selects (`moveMessages(filter,
	queue)`...). Those can be exported whole, and messages exported imported.
-->
<script lang="ts">
	import { SvelteSet } from 'svelte/reactivity';
	import { toast } from 'svelte-sonner';
	import ChevronLeftIcon from '@lucide/svelte/icons/chevron-left';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import ChevronDownIcon from '@lucide/svelte/icons/chevron-down';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { admin } from '#lib/api/index.js';
	import { messageCount, messagesFile, saveText } from '#lib/files.js';
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
		matching,
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
		'sendMessageToDeadLetterAddress',
		'changeMessagePriority',
		'moveMessages',
		'removeMessages',
		'retryMessages',
		'expireMessages',
		'sendMessagesToDeadLetterAddress',
		'changeMessagesPriority',
		'sendMessage'
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

	type Action = 'remove' | 'move' | 'copy' | 'retry' | 'expire' | 'toDeadLetter' | 'priority';

	const DONE: Record<Action, string> = {
		remove: 'Deleted',
		move: 'Moved',
		copy: 'Copied',
		retry: 'Retried',
		expire: 'Expired',
		toDeadLetter: 'Sent to the dead-letter address',
		priority: 'Changed the priority of'
	};

	async function run(action: Action, ids: string[], to?: string, priority?: number) {
		const call = (id: string) =>
			action === 'move' || action === 'copy'
				? ops[action](queue, id, to!)
				: action === 'priority'
					? ops.changePriority(queue, id, priority!)
					: ops[action](queue, id);
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

	/** What every message the filter selects is having done, by the queue's `*Messages` operations. */
	type Bulk = 'move' | 'remove' | 'retry' | 'expire' | 'toDeadLetter' | 'priority';

	async function runMatching(action: Bulk, to?: string, priority?: number) {
		try {
			const n =
				action === 'move'
					? await matching.move(queue, filter, to!)
					: action === 'priority'
						? await matching.changePriority(queue, filter, priority!)
						: await matching[action](queue, filter);
			toast.success(
				`${DONE[action]} ${n} message${n === 1 ? '' : 's'}`,
				to ? { description: `To ${to}` } : {}
			);
		} catch (e) {
			toast.error(`They could not be ${DONE[action].toLowerCase()}`, {
				description: (e as Error).message
			});
		}
		selected.clear();
		refresh();
	}

	let exporting = $state(false);
	async function exportMatching() {
		exporting = true;
		try {
			const text = await admin.exportMessages(queue, filter);
			saveText(messagesFile(queue), text);
			const n = messageCount(text);
			toast.success(`Exported ${n} message${n === 1 ? '' : 's'}`);
		} catch (e) {
			toast.error('They could not be exported', { description: (e as Error).message });
		} finally {
			exporting = false;
		}
	}

	let importer = $state<HTMLInputElement | null>(null);
	async function importFile(file: File | undefined) {
		if (!file) return;
		try {
			const r = await admin.importMessages(queue, await file.text());
			const n = (k: number) => `${k} message${k === 1 ? '' : 's'}`;
			if (r.error) toast.warning(`Imported ${n(r.sent)}, then stopped`, { description: r.error });
			else toast.success(`Imported ${n(r.sent)}`, { description: `From ${file.name}` });
		} catch (e) {
			toast.error(`${file.name} could not be imported`, { description: (e as Error).message });
		}
		if (importer) importer.value = '';
		refresh();
	}

	let picking = $state<{ action: 'move' | 'copy'; ids: string[] | null } | null>(null);
	/** A new priority, for the selection or, with no IDs, for every message the filter selects. */
	let prioritizing = $state<{ ids: string[] | null; value: number } | null>(null);
	let confirmingAll = $state<'remove' | 'expire' | 'toDeadLetter' | 'retry' | null>(null);
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
	{#if may.changeMessagePriority}
		<Button {size} variant="outline" onclick={() => (prioritizing = { ids, value: 4 })}
			>Priority</Button
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
		<DropdownMenu.Root>
			<DropdownMenu.Trigger>
				{#snippet child({ props })}
					<Button {...props} size="sm" variant="outline" disabled={!count}>
						{filter ? 'Every match' : 'Every message'}
						<ChevronDownIcon />
					</Button>
				{/snippet}
			</DropdownMenu.Trigger>
			<DropdownMenu.Content align="end">
				<DropdownMenu.Label class="text-xs font-normal text-muted-foreground">
					{formatNumber(count)} message{count === 1 ? '' : 's'}{filter ? ' matching' : ''}
				</DropdownMenu.Label>
				<DropdownMenu.Item
					disabled={!may.moveMessages}
					onSelect={() => (picking = { action: 'move', ids: null })}>Move…</DropdownMenu.Item
				>
				{#if retryable}
					<DropdownMenu.Item
						disabled={!may.retryMessages}
						onSelect={() => (confirmingAll = 'retry')}>Retry</DropdownMenu.Item
					>
				{:else}
					<DropdownMenu.Item
						disabled={!may.sendMessagesToDeadLetterAddress}
						onSelect={() => (confirmingAll = 'toDeadLetter')}
						>Send to the dead-letter address</DropdownMenu.Item
					>
				{/if}
				<DropdownMenu.Item
					disabled={!may.changeMessagesPriority}
					onSelect={() => (prioritizing = { ids: null, value: 4 })}
					>Change the priority…</DropdownMenu.Item
				>
				<DropdownMenu.Item
					disabled={!may.expireMessages}
					onSelect={() => (confirmingAll = 'expire')}>Expire</DropdownMenu.Item
				>
				<DropdownMenu.Item disabled={exporting || !may.browse} onSelect={exportMatching}
					>Export as JSON</DropdownMenu.Item
				>
				<DropdownMenu.Separator />
				<DropdownMenu.Item
					variant="destructive"
					disabled={!may.removeMessages}
					onSelect={() => (confirmingAll = 'remove')}>Delete</DropdownMenu.Item
				>
			</DropdownMenu.Content>
		</DropdownMenu.Root>
		<Button
			size="sm"
			variant="outline"
			disabled={may.sendMessage === false}
			onclick={() => importer?.click()}>Import</Button
		>
		<input
			bind:this={importer}
			type="file"
			accept="application/json,.json"
			class="hidden"
			aria-label="Messages to import"
			onchange={(e) => importFile(e.currentTarget.files?.[0])}
		/>
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
	description={picking?.ids
		? `${picking.ids.length} message${picking.ids.length === 1 ? '' : 's'} from ${queue}.`
		: `Every message${filter ? ` that ${filter} selects` : ''} on ${queue}.`}
	action={picking?.action === 'copy' ? 'Copy' : 'Move'}
	exclude={queue}
	onpick={(to) =>
		picking && (picking.ids ? run(picking.action, picking.ids, to) : runMatching('move', to))}
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

<AlertDialog.Root open={confirmingAll !== null} onOpenChange={(o) => !o && (confirmingAll = null)}>
	<AlertDialog.Content>
		{#if confirmingAll}
			<AlertDialog.Header>
				<AlertDialog.Title>
					{confirmingAll === 'remove'
						? 'Delete'
						: confirmingAll === 'expire'
							? 'Expire'
							: confirmingAll === 'retry'
								? 'Retry'
								: 'Send to the dead-letter address'}
					{filter ? `every message that ${filter} selects` : 'every message'}?
				</AlertDialog.Title>
				<AlertDialog.Description>
					{formatNumber(count)} message{count === 1 ? '' : 's'} on {queue} now; any that arrive meanwhile
					and match too.
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action
					onclick={() => {
						const a = confirmingAll;
						confirmingAll = null;
						if (a) runMatching(a);
					}}
				>
					{confirmingAll === 'remove'
						? 'Delete'
						: confirmingAll === 'expire'
							? 'Expire'
							: confirmingAll === 'retry'
								? 'Retry'
								: 'Send'}
				</AlertDialog.Action>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>

<Dialog.Root open={prioritizing !== null} onOpenChange={(o) => !o && (prioritizing = null)}>
	<Dialog.Content class="sm:max-w-sm">
		{#if prioritizing}
			<Dialog.Header>
				<Dialog.Title>Change the priority</Dialog.Title>
				<Dialog.Description>
					{prioritizing.ids
						? `${prioritizing.ids.length} message${prioritizing.ids.length === 1 ? '' : 's'}.`
						: `Every message${filter ? ` that ${filter} selects` : ''} on ${queue}.`}
				</Dialog.Description>
			</Dialog.Header>
			<form
				id="priority"
				class="grid gap-1.5"
				onsubmit={(e) => {
					e.preventDefault();
					const p = prioritizing;
					prioritizing = null;
					if (!p) return;
					if (p.ids) run('priority', p.ids, undefined, p.value);
					else runMatching('priority', undefined, p.value);
				}}
			>
				<Label for="priority-value">Priority, 0 to 9</Label>
				<Input
					id="priority-value"
					type="number"
					min="0"
					max="9"
					required
					bind:value={prioritizing.value}
				/>
			</form>
			<Dialog.Footer>
				<Button variant="outline" onclick={() => (prioritizing = null)}>Cancel</Button>
				<Button type="submit" form="priority">Change</Button>
			</Dialog.Footer>
		{/if}
	</Dialog.Content>
</Dialog.Root>

{#if resend}
	<SendMessage
		bind:open={sending}
		resource={addressResource(resend.address)}
		target={`address ${resend.address}`}
		draft={resend.draft}
		onsent={refresh}
	/>
{/if}
