<!--
	A node's XA transactions: those prepared and waiting for their
	coordinator, each with the messages it sends and acknowledges, to commit
	or roll back by hand; and those completed heuristically.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import { transactions, type PreparedTransaction } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { display } from '#lib/explorer.js';

	let prepared = $state<PreparedTransaction[] | null>(null);
	let committed = $state<string[]>([]);
	let rolledBack = $state<string[]>([]);
	let error = $state<string | null>(null);
	let tick = $state(0);
	let open = $state<Record<string, boolean>>({});
	let may = $state<Allowed>({});

	$effect(() => {
		allowed('broker', ['commitPreparedTransaction', 'rollbackPreparedTransaction']).then(
			(a) => (may = a)
		);
	});
	$effect(() => {
		void tick;
		Promise.all([
			transactions.prepared(),
			transactions.heuristicCommitted(),
			transactions.heuristicRolledBack()
		])
			.then(([p, c, r]) => {
				prepared = p;
				committed = c ?? [];
				rolledBack = r ?? [];
				error = null;
			})
			.catch((e: Error) => (error = e.message));
	});

	let confirm = $state<{ xid: string; commit: boolean } | null>(null);
	async function complete() {
		const c = confirm;
		confirm = null;
		if (!c) return;
		const what = c.commit ? 'committed' : 'rolled back';
		try {
			const done = await (c.commit ? transactions.commit(c.xid) : transactions.rollback(c.xid));
			if (done) toast.success(`The transaction was ${what}`);
			else toast.error(`The transaction was not ${what}: it is no longer prepared`);
		} catch (e) {
			toast.error(`The transaction could not be ${what}`, { description: (e as Error).message });
		}
		tick++;
	}
</script>

<svelte:head>
	<title>Transactions · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<div class="flex items-center justify-between gap-2">
		<h1 class="text-xl font-semibold">Transactions</h1>
		<Button size="sm" variant="outline" onclick={() => tick++}>Refresh</Button>
	</div>
	{#if error}<p class="text-sm text-destructive">{error}</p>{/if}

	<section class="flex flex-col gap-2" aria-labelledby="prepared">
		<h2 id="prepared" class="font-semibold">Prepared</h2>
		<p class="max-w-2xl text-sm text-muted-foreground">
			Each waits for its transaction manager to commit or roll it back. Completing one here is a
			heuristic decision, which the manager is told of when it recovers.
		</p>
		{#if prepared === null}
			{#if !error}<Skeleton class="h-16 w-full" />{/if}
		{:else if !prepared.length}
			<p class="text-sm text-muted-foreground">No prepared transaction.</p>
		{:else}
			<div class="overflow-x-auto rounded-md border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head><span class="sr-only">Messages</span></Table.Head>
							<Table.Head>Created</Table.Head>
							<Table.Head>Format</Table.Head>
							<Table.Head>Global transaction ID</Table.Head>
							<Table.Head>Branch</Table.Head>
							<Table.Head class="text-right">Messages</Table.Head>
							<Table.Head><span class="sr-only">Actions</span></Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each prepared as t (t.xid_as_base64)}
							<Table.Row>
								<Table.Cell class="w-8">
									<Button
										variant="ghost"
										size="icon-xs"
										aria-expanded={!!open[t.xid_as_base64]}
										aria-label="Messages of {t.xid_global_txid}"
										onclick={() => (open[t.xid_as_base64] = !open[t.xid_as_base64])}
									>
										<ChevronRightIcon
											class="transition-transform {open[t.xid_as_base64] ? 'rotate-90' : ''}"
										/>
									</Button>
								</Table.Cell>
								<Table.Cell>{t.creation_time}</Table.Cell>
								<Table.Cell class="tabular-nums">{t.xid_format_id}</Table.Cell>
								<Table.Cell class="max-w-64 truncate font-mono text-xs"
									>{t.xid_global_txid}</Table.Cell
								>
								<Table.Cell class="max-w-48 truncate font-mono text-xs"
									>{t.xid_branch_qual}</Table.Cell
								>
								<Table.Cell class="text-right tabular-nums"
									>{t.tx_related_messages.length}</Table.Cell
								>
								<Table.Cell class="text-right whitespace-nowrap">
									<Button
										size="xs"
										variant="outline"
										disabled={may.commitPreparedTransaction === false}
										onclick={() => (confirm = { xid: t.xid_as_base64, commit: true })}
										>Commit</Button
									>
									<Button
										size="xs"
										variant="outline"
										disabled={may.rollbackPreparedTransaction === false}
										onclick={() => (confirm = { xid: t.xid_as_base64, commit: false })}
										>Roll back</Button
									>
								</Table.Cell>
							</Table.Row>
							{#if open[t.xid_as_base64]}
								<Table.Row>
									<Table.Cell colspan={7} class="bg-muted/30">
										<ul class="flex flex-col gap-1 text-xs">
											{#each t.tx_related_messages as m, i (i)}
												<li class="flex flex-wrap items-baseline gap-2">
													<Badge variant="secondary">{m.message_operation_type}</Badge>
													<span>{m.message_type}</span>
													<span class="font-mono text-muted-foreground"
														>{display(m.message_properties)}</span
													>
												</li>
											{:else}
												<li class="text-muted-foreground">No message.</li>
											{/each}
										</ul>
									</Table.Cell>
								</Table.Row>
							{/if}
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/if}
	</section>

	{#each [{ id: 'committed', title: 'Heuristically committed', list: committed }, { id: 'rolled-back', title: 'Heuristically rolled back', list: rolledBack }] as h (h.id)}
		<section class="flex flex-col gap-2" aria-labelledby={h.id}>
			<h2 id={h.id} class="font-semibold">{h.title}</h2>
			{#if !h.list.length}
				<p class="text-sm text-muted-foreground">None.</p>
			{:else}
				<ul class="flex flex-col gap-1 font-mono text-xs">
					{#each h.list as xid (xid)}<li class="break-all">{xid}</li>{/each}
				</ul>
			{/if}
		</section>
	{/each}
</div>

<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
	<AlertDialog.Content>
		{#if confirm}
			<AlertDialog.Header>
				<AlertDialog.Title
					>{confirm.commit ? 'Commit' : 'Roll back'} this transaction?</AlertDialog.Title
				>
				<AlertDialog.Description>
					{confirm.commit
						? 'Its messages are sent and acknowledged now, whatever its transaction manager decides.'
						: 'Its sends are dropped and its acknowledged messages go back to their queues, whatever its transaction manager decides.'}
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action
					class="bg-destructive text-white hover:bg-destructive/90"
					onclick={complete}>{confirm.commit ? 'Commit' : 'Roll back'}</AlertDialog.Action
				>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
