<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { untrack } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { toast } from 'svelte-sonner';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Switch } from '#lib/components/ui/switch/index.js';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import DataTable, { type Column, type Row } from '#lib/components/DataTable.svelte';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import { api, ALL, type Listing, type NodeError } from '#lib/api/index.js';
	import { LISTINGS, isListing, label, type ListingInfo } from '#lib/listings.js';
	import { listingHref, linkHref, hashQuery, overviewHref } from '#lib/nav.js';
	import { nodes } from '#lib/state/nodes.svelte.js';
	import CreateAddress from '#lib/components/CreateAddress.svelte';
	import CreateQueue from '#lib/components/CreateQueue.svelte';
	import { MAX_PAGE_SIZE, parseQuery, queryParams, type ListQuery } from '#lib/query.js';
	import { listingViews } from '#lib/views.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { session } from '#lib/state/session.svelte.js';
	import { live, listingPage, listingView } from '#lib/state/live.svelte.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	const kind = $derived(isListing(params.listing) ? params.listing : undefined);
	const info = $derived(kind ? (LISTINGS[kind] as unknown as ListingInfo<Listing>) : undefined);
	const query = $derived(parseQuery(hashQuery(page.url), prefs.pageSize));

	/** Across every node, each row names its node. */
	const every = $derived(params.node === ALL);

	let fields = $state<string[]>([]);
	// The page loaded last, and the listing it is of: another listing's rows
	// do not have this one's keys.
	let loaded = $state<{
		kind: Listing;
		rows: Row[];
		count: number;
		errors: NodeError[];
	} | null>(null);
	const rows = $derived(loaded && loaded.kind === kind ? loaded.rows : []);
	const count = $derived(loaded && loaded.kind === kind ? loaded.count : 0);
	const nodeErrors = $derived(loaded && loaded.kind === kind ? loaded.errors : []);

	/** A page's rows, a row's node shown by its name and kept by its ID. */
	function received(p: { data: unknown[]; errors?: NodeError[] }): {
		rows: Row[];
		errors: NodeError[];
	} {
		const rows = (p.data as Row[]).map((r) =>
			typeof r.node === 'string' ? { ...r, node: nodes.nameOf(r.node), nodeID: r.node } : r
		);
		return { rows, errors: p.errors ?? [] };
	}
	let loading = $state(false);
	let error = $state<string | null>(null);
	let tick = $state(0);
	const selected = new SvelteSet<string>();

	$effect(() => {
		if (!kind) return;
		listingViews()
			.then((v) => (fields = v[kind].fields.map((f) => f.name)))
			.catch(() => (fields = []));
	});

	$effect(() => {
		if (!kind) return;
		const q = query;
		const listing = kind;
		void tick;
		let stale = false;
		loading = true;
		api
			.list(kind, q)
			.then((p) => {
				if (stale) return;
				loaded = { kind: listing, count: p.count, ...received(p) };
				error = null;
			})
			.catch((e: Error) => {
				if (stale) return;
				loaded = null;
				error = e.message;
			})
			.finally(() => {
				if (!stale) loading = false;
			});
		return () => {
			stale = true;
		};
	});

	// Live, the page as the stream reads it every tick.
	const liveView = $derived(kind && prefs.liveTables ? listingView(kind, query) : null);
	$effect(() => {
		const v = liveView;
		if (v) return live.watch(v);
	});
	$effect(() => {
		const v = liveView;
		const listing = kind;
		if (!v || !listing) return;
		const p = listingPage(live.value(v));
		if (!p) return;
		untrack(() => {
			loaded = { kind: listing, count: p.count, ...received(p) };
			error = null;
		});
	});
	$effect(() => {
		void tick;
		live.refresh();
	});

	const shown = $derived(kind ? prefs.columns(kind) : []);
	const NODE: Column = { key: 'node', label: 'Node', sortable: false };
	const columns = $derived<Column[]>([
		...(every ? [NODE] : []),
		...shown.map((key) => ({
			key,
			label: label(key),
			sortable: fields.includes(key),
			identifier: key === 'id' || /ID$/.test(key)
		}))
	]);

	/**
	 * A linked value leads to its page, or to the listing it names, filtered
	 * to it, on the row's node; a row's node to that node.
	 */
	function href(row: Row, column: Column): string | undefined {
		const node = every && typeof row.nodeID === 'string' ? nodes.idOf(row.nodeID) : params.node;
		if (column.key === 'node') return every ? overviewHref(node) : undefined;
		const link = info?.links?.[column.key as never] as { kind: Listing; field: string } | undefined;
		const value = row[column.key];
		if (!link || value === '' || value === undefined || value === null) return undefined;
		return linkHref(node, link, String(value));
	}
	const allColumns = $derived(
		(info?.columns ?? []).map((key) => ({ key, label: label(key as string) }))
	);
	const filterFields = $derived(fields.map((name) => ({ name, label: label(name) })));

	function onquery(q: ListQuery) {
		if (!kind) return;
		goto(listingHref(params.node, kind, queryParams(q, prefs.pageSize)), { reset: false });
	}

	async function exportAll(): Promise<Row[]> {
		if (!kind) return [];
		const all: Row[] = [];
		for (let p = 1; ; p++) {
			const r = await api.list(kind, { ...query, page: p, size: MAX_PAGE_SIZE });
			all.push(...received(r).rows);
			if (all.length >= r.count || r.data.length === 0) return all;
		}
	}

	const may = $derived(info?.bulk ? session.may(info.bulk.operation) : false);
	let confirm = $state<Row[] | null>(null);

	const creates = $derived(
		kind === 'addresses' ? 'createAddress' : kind === 'queues' ? 'createQueue' : undefined
	);
	const mayCreate = $derived(creates ? session.may(creates) : false);
	let creating = $state(false);

	/** `the queue`, `3 addresses`. */
	function some(n: number): string {
		const noun = info?.noun ?? '';
		if (n === 1) return `the ${noun}`;
		return `${n} ${noun === 'address' ? 'addresses' : `${noun}s`}`;
	}

	async function runAll(which: Row[]) {
		if (!info?.bulk) return;
		const bulk = info.bulk;
		confirm = null;
		const results = await Promise.allSettled(which.map((r) => bulk.run(r as never)));
		const failed = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
		const done = results.length - failed.length;
		const past = bulk.verb === 'Close' ? 'Closed' : 'Deleted';
		if (done) toast.success(`${past} ${some(done)}`);
		if (failed.length)
			toast.error(`${failed.length} could not be ${past.toLowerCase()}`, {
				description: (failed[0].reason as Error).message
			});
		selected.clear();
		tick++;
	}
</script>

<svelte:head>
	<title>{info?.title ?? 'Not found'} · Saranga console</title>
</svelte:head>

{#if kind && info}
	<div class="flex flex-col gap-4">
		<div class="flex items-center justify-between gap-3">
			<h1 class="text-xl font-semibold">{info.title}</h1>
			<div class="flex items-center gap-4">
				<div class="flex items-center gap-2">
					<Switch
						id="live-table"
						checked={prefs.liveTables}
						onCheckedChange={(c) => (prefs.liveTables = c)}
					/>
					<Label for="live-table">Live</Label>
				</div>
				{#if mayCreate}
					<Button size="sm" onclick={() => (creating = true)}>Create {info.noun}</Button>
				{/if}
			</div>
		</div>
		{#each nodeErrors as e (e.node)}
			<Alert.Root variant="destructive">
				<Alert.Description>
					{nodes.nameOf(e.node)} could not answer: {e.error}
				</Alert.Description>
			</Alert.Root>
		{/each}
		<DataTable
			label={info.title}
			{columns}
			{allColumns}
			onColumns={(keys) => prefs.setColumns(kind, keys)}
			{rows}
			rowKey={(r) => `${every ? `${r.nodeID}/` : ''}${info.key(r as never)}`}
			{count}
			{query}
			{onquery}
			fields={filterFields}
			{loading}
			{error}
			selectable={may}
			{selected}
			exportName={kind}
			{exportAll}
			onrefresh={() => tick++}
			{href}
		>
			{#snippet bulk({ rows: chosen })}
				<Button size="xs" variant="destructive" onclick={() => (confirm = chosen)}>
					{info.bulk?.verb}
					{some(chosen.length)}
				</Button>
			{/snippet}
		</DataTable>
	</div>

	<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
		<AlertDialog.Content>
			<AlertDialog.Header>
				<AlertDialog.Title>{info.bulk?.verb} {some(confirm?.length ?? 0)}?</AlertDialog.Title>
				<AlertDialog.Description>{info.bulk?.warning}</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action onclick={() => confirm && runAll(confirm)}>
					{info.bulk?.verb}
				</AlertDialog.Action>
			</AlertDialog.Footer>
		</AlertDialog.Content>
	</AlertDialog.Root>

	{#if kind === 'addresses'}
		<CreateAddress bind:open={creating} oncreated={() => tick++} />
	{:else if kind === 'queues'}
		<CreateQueue bind:open={creating} oncreated={() => tick++} />
	{/if}
{:else}
	<h1 class="text-lg font-semibold">No such page</h1>
	<p class="mt-1 text-sm text-muted-foreground">
		{params.listing} is not one of the console's pages.
	</p>
{/if}
