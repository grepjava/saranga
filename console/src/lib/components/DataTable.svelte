<script lang="ts" module>
	export interface Column {
		key: string;
		label: string;
		/** Whether the broker sorts by it. */
		sortable: boolean;
		/** An identifier: shown as it is, even when it is a number. */
		identifier?: boolean;
	}

	export type Row = Record<string, unknown>;

	/** Rows taller pages render only around what is in view. */
	export const VIRTUAL_FROM = 150;
	const ROW_HEIGHT = 37;
	const OVERSCAN = 10;
</script>

<script lang="ts">
	import type { Snippet } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import ArrowUpIcon from '@lucide/svelte/icons/arrow-up';
	import ArrowDownIcon from '@lucide/svelte/icons/arrow-down';
	import ChevronsUpDownIcon from '@lucide/svelte/icons/chevrons-up-down';
	import ChevronLeftIcon from '@lucide/svelte/icons/chevron-left';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import ChevronsLeftIcon from '@lucide/svelte/icons/chevrons-left';
	import ChevronsRightIcon from '@lucide/svelte/icons/chevrons-right';
	import Columns3Icon from '@lucide/svelte/icons/columns-3';
	import DownloadIcon from '@lucide/svelte/icons/download';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import FilterBar from './FilterBar.svelte';
	import { PAGE_SIZES, pages, type ListQuery } from '#lib/query.js';
	import { formatNumber, kindOf, type Kind } from '#lib/format.js';
	import { download, toCsv } from '#lib/csv.js';
	import { cn } from '#lib/utils.js';
	import { scrollable } from '#lib/scrollable.js';

	let {
		columns,
		allColumns,
		onColumns,
		rows,
		rowKey,
		count,
		query,
		onquery,
		fields,
		loading = false,
		error = null,
		selectable = false,
		selected = $bindable(new SvelteSet<string>()),
		href,
		bulk,
		exportName,
		exportAll,
		onrefresh,
		label
	}: {
		/** The columns shown, in order. */
		columns: Column[];
		/** Every column the user may show. */
		allColumns: { key: string; label: string }[];
		/** The user picked columns; `undefined` puts back the defaults. */
		onColumns: (keys: string[] | undefined) => void;
		rows: Row[];
		rowKey: (row: Row) => string;
		/** How many rows match, over every page. */
		count: number;
		query: ListQuery;
		onquery: (q: ListQuery) => void;
		/** The fields the rows filter by. */
		fields: { name: string; label: string }[];
		loading?: boolean;
		error?: string | null;
		selectable?: boolean;
		selected?: SvelteSet<string>;
		/** Where a cell's value leads, if anywhere. */
		href?: (row: Row, column: Column) => string | undefined;
		/** Actions on the selected rows. */
		bulk?: Snippet<[{ rows: Row[]; clear: () => void }]>;
		exportName: string;
		/** Every matching row, for the export of all pages. */
		exportAll?: () => Promise<Row[]>;
		onrefresh?: () => void;
		/** The table's accessible name. */
		label: string;
	} = $props();

	const kinds = $derived(
		Object.fromEntries(
			columns.map((c) => [c.key, c.identifier ? 'text' : kindOf(rows.map((r) => r[c.key]))])
		) as Record<string, Kind>
	);
	const lastPage = $derived(pages(count, query.size));
	const first = $derived(count === 0 ? 0 : (query.page - 1) * query.size + 1);
	const last = $derived(Math.min(count, (query.page - 1) * query.size + rows.length));

	// Selection is of the rows on the page.
	const keys = $derived(rows.map(rowKey));
	$effect(() => {
		for (const k of [...selected]) if (!keys.includes(k)) selected.delete(k);
	});
	const selectedRows = $derived(rows.filter((r) => selected.has(rowKey(r))));
	const allSelected = $derived(rows.length > 0 && selectedRows.length === rows.length);

	// Long pages render the rows in view and spacers for the rest.
	let scrollTop = $state(0);
	// The window bounds what the table shows; measuring the table itself
	// would change with the rows it renders.
	let viewport = $state(900);
	const virtual = $derived(rows.length > VIRTUAL_FROM);
	const start = $derived(virtual ? Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN) : 0);
	const end = $derived(
		virtual
			? Math.min(rows.length, Math.ceil((scrollTop + viewport) / ROW_HEIGHT) + OVERSCAN)
			: rows.length
	);
	const visible = $derived(rows.slice(start, end));

	function sortBy(column: Column) {
		if (!column.sortable) return;
		const q = { ...query, page: 1 };
		if (query.sort !== column.key) onquery({ ...q, sort: column.key, order: 'asc' });
		else if (query.order === 'asc') onquery({ ...q, order: 'desc' });
		else onquery({ ...q, sort: undefined, order: 'asc' });
	}

	function toPage(page: number) {
		onquery({ ...query, page: Math.min(Math.max(1, page), lastPage) });
	}

	function text(value: unknown, kind: Kind): string {
		if (value === null || value === undefined) return '';
		if (kind === 'number') return formatNumber(value);
		return String(value);
	}

	function exportRows(which: Row[]) {
		download(
			`${exportName}.csv`,
			toCsv(
				columns.map((c) => c.key),
				columns.map((c) => c.label),
				which
			)
		);
	}

	let exporting = $state(false);
	async function exportEvery() {
		if (!exportAll) return;
		exporting = true;
		try {
			exportRows(await exportAll());
		} finally {
			exporting = false;
		}
	}

	const shownKeys = $derived(columns.map((c) => c.key));
	function toggleColumn(key: string, on: boolean) {
		const next = on
			? allColumns.map((c) => c.key).filter((k) => k === key || shownKeys.includes(k))
			: shownKeys.filter((k) => k !== key);
		if (next.length) onColumns(next);
	}
</script>

<svelte:window bind:innerHeight={viewport} />

<div class="flex min-w-0 flex-col gap-3">
	<div class="flex flex-wrap items-start justify-between gap-2">
		<FilterBar
			{fields}
			filters={query.filters}
			onchange={(filters) => onquery({ ...query, filters, page: 1 })}
		/>
		<div class="flex items-center gap-1">
			{#if onrefresh}
				<Button variant="ghost" size="icon-sm" aria-label="Refresh" onclick={onrefresh}>
					<RefreshCwIcon class={cn(loading && 'animate-spin')} />
				</Button>
			{/if}
			<DropdownMenu.Root>
				<DropdownMenu.Trigger>
					{#snippet child({ props })}
						<Button {...props} variant="outline" size="sm">
							<Columns3Icon />
							Columns
						</Button>
					{/snippet}
				</DropdownMenu.Trigger>
				<DropdownMenu.Content align="end" class="max-h-96 overflow-y-auto">
					{#each allColumns as c (c.key)}
						<DropdownMenu.CheckboxItem
							checked={shownKeys.includes(c.key)}
							onCheckedChange={(on) => toggleColumn(c.key, on)}
							closeOnSelect={false}
						>
							{c.label}
						</DropdownMenu.CheckboxItem>
					{/each}
					<DropdownMenu.Separator />
					<DropdownMenu.Item onSelect={() => onColumns(undefined)}
						>Default columns</DropdownMenu.Item
					>
				</DropdownMenu.Content>
			</DropdownMenu.Root>
			<DropdownMenu.Root>
				<DropdownMenu.Trigger>
					{#snippet child({ props })}
						<Button {...props} variant="outline" size="sm" disabled={exporting}>
							<DownloadIcon />
							Export
						</Button>
					{/snippet}
				</DropdownMenu.Trigger>
				<DropdownMenu.Content align="end">
					<DropdownMenu.Item onSelect={() => exportRows(rows)}>This page as CSV</DropdownMenu.Item>
					{#if exportAll}
						<DropdownMenu.Item onSelect={exportEvery}>
							Every matching row as CSV ({formatNumber(count)})
						</DropdownMenu.Item>
					{/if}
				</DropdownMenu.Content>
			</DropdownMenu.Root>
		</div>
	</div>

	{#if error}
		<Alert.Root variant="destructive">
			<Alert.Description>{error}</Alert.Description>
		</Alert.Root>
	{/if}

	{#if selectable && bulk && selectedRows.length}
		<div
			class="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/40 px-3 py-1.5 text-sm"
		>
			<span class="font-medium">{selectedRows.length} selected</span>
			{@render bulk({ rows: selectedRows, clear: () => selected.clear() })}
			<Button variant="ghost" size="xs" class="ml-auto" onclick={() => selected.clear()}>
				Clear
			</Button>
		</div>
	{/if}

	<div
		{@attach scrollable}
		class="relative max-h-[calc(100svh-16rem)] min-h-40 overflow-auto rounded-lg border"
		onscroll={(e) => (scrollTop = e.currentTarget.scrollTop)}
	>
		<table class="w-full caption-bottom text-sm" aria-label={label} aria-rowcount={count + 1}>
			<thead class="sticky top-0 z-[1] bg-muted/95 backdrop-blur">
				<tr class="border-b">
					{#if selectable}
						<th class="w-9 px-3">
							<Checkbox
								aria-label="Select every row on the page"
								checked={allSelected}
								indeterminate={selectedRows.length > 0 && !allSelected}
								onCheckedChange={(on) => {
									if (on) for (const k of keys) selected.add(k);
									else selected.clear();
								}}
							/>
						</th>
					{/if}
					{#each columns as c (c.key)}
						{@const sorted = query.sort === c.key}
						<th
							class={cn(
								'h-9 px-3 text-left align-middle font-medium whitespace-nowrap text-foreground',
								kinds[c.key] === 'number' && 'text-right'
							)}
							aria-sort={sorted ? (query.order === 'asc' ? 'ascending' : 'descending') : undefined}
						>
							{#if c.sortable}
								<button
									class={cn(
										'-mx-1 inline-flex items-center gap-1 rounded px-1 hover:bg-accent',
										kinds[c.key] === 'number' && 'flex-row-reverse'
									)}
									onclick={() => sortBy(c)}
								>
									{c.label}
									{#if sorted && query.order === 'asc'}
										<ArrowUpIcon class="size-3.5" />
									{:else if sorted}
										<ArrowDownIcon class="size-3.5" />
									{:else}
										<ChevronsUpDownIcon class="size-3.5 text-muted-foreground/60" />
									{/if}
								</button>
							{:else}
								{c.label}
							{/if}
						</th>
					{/each}
				</tr>
			</thead>
			<tbody>
				{#if virtual && start > 0}
					<tr aria-hidden="true" style:height="{start * ROW_HEIGHT}px"></tr>
				{/if}
				{#each visible as row, i (rowKey(row))}
					{@const key = rowKey(row)}
					<tr
						class="h-9 border-b transition-colors hover:bg-muted/50 data-[selected=true]:bg-muted"
						data-selected={selected.has(key)}
						aria-rowindex={first + start + i + 1}
					>
						{#if selectable}
							<td class="px-3">
								<Checkbox
									aria-label="Select the row"
									checked={selected.has(key)}
									onCheckedChange={(on) => (on ? selected.add(key) : selected.delete(key))}
								/>
							</td>
						{/if}
						{#each columns as c (c.key)}
							{@const kind = kinds[c.key]}
							{@const to = href?.(row, c)}
							<td
								class={cn(
									'max-w-96 truncate px-3 align-middle whitespace-nowrap',
									kind === 'number' && 'text-right tabular-nums'
								)}
								title={String(row[c.key] ?? '')}
							>
								{#if to}
									<a class="text-primary underline-offset-4 hover:underline" href={to}
										>{text(row[c.key], kind)}</a
									>
								{:else if kind === 'flag'}
									<span class={cn(row[c.key] !== 'true' && 'text-muted-foreground')}>
										{row[c.key] === 'true' ? 'Yes' : 'No'}
									</span>
								{:else}
									{text(row[c.key], kind)}
								{/if}
							</td>
						{/each}
					</tr>
				{/each}
				{#if virtual && end < rows.length}
					<tr aria-hidden="true" style:height="{(rows.length - end) * ROW_HEIGHT}px"></tr>
				{/if}
				{#if rows.length === 0}
					<tr>
						<td
							colspan={columns.length + (selectable ? 1 : 0)}
							class="h-24 text-center text-muted-foreground"
						>
							{loading
								? 'Loading…'
								: query.filters.length
									? 'Nothing matches the filters.'
									: 'None.'}
						</td>
					</tr>
				{/if}
			</tbody>
		</table>
	</div>

	<div class="flex flex-wrap items-center justify-between gap-2 text-sm">
		<span class="text-muted-foreground" aria-live="polite">
			{#if count}{formatNumber(first)}–{formatNumber(last)} of {formatNumber(count)}{:else}No rows{/if}
		</span>
		<div class="flex items-center gap-1">
			<label class="mr-2 flex items-center gap-2 text-muted-foreground">
				Rows
				<select
					class="h-7 rounded-md border border-input bg-background px-1.5 text-sm text-foreground dark:bg-input/30"
					value={query.size}
					onchange={(e) => onquery({ ...query, size: Number(e.currentTarget.value), page: 1 })}
				>
					{#each [...new Set([...PAGE_SIZES, query.size])].sort((a, b) => a - b) as size (size)}
						<option value={size}>{size}</option>
					{/each}
				</select>
			</label>
			<Button
				variant="ghost"
				size="icon-sm"
				aria-label="First page"
				disabled={query.page <= 1}
				onclick={() => toPage(1)}><ChevronsLeftIcon /></Button
			>
			<Button
				variant="ghost"
				size="icon-sm"
				aria-label="Previous page"
				disabled={query.page <= 1}
				onclick={() => toPage(query.page - 1)}><ChevronLeftIcon /></Button
			>
			<span class="px-1 tabular-nums">{query.page} / {lastPage}</span>
			<Button
				variant="ghost"
				size="icon-sm"
				aria-label="Next page"
				disabled={query.page >= lastPage}
				onclick={() => toPage(query.page + 1)}><ChevronRightIcon /></Button
			>
			<Button
				variant="ghost"
				size="icon-sm"
				aria-label="Last page"
				disabled={query.page >= lastPage}
				onclick={() => toPage(lastPage)}><ChevronsRightIcon /></Button
			>
		</div>
	</div>
</div>
