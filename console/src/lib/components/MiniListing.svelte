<!--
	The rows of a listing that belong to something, such as a queue's
	consumers: the first page, with its links and a way to the whole listing.
-->
<script lang="ts" generics="K extends Listing">
	import * as Table from '#lib/components/ui/table/index.js';
	import { api, type Listing, type Rows } from '#lib/api/index.js';
	import { LISTINGS, label, type ListingInfo } from '#lib/listings.js';
	import { formatNumber, kindOf } from '#lib/format.js';
	import { linkHref, listingHref } from '#lib/nav.js';
	import type { Filter } from '#lib/query.js';

	let {
		node,
		kind,
		filter,
		columns,
		tick = 0
	}: {
		node: string;
		kind: K;
		filter: Filter;
		columns: (keyof Rows[K] & string)[];
		/** Changed to read the rows again. */
		tick?: number;
	} = $props();

	const SIZE = 50;
	const info = $derived(LISTINGS[kind] as unknown as ListingInfo<K>);
	let rows = $state<Rows[K][]>([]);
	let count = $state(0);
	let error = $state<string | null>(null);

	$effect(() => {
		void tick;
		const f = filter;
		api
			.list(kind, { filters: [f], order: 'asc', page: 1, size: SIZE })
			.then((p) => {
				rows = p.data as Rows[K][];
				count = p.count;
				error = null;
			})
			.catch((e: Error) => (error = e.message));
	});

	const numeric = $derived(
		Object.fromEntries(
			columns.map((c) => [c, kindOf(rows.map((r) => r[c])) === 'number' && !/(^id|ID)$/.test(c)])
		)
	);
	const all = $derived(
		listingHref(
			node,
			kind,
			new URLSearchParams({ filter: `${filter.field}:${filter.operation}:${filter.value}` })
		)
	);

	function href(c: string, value: unknown): string | undefined {
		const link = info.links?.[c as keyof Rows[K] & string];
		if (!link || value === '' || value == null) return undefined;
		return linkHref(node, link, String(value));
	}
</script>

{#if error}
	<p class="text-sm text-destructive">{error}</p>
{:else}
	<div class="overflow-x-auto rounded-lg border">
		<Table.Root>
			<Table.Header>
				<Table.Row>
					{#each columns as c (c)}
						<Table.Head class={numeric[c] ? 'text-right' : ''}>{label(c)}</Table.Head>
					{/each}
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#each rows as row, i (i)}
					<Table.Row>
						{#each columns as c (c)}
							{@const v = row[c]}
							{@const to = href(c, v)}
							<Table.Cell class={numeric[c] ? 'text-right tabular-nums' : ''}>
								{#if to}
									<a class="text-primary underline-offset-4 hover:underline" href={to}>{v}</a>
								{:else}
									{numeric[c] ? formatNumber(v) : String(v ?? '')}
								{/if}
							</Table.Cell>
						{/each}
					</Table.Row>
				{:else}
					<Table.Row>
						<Table.Cell colspan={columns.length} class="py-6 text-center text-muted-foreground">
							None.
						</Table.Cell>
					</Table.Row>
				{/each}
			</Table.Body>
		</Table.Root>
	</div>
	{#if count > 0}
		<a class="text-sm text-primary underline-offset-4 hover:underline" href={all}>
			{count > SIZE ? `All ${formatNumber(count)} in ${info.title}` : `In ${info.title}`}
		</a>
	{/if}
{/if}
