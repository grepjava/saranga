<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { untrack } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import ArrowLeftIcon from '@lucide/svelte/icons/arrow-left';
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import * as Collapsible from '#lib/components/ui/collapsible/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import * as Card from '#lib/components/ui/card/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import AttributeTable from '#lib/components/AttributeTable.svelte';
	import OperationForm from '#lib/components/OperationForm.svelte';
	import Chart from '#lib/components/Chart.svelte';
	import {
		live as history,
		resources,
		type Resource,
		type ResourceSummary,
		type View
	} from '#lib/api/index.js';
	import { GROUPS, isInternal, numeric, shortName } from '#lib/explorer.js';
	import { formatNumber } from '#lib/format.js';
	import { addressHref, explorerHref, hashQuery, queueHref } from '#lib/nav.js';
	import { prefs } from '#lib/state/prefs.svelte.js';
	import { attributeValues, live } from '#lib/state/live.svelte.js';
	import { prepend } from '#lib/series.js';
	import { cn } from '#lib/utils.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	/** The most of a group the tree lists; a search finds the others. */
	const LISTED = 200;
	/** The points a chart keeps, every tick, after the broker's history. */
	const POINTS = 2000;

	const selected = $derived(hashQuery(page.url).get('r'));

	let all = $state<ResourceSummary[]>([]);
	let listError = $state<string | null>(null);
	let search = $state('');
	let tick = $state(0);

	$effect(() => {
		void tick;
		resources
			.list()
			.then((r) => {
				all = r;
				listError = null;
			})
			.catch((e: Error) => (listError = e.message));
	});

	const groups = $derived.by(() => {
		const q = search.trim().toLowerCase();
		return GROUPS.map((g) => {
			const members = all
				.filter((r) => r.type === g.type)
				.filter(
					(r) =>
						prefs.showInternal ||
						!((r.type === 'address' || r.type === 'queue') && isInternal(shortName(r)))
				)
				.filter((r) => !q || r.resource.toLowerCase().includes(q))
				.sort((a, b) => shortName(a).localeCompare(shortName(b)));
			return { ...g, members };
		}).filter((g) => g.members.length);
	});

	let detail = $state<Resource | null>(null);
	let detailError = $state<string | null>(null);
	let attributeFilter = $state('');
	let operationFilter = $state('');

	$effect(() => {
		const r = selected;
		void tick;
		detailError = null;
		if (!r) {
			detail = null;
			return;
		}
		let stale = false;
		resources
			.get(r)
			.then((d) => !stale && (detail = d))
			.catch((e: Error) => {
				if (stale) return;
				detail = null;
				detailError = e.message;
			});
		return () => {
			stale = true;
		};
	});

	// A change of resource clears the charts.
	const charted = new SvelteSet<string>();
	let series = $state<number[][]>([[]]);
	let chartedOf = '';
	$effect(() => {
		if (selected !== chartedOf) {
			chartedOf = selected ?? '';
			charted.clear();
			series = [[]];
		}
	});

	function toggleChart(name: string) {
		if (charted.has(name)) charted.delete(name);
		else charted.add(name);
		series = [[], ...[...charted].map(() => [])];
	}

	// The charted attributes, as the live stream reads them every tick.
	const chartView = $derived<View | null>(
		selected && charted.size
			? { kind: 'attributes', resource: selected, names: [...charted] }
			: null
	);
	$effect(() => {
		const v = chartView;
		if (v) return live.watch(v);
	});

	$effect(() => {
		const v = chartView;
		if (!v) return;
		const names = v.names ?? [];
		let stale = false;
		// The broker's history of those it keeps, before what comes now.
		history
			.history([v.resource ?? ''])
			.then((h) => {
				const past = h[v.resource ?? ''];
				if (stale || !past?.times?.length) return;
				const now = untrack(() => series);
				const merged = prepend(
					{ times: past.times, attributes: past.attributes ?? {} },
					names,
					(now[0] ?? []).map((t) => t * 1000),
					now.slice(1)
				);
				series = [
					merged.times.map((t) => t / 1000),
					...merged.values.map((s) => s.map((x) => x ?? NaN))
				];
			})
			.catch(() => {});
		const off = live.onTick((t) => {
			const values = attributeValues(untrack(() => live.value(v)));
			if (!values) return;
			// The samples so far, which the next one does not depend on.
			const series0 = untrack(() => series);
			const next = [
				[...(series0[0] ?? []), t / 1000],
				...names.map((n, i) => {
					const x = values[n];
					return [...(series0[i + 1] ?? []), numeric(x) ? x : NaN];
				})
			];
			series = next.map((s) => s.slice(-POINTS));
		});
		return () => {
			stale = true;
			off();
		};
	});

	const operations = $derived(
		(detail?.operations ?? []).filter((o) =>
			o.name.toLowerCase().includes(operationFilter.trim().toLowerCase())
		)
	);

	const pageOf = $derived.by(() => {
		if (!detail) return undefined;
		const name = shortName(detail);
		if (detail.type === 'queue') return queueHref(params.node, name);
		if (detail.type === 'address') return addressHref(params.node, name);
		return undefined;
	});
</script>

<svelte:head>
	<title>{selected ? `${selected} · ` : ''}Explorer · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	<div class="flex items-center justify-between gap-3">
		<h1 class="text-xl font-semibold">Explorer</h1>
		<Button variant="ghost" size="icon-sm" aria-label="Refresh" onclick={() => tick++}>
			<RefreshCwIcon />
		</Button>
	</div>

	<div class="grid gap-4 md:grid-cols-[18rem_1fr]">
		<nav
			aria-label="Resources"
			class={cn(
				'flex flex-col gap-2 md:max-h-[calc(100vh-10rem)] md:overflow-y-auto',
				selected && 'hidden md:flex'
			)}
		>
			<Input aria-label="Find a resource" placeholder="Find a resource" bind:value={search} />
			{#if listError}
				<p class="text-sm text-destructive">{listError}</p>
			{:else if all.length === 0}
				<Skeleton class="h-40 w-full" />
			{/if}
			{#each groups as g (g.type)}
				<Collapsible.Root
					open={g.type === 'broker' ||
						!!search.trim() ||
						g.members.some((m) => m.resource === selected)}
				>
					<Collapsible.Trigger
						class="group flex w-full items-center gap-1 rounded-md px-2 py-1 text-sm font-medium hover:bg-muted"
					>
						<ChevronRightIcon
							class="size-4 text-muted-foreground transition-transform group-data-[state=open]:rotate-90"
						/>
						<span class="flex-1 text-left">{g.label}</span>
						<span class="text-xs text-muted-foreground tabular-nums">
							{formatNumber(g.members.length)}
						</span>
					</Collapsible.Trigger>
					<Collapsible.Content>
						<ul class="ml-5 grid border-l py-1">
							{#each g.members.slice(0, LISTED) as r (r.resource)}
								<li>
									<a
										href={explorerHref(params.node, r.resource)}
										aria-current={r.resource === selected ? 'page' : undefined}
										class="block truncate rounded-r-md px-2 py-0.5 font-mono text-xs hover:bg-muted aria-[current=page]:bg-accent aria-[current=page]:text-accent-foreground"
									>
										{shortName(r)}
									</a>
								</li>
							{/each}
							{#if g.members.length > LISTED}
								<li class="px-2 py-1 text-xs text-muted-foreground">
									{formatNumber(g.members.length - LISTED)} more: search to find them.
								</li>
							{/if}
						</ul>
					</Collapsible.Content>
				</Collapsible.Root>
			{/each}
		</nav>

		<section class={cn('flex min-w-0 flex-col gap-4', !selected && 'hidden md:flex')}>
			{#if !selected}
				<Card.Root>
					<Card.Content class="text-sm text-muted-foreground">
						Every management resource of the broker, with its attributes and operations, as
						Artemis's console shows its MBeans. Pick one.
					</Card.Content>
				</Card.Root>
			{:else}
				<div class="flex flex-wrap items-start gap-2">
					<Button
						variant="ghost"
						size="icon-sm"
						class="md:hidden"
						aria-label="Back to the resources"
						onclick={() => goto(explorerHref(params.node), { reset: false })}
					>
						<ArrowLeftIcon />
					</Button>
					<div class="grid min-w-0 flex-1 gap-1">
						<h2 class="font-mono text-lg font-semibold break-all">{selected}</h2>
						{#if detail}
							<p class="font-mono text-xs break-all text-muted-foreground">{detail.objectName}</p>
						{/if}
					</div>
					{#if pageOf}
						<Button size="sm" variant="outline" href={pageOf}>Its page</Button>
					{/if}
				</div>
				{#if detailError}
					<Alert.Root variant="destructive">
						<Alert.Title>The resource cannot be read</Alert.Title>
						<Alert.Description>{detailError}</Alert.Description>
					</Alert.Root>
				{:else if !detail}
					<Skeleton class="h-60 w-full" />
				{:else}
					{#if charted.size}
						<Card.Root>
							<Card.Header>
								<Card.Title class="text-sm">{[...charted].join(', ')}</Card.Title>
								<Card.Description>Read every two seconds</Card.Description>
							</Card.Header>
							<Card.Content>
								<Chart data={series} labels={[...charted]} format={(v) => formatNumber(v)} />
							</Card.Content>
						</Card.Root>
					{/if}
					<Tabs.Root value="attributes">
						<Tabs.List>
							<Tabs.Trigger value="attributes">
								Attributes ({detail.attributes.length})
							</Tabs.Trigger>
							<Tabs.Trigger value="operations">
								Operations ({detail.operations.length})
							</Tabs.Trigger>
						</Tabs.List>
						<Tabs.Content value="attributes" class="mt-3 grid gap-3">
							<Input
								aria-label="Find an attribute"
								placeholder="Find an attribute"
								bind:value={attributeFilter}
							/>
							<AttributeTable
								resource={selected}
								attributes={detail.attributes}
								filter={attributeFilter}
								{charted}
								onchart={toggleChart}
								onwritten={() => tick++}
							/>
						</Tabs.Content>
						<Tabs.Content value="operations" class="mt-3 grid gap-2">
							<Input
								aria-label="Find an operation"
								placeholder="Find an operation"
								bind:value={operationFilter}
							/>
							{#each operations as o (o.signature)}
								<OperationForm resource={selected} operation={o} />
							{:else}
								<p class="text-sm text-muted-foreground">No operation matches.</p>
							{/each}
						</Tabs.Content>
					</Tabs.Root>
				{/if}
			{/if}
		</section>
	</div>
</div>
