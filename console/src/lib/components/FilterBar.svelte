<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import XIcon from '@lucide/svelte/icons/x';
	import PlusIcon from '@lucide/svelte/icons/plus';
	import { OPERATIONS, OPERATION_LABELS, type Filter, type Operation } from '#lib/query.js';

	let {
		fields,
		filters,
		onchange
	}: {
		/** The fields the listing filters by, with their headings. */
		fields: { name: string; label: string }[];
		filters: Filter[];
		onchange: (filters: Filter[]) => void;
	} = $props();

	let field = $state('');
	let operation = $state<Operation>('CONTAINS');
	let value = $state('');

	$effect(() => {
		if (!fields.some((f) => f.name === field))
			field = (fields.find((f) => f.name === 'name') ?? fields[0])?.name ?? '';
	});

	const labelOf = (name: string) => fields.find((f) => f.name === name)?.label ?? name;

	function add(event: SubmitEvent) {
		event.preventDefault();
		if (!field) return;
		onchange([...filters, { field, operation, value }]);
		value = '';
	}

	const select =
		'h-8 rounded-lg border border-input bg-background px-2 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 dark:bg-input/30';
</script>

<div class="flex flex-col gap-2">
	<form class="flex flex-wrap items-center gap-2" onsubmit={add} aria-label="Add a filter">
		<select class={select} bind:value={field} aria-label="Field">
			{#each fields as f (f.name)}
				<option value={f.name}>{f.label}</option>
			{/each}
		</select>
		<select class={select} bind:value={operation} aria-label="Operation">
			{#each OPERATIONS as op (op)}
				<option value={op}>{OPERATION_LABELS[op]}</option>
			{/each}
		</select>
		<Input class="h-8 w-48" placeholder="Value" bind:value aria-label="Value" />
		<Button type="submit" size="sm" variant="secondary">
			<PlusIcon />
			Filter
		</Button>
	</form>
	{#if filters.length}
		<ul class="flex flex-wrap items-center gap-1.5" aria-label="Filters">
			{#each filters as f, i (i)}
				<li
					class="inline-flex items-center gap-1 rounded-md border bg-muted/50 py-0.5 pr-0.5 pl-2 text-xs"
				>
					<span class="font-medium">{labelOf(f.field)}</span>
					<span class="text-muted-foreground">{OPERATION_LABELS[f.operation]}</span>
					<span class="max-w-64 truncate font-mono">{f.value === '' ? '""' : f.value}</span>
					<Button
						variant="ghost"
						size="icon-xs"
						aria-label="Remove the filter"
						onclick={() => onchange(filters.filter((_, j) => j !== i))}
					>
						<XIcon />
					</Button>
				</li>
			{/each}
			{#if filters.length > 1}
				<li>
					<Button variant="link" size="xs" onclick={() => onchange([])}>Clear all</Button>
				</li>
			{/if}
		</ul>
	{/if}
</div>
