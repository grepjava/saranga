<!--
	A resource's attributes: their values, those the user may write edited in
	place, and numeric ones that can be charted.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import PencilIcon from '@lucide/svelte/icons/pencil';
	import ChartLineIcon from '@lucide/svelte/icons/chart-line';
	import LockIcon from '@lucide/svelte/icons/lock';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import * as Tooltip from '#lib/components/ui/tooltip/index.js';
	import { resources, type Attribute } from '#lib/api/index.js';
	import { argument, display, inputFor, numeric, typeLabel } from '#lib/explorer.js';
	import { scrollable } from '#lib/scrollable.js';

	let {
		resource,
		attributes,
		filter = '',
		charted,
		onchart,
		onwritten
	}: {
		resource: string;
		attributes: Attribute[];
		filter?: string;
		/** The attributes being charted. */
		charted?: ReadonlySet<string>;
		onchart?: (name: string) => void;
		onwritten?: () => void;
	} = $props();

	const shown = $derived(
		attributes.filter((a) => a.name.toLowerCase().includes(filter.trim().toLowerCase()))
	);

	let editing = $state<string | null>(null);
	let draft = $state<string | boolean>('');

	function edit(a: Attribute) {
		editing = a.name;
		draft = inputFor(a.type) === 'boolean' ? a.value === true : display(a.value);
	}

	async function save(a: Attribute) {
		try {
			let value = argument(a.type, draft);
			if (inputFor(a.type) === 'json' && typeof value === 'string') value = JSON.parse(value);
			await resources.write(resource, a.name, value);
			toast.success(`Wrote ${a.name}`);
			editing = null;
			onwritten?.();
		} catch (e) {
			toast.error(`${a.name} cannot be written`, { description: (e as Error).message });
		}
	}
</script>

<div class="overflow-x-auto rounded-lg border">
	<Table.Root>
		<Table.Header>
			<Table.Row>
				<Table.Head>Attribute</Table.Head>
				<Table.Head>Value</Table.Head>
				<Table.Head>Type</Table.Head>
				<Table.Head class="w-16"><span class="sr-only">Actions</span></Table.Head>
			</Table.Row>
		</Table.Header>
		<Table.Body>
			{#each shown as a (a.name)}
				<Table.Row>
					<Table.Cell class="align-top font-medium">
						<Tooltip.Root>
							<Tooltip.Trigger class="text-left">{a.name}</Tooltip.Trigger>
							<Tooltip.Content class="max-w-sm">{a.description}</Tooltip.Content>
						</Tooltip.Root>
					</Table.Cell>
					<Table.Cell class="max-w-[36rem] align-top">
						{#if editing === a.name}
							<form
								class="flex items-center gap-2"
								onsubmit={(e) => {
									e.preventDefault();
									save(a);
								}}
							>
								{#if inputFor(a.type) === 'boolean'}
									<select
										aria-label="New value of {a.name}"
										class="h-8 rounded-lg border border-input bg-background px-2 text-sm"
										value={String(draft)}
										onchange={(e) => (draft = e.currentTarget.value === 'true')}
									>
										<option value="true">true</option>
										<option value="false">false</option>
									</select>
								{:else}
									<Input
										aria-label="New value of {a.name}"
										class="h-8 font-mono"
										value={String(draft)}
										oninput={(e) => (draft = e.currentTarget.value)}
									/>
								{/if}
								<Button type="submit" size="xs">Save</Button>
								<Button size="xs" variant="ghost" onclick={() => (editing = null)}>Cancel</Button>
							</form>
						{:else if a.denied}
							<span class="inline-flex items-center gap-1 text-muted-foreground">
								<LockIcon class="size-3.5" /> Not permitted
							</span>
						{:else if a.error !== undefined}
							<span class="text-muted-foreground">{a.error}</span>
						{:else if !a.readable}
							<span class="text-muted-foreground">Write only</span>
						{:else}
							<pre
								{@attach scrollable}
								class="max-h-40 overflow-auto font-mono text-xs break-all whitespace-pre-wrap">{display(
									a.value,
									typeof a.value === 'object' && a.value !== null
								)}</pre>
						{/if}
					</Table.Cell>
					<Table.Cell class="align-top font-mono text-xs text-muted-foreground">
						{typeLabel(a.type)}
					</Table.Cell>
					<Table.Cell class="align-top">
						<div class="flex justify-end gap-1">
							{#if onchart && numeric(a.value)}
								<Button
									size="icon-sm"
									variant={charted?.has(a.name) ? 'secondary' : 'ghost'}
									aria-label="Chart {a.name}"
									aria-pressed={charted?.has(a.name) ?? false}
									onclick={() => onchart(a.name)}
								>
									<ChartLineIcon />
								</Button>
							{/if}
							{#if a.writable && editing !== a.name}
								<Button
									size="icon-sm"
									variant="ghost"
									aria-label="Edit {a.name}"
									onclick={() => edit(a)}
								>
									<PencilIcon />
								</Button>
							{/if}
						</div>
					</Table.Cell>
				</Table.Row>
			{:else}
				<Table.Row>
					<Table.Cell colspan={4} class="py-6 text-center text-muted-foreground">
						No attribute matches.
					</Table.Cell>
				</Table.Row>
			{/each}
		</Table.Body>
	</Table.Root>
</div>
