<!--
	One operation of a resource: a form for its parameters, typed by their
	Java types, and what it answered. One the guard refuses says so.
-->
<script lang="ts">
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import LockIcon from '@lucide/svelte/icons/lock';
	import * as Collapsible from '#lib/components/ui/collapsible/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { Textarea } from '#lib/components/ui/textarea/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { resources, type Operation } from '#lib/api/index.js';
	import { argument, display, inputFor, typeLabel } from '#lib/explorer.js';

	let { resource, operation }: { resource: string; operation: Operation } = $props();

	let values = $state<(string | boolean)[]>([]);
	let running = $state(false);
	let answer = $state<{ ok: boolean; text: string } | null>(null);

	$effect(() => {
		values = operation.params.map((p) => (inputFor(p.type) === 'boolean' ? false : ''));
		answer = null;
	});

	const signature = $derived(
		`${operation.name}(${operation.params.map((p) => typeLabel(p.type)).join(', ')})`
	);
	const id = (i: number) => `${operation.signature}-${i}`;

	async function run(e: SubmitEvent) {
		e.preventDefault();
		running = true;
		answer = null;
		try {
			const args = operation.params.map((p, i) => argument(p.type, values[i]));
			const result = await resources.invoke(resource, operation.signature, args);
			answer = {
				ok: true,
				text:
					operation.returns === 'void'
						? 'Done.'
						: display(result, typeof result === 'object' && result !== null)
			};
		} catch (err) {
			answer = { ok: false, text: (err as Error).message };
		} finally {
			running = false;
		}
	}
</script>

<Collapsible.Root class="rounded-lg border">
	<Collapsible.Trigger
		class="group flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted/50"
	>
		<ChevronRightIcon
			class="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-90"
		/>
		<span class="min-w-0 flex-1 truncate font-mono">{signature}</span>
		<span class="hidden font-mono text-xs text-muted-foreground sm:inline">
			{typeLabel(operation.returns)}
		</span>
		{#if !operation.allowed}
			<Badge variant="outline" class="gap-1"><LockIcon class="size-3" /> Not permitted</Badge>
		{:else if operation.impact === 'ACTION' || operation.impact === 'ACTION_INFO'}
			<Badge variant="secondary">Changes</Badge>
		{/if}
	</Collapsible.Trigger>
	<Collapsible.Content class="grid gap-3 border-t px-3 py-3">
		<p class="text-sm text-muted-foreground">{operation.description}</p>
		<form class="grid gap-3" onsubmit={run}>
			{#each operation.params as p, i (i)}
				{@const kind = inputFor(p.type)}
				<div class="grid gap-1.5">
					{#if kind === 'boolean'}
						<Label class="font-normal">
							<Checkbox checked={values[i] === true} onCheckedChange={(c) => (values[i] = c)} />
							{p.name}
							<span class="font-mono text-xs text-muted-foreground">{typeLabel(p.type)}</span>
						</Label>
					{:else}
						<Label for={id(i)}>
							{p.name}
							<span class="font-mono text-xs font-normal text-muted-foreground">
								{typeLabel(p.type)}
							</span>
						</Label>
						{#if kind === 'json'}
							<Textarea
								id={id(i)}
								class="font-mono text-xs"
								rows={3}
								placeholder="JSON"
								value={String(values[i])}
								oninput={(e) => (values[i] = e.currentTarget.value)}
							/>
						{:else}
							<Input
								id={id(i)}
								class="font-mono"
								inputmode={kind === 'number' ? 'decimal' : undefined}
								value={String(values[i])}
								oninput={(e) => (values[i] = e.currentTarget.value)}
							/>
						{/if}
					{/if}
					{#if p.description && p.description !== p.name}
						<span class="text-xs text-muted-foreground">{p.description}</span>
					{/if}
				</div>
			{/each}
			<div>
				<Button type="submit" size="sm" disabled={running || !operation.allowed}>
					{running ? 'Running…' : 'Run'}
				</Button>
			</div>
		</form>
		{#if answer}
			<pre
				role={answer.ok ? 'status' : 'alert'}
				class="max-h-96 overflow-auto rounded-md bg-muted p-3 font-mono text-xs break-all whitespace-pre-wrap {answer.ok
					? ''
					: 'text-destructive'}">{answer.text}</pre>
		{/if}
	</Collapsible.Content>
</Collapsible.Root>
