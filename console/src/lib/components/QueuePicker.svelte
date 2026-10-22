<!-- Asks for a queue to move or copy messages to, offering the broker's queues. -->
<script lang="ts">
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { api } from '#lib/api/index.js';

	let {
		open = $bindable(false),
		title,
		description,
		action,
		exclude,
		onpick
	}: {
		open?: boolean;
		title: string;
		description: string;
		/** The button: `Move`. */
		action: string;
		/** The queue the messages are on. */
		exclude?: string;
		onpick: (queue: string) => void;
	} = $props();

	let target = $state('');
	let queues = $state<string[]>([]);

	$effect(() => {
		if (!open) return;
		target = '';
		api
			.list('queues', { filters: [], sort: 'name', order: 'asc', page: 1, size: 1000 })
			.then((p) => (queues = p.data.map((q) => q.name).filter((n) => n !== exclude)))
			.catch(() => (queues = []));
	});

	function submit(e: SubmitEvent) {
		e.preventDefault();
		const t = target.trim();
		if (!t) return;
		// Before closing, which may clear what the pick is for.
		onpick(t);
		open = false;
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>{title}</Dialog.Title>
			<Dialog.Description>{description}</Dialog.Description>
		</Dialog.Header>
		<form id="pick-queue" class="grid gap-1.5" onsubmit={submit}>
			<Label for="target-queue">Queue</Label>
			<Input
				id="target-queue"
				list="target-queues"
				autocomplete="off"
				required
				bind:value={target}
			/>
			<datalist id="target-queues">
				{#each queues as q (q)}<option value={q}></option>{/each}
			</datalist>
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button type="submit" form="pick-queue" disabled={!target.trim()}>{action}</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
