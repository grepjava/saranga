<!-- Creates an address, anycast, multicast or both (`createAddress`). -->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { broker } from '#lib/broker.js';

	let {
		open = $bindable(false),
		oncreated
	}: { open?: boolean; oncreated?: (name: string) => void } = $props();

	let name = $state('');
	let anycast = $state(true);
	let multicast = $state(false);
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		if (!open) return;
		name = '';
		anycast = true;
		multicast = false;
		error = null;
	});

	const routing = $derived(
		[anycast && 'ANYCAST', multicast && 'MULTICAST'].filter(Boolean).join(',')
	);

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		busy = true;
		error = null;
		try {
			await broker.createAddress(name.trim(), routing);
			toast.success(`Created address ${name.trim()}`);
			oncreated?.(name.trim());
			open = false;
		} catch (err) {
			error = (err as Error).message;
		} finally {
			busy = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>Create an address</Dialog.Title>
			<Dialog.Description>Queues are bound to it once it is there.</Dialog.Description>
		</Dialog.Header>
		<form id="create-address" class="grid gap-4" onsubmit={submit}>
			<div class="grid gap-1.5">
				<Label for="address-name">Name</Label>
				<Input id="address-name" required autocomplete="off" bind:value={name} />
			</div>
			<fieldset class="grid gap-2">
				<legend class="mb-1 text-sm font-medium">Routing</legend>
				<Label class="font-normal">
					<Checkbox bind:checked={anycast} /> Anycast: each message to one queue
				</Label>
				<Label class="font-normal">
					<Checkbox bind:checked={multicast} /> Multicast: each message to every queue
				</Label>
			</fieldset>
			{#if error}<p class="text-sm text-destructive" role="alert">{error}</p>{/if}
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button type="submit" form="create-address" disabled={busy || !name.trim() || !routing}>
				Create
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
