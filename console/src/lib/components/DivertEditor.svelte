<!--
	Creates a divert (`createDivert` with its configuration as JSON), or
	changes one (`updateDivert`): its forwarding address, filter, routing type
	and transformer; the rest a divert keeps.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { config, type Divert } from '#lib/broker.js';

	let {
		open = $bindable(false),
		divert,
		onsaved
	}: {
		open?: boolean;
		/** The divert changed; none for a new one. */
		divert?: Divert;
		onsaved?: (name: string) => void;
	} = $props();

	const blank = (): Divert => ({
		name: '',
		address: '',
		forwardingAddress: '',
		routingName: '',
		exclusive: false,
		filter: '',
		routingType: 'STRIP',
		transformerClassName: ''
	});

	let d = $state<Divert>(blank());
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		if (!open) return;
		d = divert ? { ...divert } : blank();
		error = null;
	});

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		busy = true;
		error = null;
		const saved = { ...d, name: d.name.trim(), address: d.address.trim() };
		try {
			await (divert ? config.updateDivert(saved) : config.createDivert(saved));
			toast.success(`${divert ? 'Changed' : 'Created'} divert ${saved.name}`);
			onsaved?.(saved.name);
			open = false;
		} catch (err) {
			error = (err as Error).message;
		} finally {
			busy = false;
		}
	}

	const select =
		'h-8 w-full rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30';
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-lg">
		<Dialog.Header>
			<Dialog.Title>{divert ? `Divert ${divert.name}` : 'Create a divert'}</Dialog.Title>
			<Dialog.Description>
				A divert sends what reaches an address on to another as well, or instead.
			</Dialog.Description>
		</Dialog.Header>
		<form id="divert" class="grid gap-4 sm:grid-cols-2" onsubmit={submit}>
			<div class="grid gap-1.5">
				<Label for="divert-name">Name</Label>
				<Input
					id="divert-name"
					required
					autocomplete="off"
					readonly={!!divert}
					bind:value={d.name}
				/>
			</div>
			<div class="grid gap-1.5">
				<Label for="divert-routing-name">Routing name</Label>
				<Input
					id="divert-routing-name"
					autocomplete="off"
					placeholder="A UUID"
					readonly={!!divert}
					bind:value={d.routingName}
				/>
			</div>
			<div class="grid gap-1.5">
				<Label for="divert-address">Address</Label>
				<Input
					id="divert-address"
					required
					autocomplete="off"
					readonly={!!divert}
					bind:value={d.address}
				/>
			</div>
			<div class="grid gap-1.5">
				<Label for="divert-forwarding">Forwarding address</Label>
				<Input
					id="divert-forwarding"
					required
					autocomplete="off"
					bind:value={d.forwardingAddress}
				/>
			</div>
			<div class="grid gap-1.5 sm:col-span-2">
				<Label for="divert-filter">Filter</Label>
				<Input
					id="divert-filter"
					autocomplete="off"
					placeholder="color = 'red'"
					bind:value={d.filter}
				/>
			</div>
			<div class="grid gap-1.5">
				<Label for="divert-routing-type">Routing type</Label>
				<select id="divert-routing-type" class={select} bind:value={d.routingType}>
					<option value="STRIP">Strip: the forwarding address's own</option>
					<option value="PASS">Pass: the message's</option>
					<option value="ANYCAST">Anycast</option>
					<option value="MULTICAST">Multicast</option>
				</select>
			</div>
			<div class="grid gap-1.5">
				<Label for="divert-transformer">Transformer class</Label>
				<Input id="divert-transformer" autocomplete="off" bind:value={d.transformerClassName} />
			</div>
			<Label class="font-normal sm:col-span-2">
				<Checkbox bind:checked={d.exclusive} disabled={!!divert} /> Exclusive: the message goes to the
				forwarding address only
			</Label>
			{#if error}<p class="text-sm text-destructive sm:col-span-2" role="alert">{error}</p>{/if}
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button
				type="submit"
				form="divert"
				disabled={busy || !d.name.trim() || !d.address.trim() || !d.forwardingAddress.trim()}
			>
				{divert ? 'Save' : 'Create'}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
