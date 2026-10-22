<!--
	Sends a text message to an address or a queue, as Artemis's console does:
	string properties, a body, durable, and as the logged-in user or as
	another. Opened with a message, it resends that message's body and string
	properties.
-->
<script lang="ts" module>
	export interface Draft {
		headers: { name: string; value: string }[];
		body: string;
		durable: boolean;
	}
</script>

<script lang="ts">
	import { toast } from 'svelte-sonner';
	import PlusIcon from '@lucide/svelte/icons/plus';
	import XIcon from '@lucide/svelte/icons/x';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import CodeEditor from '#lib/components/CodeEditor.svelte';
	import { send } from '#lib/broker.js';
	import { pretty } from '#lib/body.js';

	let {
		open = $bindable(false),
		resource,
		target,
		draft,
		onsent
	}: {
		open?: boolean;
		/** The address's or queue's resource. */
		resource: string;
		/** What it is, for the title: `address orders`. */
		target: string;
		/** What the form starts with. */
		draft?: Draft;
		onsent?: (id: string) => void;
	} = $props();

	let headers = $state<{ name: string; value: string }[]>([]);
	let body = $state('');
	let durable = $state(true);
	let createMessageId = $state(true);
	let asMe = $state(true);
	let user = $state('');
	let password = $state('');
	let sending = $state(false);
	let error = $state<string | null>(null);

	// Each opening starts from the draft.
	$effect(() => {
		if (!open) return;
		headers = draft?.headers.map((h) => ({ ...h })) ?? [];
		body = draft?.body ?? '';
		durable = draft?.durable ?? true;
		error = null;
	});

	const names = $derived(headers.map((h) => h.name.trim()).filter(Boolean));
	const duplicate = $derived(names.find((n, i) => names.indexOf(n) !== i));
	const formatted = $derived(pretty(body));

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		if (duplicate) return;
		sending = true;
		error = null;
		try {
			const id = await send(resource, {
				headers: Object.fromEntries(
					headers.filter((h) => h.name.trim()).map((h) => [h.name.trim(), h.value])
				),
				body,
				durable,
				createMessageId,
				credentials: asMe ? undefined : { user, password }
			});
			toast.success(`Sent message ${id}`, { description: `To ${target}` });
			onsent?.(id);
			open = false;
		} catch (err) {
			error = (err as Error).message;
		} finally {
			sending = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
		<Dialog.Header>
			<Dialog.Title>Send a message</Dialog.Title>
			<Dialog.Description>A text message to {target}.</Dialog.Description>
		</Dialog.Header>
		<form id="send-message" class="grid gap-4" onsubmit={submit}>
			<fieldset class="grid gap-2">
				<div class="flex items-center justify-between">
					<legend class="text-sm font-medium">Properties</legend>
					<Button
						variant="ghost"
						size="xs"
						onclick={() => (headers = [...headers, { name: '', value: '' }])}
					>
						<PlusIcon /> Add
					</Button>
				</div>
				{#each headers as h, i (i)}
					<div class="flex gap-2">
						<Input
							aria-label="Property name"
							placeholder="Name"
							bind:value={h.name}
							aria-invalid={h.name.trim() !== '' && h.name.trim() === duplicate}
						/>
						<Input aria-label="Property value" placeholder="Value" bind:value={h.value} />
						<Button
							variant="ghost"
							size="icon"
							aria-label="Remove the property"
							onclick={() => (headers = headers.filter((_, j) => j !== i))}
						>
							<XIcon />
						</Button>
					</div>
				{:else}
					<p class="text-xs text-muted-foreground">None. Each one is a string property.</p>
				{/each}
				{#if duplicate}
					<p class="text-xs text-destructive">{duplicate} is there twice.</p>
				{/if}
			</fieldset>
			<div class="grid gap-2">
				<div class="flex items-center justify-between">
					<Label>Body</Label>
					{#if formatted && formatted !== body}
						<Button variant="ghost" size="xs" onclick={() => (body = formatted)}>Format</Button>
					{/if}
				</div>
				<CodeEditor bind:value={body} label="Body" class="h-56" placeholder="The message's text" />
			</div>
			<div class="flex flex-wrap gap-x-6 gap-y-3">
				<Label class="font-normal"><Checkbox bind:checked={durable} /> Durable</Label>
				<Label class="font-normal">
					<Checkbox bind:checked={createMessageId} /> Give it a message ID
				</Label>
				<Label class="font-normal"><Checkbox bind:checked={asMe} /> As me</Label>
			</div>
			{#if !asMe}
				<div class="grid gap-3 sm:grid-cols-2">
					<div class="grid gap-1.5">
						<Label for="send-user">User</Label>
						<Input id="send-user" autocomplete="off" bind:value={user} />
					</div>
					<div class="grid gap-1.5">
						<Label for="send-password">Password</Label>
						<Input id="send-password" type="password" autocomplete="off" bind:value={password} />
					</div>
				</div>
			{/if}
			{#if error}
				<p class="text-sm text-destructive" role="alert">{error}</p>
			{/if}
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button type="submit" form="send-message" disabled={sending || !!duplicate}>
				{sending ? 'Sending…' : 'Send'}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
