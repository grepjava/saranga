<!--
	Adds or replaces the settings of an address match (`addAddressSettings`
	with the settings as JSON): the settings given replace the match's, and
	the others come from the less specific matches.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import XIcon from '@lucide/svelte/icons/x';
	import type { Schemas } from '#lib/api/index.js';
	import { config } from '#lib/broker.js';

	type Name = Schemas['AddressSettingName'];

	let {
		open = $bindable(false),
		match = '',
		settings = {},
		names,
		onsaved
	}: {
		open?: boolean;
		/** The match edited; none for a new one. */
		match?: string;
		settings?: Record<string, unknown>;
		names: Name[];
		onsaved?: (match: string) => void;
	} = $props();

	let name = $state('');
	let rows = $state<{ name: string; value: string }[]>([]);
	let adding = $state('');
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		if (!open) return;
		name = match;
		rows = Object.entries(settings).map(([n, v]) => ({ name: n, value: String(v) }));
		adding = '';
		error = null;
	});

	const kinds = $derived(new Map(names.map((n) => [n.name, n])));
	const unused = $derived(names.filter((n) => !rows.some((r) => r.name === n.name)));

	function add() {
		const n = kinds.get(adding);
		if (!n) return;
		rows.push({ name: n.name, value: n.type === 'boolean' ? 'true' : (n.choices?.[0] ?? '') });
		adding = '';
	}

	/** A row's value as `toJSON` writes the setting: numbers and booleans as such. */
	function value(r: { name: string; value: string }): unknown {
		const kind = kinds.get(r.name)?.type;
		const text = r.value.trim();
		if (kind === 'boolean') return text === 'true';
		if ((kind === 'integer' || kind === 'number') && text !== '' && !isNaN(Number(text)))
			return Number(text);
		return text;
	}

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		busy = true;
		error = null;
		const m = name.trim();
		try {
			await config.addAddressSettings(m, Object.fromEntries(rows.map((r) => [r.name, value(r)])));
			toast.success(`Saved the settings of ${m}`);
			onsaved?.(m);
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
	<Dialog.Content class="max-h-[90vh] overflow-y-auto sm:max-w-xl">
		<Dialog.Header>
			<Dialog.Title>{match ? `Settings of ${match}` : 'Add address settings'}</Dialog.Title>
			<Dialog.Description>
				These replace the match's settings. What they leave out comes from the less specific
				matches, then the defaults.
			</Dialog.Description>
		</Dialog.Header>
		<form id="address-settings" class="grid gap-4" onsubmit={submit}>
			<div class="grid gap-1.5">
				<Label for="settings-match">Match</Label>
				<Input
					id="settings-match"
					required
					autocomplete="off"
					placeholder="orders.#"
					readonly={!!match}
					bind:value={name}
				/>
			</div>
			<fieldset class="grid gap-2">
				<legend class="mb-1 text-sm font-medium">Settings</legend>
				{#each rows as r, i (r.name)}
					{@const kind = kinds.get(r.name)}
					<div class="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] items-center gap-2">
						<Label for="setting-{i}" class="truncate font-mono text-xs">{r.name}</Label>
						{#if kind?.type === 'boolean'}
							<select id="setting-{i}" class={select} bind:value={r.value}>
								<option value="true">true</option>
								<option value="false">false</option>
							</select>
						{:else if kind?.choices?.length}
							<select id="setting-{i}" class={select} bind:value={r.value}>
								{#each kind.choices as c (c)}<option value={c}>{c}</option>{/each}
							</select>
						{:else}
							<Input
								id="setting-{i}"
								autocomplete="off"
								inputmode={kind?.type === 'string' ? undefined : 'decimal'}
								bind:value={r.value}
							/>
						{/if}
						<Button
							variant="ghost"
							size="icon-sm"
							aria-label="Remove {r.name}"
							onclick={() => rows.splice(i, 1)}><XIcon /></Button
						>
					</div>
				{:else}
					<p class="text-sm text-muted-foreground">None: every value comes from other matches.</p>
				{/each}
				<div class="flex gap-2">
					<select class={select} aria-label="Setting to add" bind:value={adding}>
						<option value="">Add a setting…</option>
						{#each unused as n (n.name)}<option value={n.name}>{n.name}</option>{/each}
					</select>
					<Button variant="outline" size="sm" disabled={!adding} onclick={add}>Add</Button>
				</div>
			</fieldset>
			{#if error}<p class="text-sm text-destructive" role="alert">{error}</p>{/if}
		</form>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button type="submit" form="address-settings" disabled={busy || !name.trim()}>Save</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
