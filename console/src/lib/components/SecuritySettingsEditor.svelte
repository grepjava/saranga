<!--
	Adds or replaces the roles of a security match (`addSecuritySettings`
	with the settings as JSON): each role and the permissions it grants on the
	addresses the match covers.
-->
<script lang="ts">
	import { toast } from 'svelte-sonner';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import XIcon from '@lucide/svelte/icons/x';
	import type { Role } from '#lib/api/index.js';
	import { PERMISSIONS, security, type Permission } from '#lib/broker.js';
	import { permissionLabel } from '#lib/security.js';

	let {
		open = $bindable(false),
		match = '',
		roles = [],
		onsaved
	}: {
		open?: boolean;
		/** The match edited; none for a new one. */
		match?: string;
		roles?: Role[];
		onsaved?: (match: string) => void;
	} = $props();

	type Row = { name: string } & Record<Permission, boolean>;

	let name = $state('');
	let rows = $state<Row[]>([]);
	let role = $state('');
	let busy = $state(false);
	let error = $state<string | null>(null);

	const none = (n: string): Row =>
		({ name: n, ...Object.fromEntries(PERMISSIONS.map((p) => [p, false])) }) as Row;

	$effect(() => {
		if (!open) return;
		name = match;
		rows = roles.map((r) => ({ ...none(r.name), ...pick(r) }));
		role = '';
		error = null;
	});

	function pick(r: Role): Record<Permission, boolean> {
		return Object.fromEntries(PERMISSIONS.map((p) => [p, r[p] === true])) as Record<
			Permission,
			boolean
		>;
	}

	function add(e: SubmitEvent) {
		e.preventDefault();
		const n = role.trim();
		if (n && !rows.some((r) => r.name === n)) rows.push(none(n));
		role = '';
	}

	async function submit() {
		busy = true;
		error = null;
		const m = name.trim();
		const grants = Object.fromEntries(
			PERMISSIONS.map((p) => [p, rows.filter((r) => r[p]).map((r) => r.name)])
		) as Record<Permission, string[]>;
		try {
			await security.addSecuritySettings(m, grants);
			toast.success(`Saved the roles of ${m}`);
			onsaved?.(m);
			open = false;
		} catch (err) {
			error = (err as Error).message;
		} finally {
			busy = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="max-h-[90vh] overflow-y-auto sm:max-w-4xl">
		<Dialog.Header>
			<Dialog.Title>{match ? `Roles of ${match}` : 'Add security settings'}</Dialog.Title>
			<Dialog.Description>
				The most specific match that covers an address decides its permissions: these replace the
				match's roles.
			</Dialog.Description>
		</Dialog.Header>
		<div class="grid gap-4">
			<div class="grid max-w-sm gap-1.5">
				<Label for="security-match">Match</Label>
				<Input
					id="security-match"
					required
					autocomplete="off"
					placeholder="orders.#"
					readonly={!!match}
					bind:value={name}
				/>
			</div>
			<div class="overflow-x-auto rounded-md border">
				<table class="w-full text-sm">
					<thead>
						<tr class="border-b">
							<th scope="col" class="p-2 text-left font-medium">Role</th>
							{#each PERMISSIONS as p (p)}
								<th scope="col" class="p-2 text-xs font-medium">{permissionLabel(p)}</th>
							{/each}
							<th scope="col"><span class="sr-only">Remove</span></th>
						</tr>
					</thead>
					<tbody>
						{#each rows as r, i (r.name)}
							<tr class="border-b last:border-0">
								<th scope="row" class="p-2 text-left font-mono font-normal">{r.name}</th>
								{#each PERMISSIONS as p (p)}
									<td class="p-2 text-center">
										<Checkbox bind:checked={r[p]} aria-label="{r.name} may {permissionLabel(p)}" />
									</td>
								{/each}
								<td class="p-1">
									<Button
										variant="ghost"
										size="icon-sm"
										aria-label="Remove {r.name}"
										onclick={() => rows.splice(i, 1)}><XIcon /></Button
									>
								</td>
							</tr>
						{:else}
							<tr>
								<td colspan={PERMISSIONS.length + 2} class="p-2 text-muted-foreground">
									No role: nobody may do anything on these addresses.
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
			<form class="flex max-w-sm gap-2" onsubmit={add}>
				<Input aria-label="Role to add" placeholder="A role" autocomplete="off" bind:value={role} />
				<Button type="submit" variant="outline" size="sm" disabled={!role.trim()}>Add role</Button>
			</form>
			{#if error}<p class="text-sm text-destructive" role="alert">{error}</p>{/if}
		</div>
		<Dialog.Footer>
			<Button variant="outline" onclick={() => (open = false)}>Cancel</Button>
			<Button disabled={busy || !name.trim()} onclick={submit}>Save</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
