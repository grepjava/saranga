<!--
	A node's configuration, changed as it runs: its address settings (every
	match, and an address's settings with the match each comes from), its
	diverts, its acceptors, and its configuration exported as broker
	properties or reloaded from its file.
-->
<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { toast } from 'svelte-sonner';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import AddressSettingsEditor from '#lib/components/AddressSettingsEditor.svelte';
	import DivertEditor from '#lib/components/DivertEditor.svelte';
	import { admin, resources, LOCAL, type AddressSettings } from '#lib/api/index.js';
	import { config, type Divert } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { display } from '#lib/explorer.js';
	import { saveText } from '#lib/files.js';
	import { configurationHref, explorerHref, hashQuery } from '#lib/nav.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();
	const local = $derived(params.node === LOCAL);

	const TABS = ['address-settings', 'diverts', 'acceptors', 'export'] as const;
	const tab = $derived.by(() => {
		const t = hashQuery(page.url).get('tab');
		return TABS.find((x) => x === t) ?? 'address-settings';
	});
	function show(t: string) {
		goto(configurationHref(params.node, new URLSearchParams({ tab: t })), {
			replaceState: true
		});
	}

	let may = $state<Allowed>({});
	$effect(() => {
		allowed('broker', [
			'addAddressSettings',
			'removeAddressSettings',
			'createDivert',
			'updateDivert',
			'destroyDivert',
			'exportConfigAsProperties',
			'reloadConfigurationFile'
		]).then((a) => (may = a));
	});

	// Address settings.
	let settings = $state<AddressSettings | null>(null);
	let settingsError = $state<string | null>(null);
	let settingsTick = $state(0);
	$effect(() => {
		void settingsTick;
		if (!local) return;
		admin
			.addressSettings()
			.then((s) => ((settings = s), (settingsError = null)))
			.catch((e: Error) => (settingsError = e.message));
	});
	let lookup = $state('');
	let effective = $state<{
		address: string;
		values: Record<string, unknown>;
		from?: Record<string, string>;
		applying?: string[];
	} | null>(null);
	let lookupError = $state<string | null>(null);
	async function look() {
		const address = lookup.trim();
		lookupError = null;
		try {
			if (local) {
				const s = await admin.addressSettings(address);
				effective = {
					address,
					values: s.effective ?? {},
					from: s.from,
					applying: s.applying
				};
			} else {
				effective = { address, values: await config.addressSettings(address) };
			}
		} catch (err) {
			lookupError = (err as Error).message;
		}
	}
	let editing = $state<{ match: string; settings: Record<string, unknown> } | null>(null);
	let editorOpen = $state(false);
	function edit(match = '', s: Record<string, unknown> = {}) {
		editing = { match, settings: s };
		editorOpen = true;
	}
	function saved() {
		settingsTick++;
		if (effective) look();
	}

	// Diverts and acceptors, from the resources.
	let listed = $state<{ diverts: string[]; acceptors: string[] } | null>(null);
	let listError = $state<string | null>(null);
	let listTick = $state(0);
	let attrs = $state<Record<string, Record<string, unknown>>>({});
	const DIVERT = [
		'UniqueName',
		'Address',
		'ForwardingAddress',
		'RoutingName',
		'Exclusive',
		'Filter',
		'RoutingType',
		'TransformerClassName'
	];
	const ACCEPTOR = ['Name', 'FactoryClassName', 'Started', 'Parameters'];
	let acceptorMay = $state<Record<string, Allowed>>({});
	$effect(() => {
		void listTick;
		resources
			.list()
			.then(async (all) => {
				const of = (t: string) => all.filter((r) => r.type === t).map((r) => r.resource);
				listed = { diverts: of('divert'), acceptors: of('acceptor') };
				listError = null;
				for (const r of listed.diverts)
					resources.attributes(r, DIVERT).then((a) => (attrs[r] = values(a)));
				for (const r of listed.acceptors) {
					resources.attributes(r, ACCEPTOR).then((a) => (attrs[r] = values(a)));
					allowed(r, ['start', 'stop', 'reload']).then((a) => (acceptorMay[r] = a));
				}
			})
			.catch((e: Error) => (listError = e.message));
	});
	function values(a: Record<string, { value?: unknown }>): Record<string, unknown> {
		return Object.fromEntries(Object.entries(a).map(([k, v]) => [k, v.value]));
	}

	let divert = $state<Divert | undefined>(undefined);
	let divertOpen = $state(false);
	function editDivert(resource?: string) {
		const a = resource ? attrs[resource] : undefined;
		divert = a
			? {
					name: String(a.UniqueName ?? ''),
					address: String(a.Address ?? ''),
					forwardingAddress: String(a.ForwardingAddress ?? ''),
					routingName: String(a.RoutingName ?? ''),
					exclusive: a.Exclusive === true,
					filter: a.Filter ? String(a.Filter) : '',
					routingType: (String(a.RoutingType ?? 'STRIP') as Divert['routingType']) || 'STRIP',
					transformerClassName: a.TransformerClassName ? String(a.TransformerClassName) : ''
				}
			: undefined;
		divertOpen = true;
	}

	async function acceptor(resource: string, action: 'start' | 'stop' | 'reload') {
		try {
			await resources.invoke(resource, `${action}()`);
			toast.success(
				`${action === 'reload' ? 'Reloaded' : action === 'start' ? 'Started' : 'Stopped'} ${resource}`
			);
			listTick++;
		} catch (e) {
			toast.error(`${resource}: ${action} failed`, { description: (e as Error).message });
		}
	}

	// Export and reload.
	let exported = $state<{ file?: string; text?: string | null } | null>(null);
	let exporting = $state(false);
	async function exportProperties() {
		exporting = true;
		try {
			exported = await admin.exportConfiguration();
			toast.success('Exported the configuration as broker properties');
		} catch (e) {
			toast.error('The configuration could not be exported', {
				description: (e as Error).message
			});
		} finally {
			exporting = false;
		}
	}
	function download() {
		if (!exported?.text) return;
		saveText('broker_config_as_properties.txt', exported.text, 'text/plain');
	}

	// What needs confirming.
	let confirm = $state<
		| { kind: 'remove-settings'; match: string }
		| { kind: 'destroy-divert'; name: string }
		| { kind: 'reload' }
		| null
	>(null);
	async function confirmed() {
		const c = confirm;
		confirm = null;
		if (!c) return;
		try {
			if (c.kind === 'remove-settings') {
				await config.removeAddressSettings(c.match);
				toast.success(`Removed the settings of ${c.match}`);
				saved();
			} else if (c.kind === 'destroy-divert') {
				await config.destroyDivert(c.name);
				toast.success(`Destroyed divert ${c.name}`);
				listTick++;
			} else {
				await config.reload();
				toast.success('Reloaded the configuration file');
				settingsTick++;
				listTick++;
			}
		} catch (e) {
			toast.error('It could not be done', { description: (e as Error).message });
		}
	}

	function setting(v: unknown): string {
		return display(v) || '–';
	}
</script>

<svelte:head>
	<title>Configuration · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	<h1 class="text-xl font-semibold">Configuration</h1>

	<Tabs.Root value={tab} onValueChange={show}>
		<Tabs.List>
			<Tabs.Trigger value="address-settings">Address settings</Tabs.Trigger>
			<Tabs.Trigger value="diverts">Diverts</Tabs.Trigger>
			<Tabs.Trigger value="acceptors">Acceptors</Tabs.Trigger>
			<Tabs.Trigger value="export">Export and reload</Tabs.Trigger>
		</Tabs.List>

		<Tabs.Content value="address-settings" class="flex flex-col gap-6 pt-2">
			<section class="flex flex-col gap-2" aria-labelledby="effective">
				<h2 id="effective" class="font-semibold">An address's settings</h2>
				<form
					class="flex max-w-xl gap-2"
					onsubmit={(e) => {
						e.preventDefault();
						look();
					}}
				>
					<Input
						aria-label="Address"
						placeholder="An address, such as orders"
						autocomplete="off"
						bind:value={lookup}
					/>
					<Button type="submit" variant="outline" disabled={!lookup.trim()}>Show</Button>
				</form>
				{#if lookupError}<p class="text-sm text-destructive" role="alert">{lookupError}</p>{/if}
				{#if effective}
					{#if effective.applying}
						<p class="text-sm text-muted-foreground">
							{#if effective.applying.length}
								Matches of {effective.address}, most specific first: {effective.applying.join(
									', '
								)}.
							{:else}
								No match applies to {effective.address}: every setting is the default.
							{/if}
						</p>
					{/if}
					<div class="overflow-x-auto rounded-md border">
						<Table.Root>
							<Table.Header>
								<Table.Row>
									<Table.Head>Setting</Table.Head>
									<Table.Head>Value</Table.Head>
									{#if effective.from}<Table.Head>From</Table.Head>{/if}
								</Table.Row>
							</Table.Header>
							<Table.Body>
								{#each Object.entries(effective.values) as [name, value] (name)}
									<Table.Row>
										<Table.Cell class="font-mono text-xs">{name}</Table.Cell>
										<Table.Cell>{setting(value)}</Table.Cell>
										{#if effective.from}
											<Table.Cell class="font-mono text-xs"
												>{effective.from[name] ?? '–'}</Table.Cell
											>
										{/if}
									</Table.Row>
								{:else}
									<Table.Row>
										<Table.Cell colspan={3} class="text-muted-foreground">
											Nothing is set: every setting is the default.
										</Table.Cell>
									</Table.Row>
								{/each}
							</Table.Body>
						</Table.Root>
					</div>
				{/if}
			</section>

			<section class="flex flex-col gap-2" aria-labelledby="matches">
				<div class="flex items-center justify-between gap-2">
					<h2 id="matches" class="font-semibold">Matches</h2>
					{#if local}
						<Button
							size="sm"
							disabled={!settings || may.addAddressSettings === false}
							onclick={() => edit()}>Add address settings</Button
						>
					{/if}
				</div>
				{#if !local}
					<p class="text-sm text-muted-foreground">
						The matches are listed for the broker you are logged in to. Another node's settings for
						an address are shown above.
					</p>
				{:else if settingsError}
					<p class="text-sm text-destructive">{settingsError}</p>
				{:else if !settings}
					<Skeleton class="h-24 w-full" />
				{:else}
					<div class="overflow-x-auto rounded-md border">
						<Table.Root>
							<Table.Header>
								<Table.Row>
									<Table.Head>Match</Table.Head>
									<Table.Head>Settings</Table.Head>
									<Table.Head><span class="sr-only">Actions</span></Table.Head>
								</Table.Row>
							</Table.Header>
							<Table.Body>
								{#each settings.matches as m (m.match)}
									<Table.Row>
										<Table.Cell class="align-top font-mono">
											{m.match}
											{#if m.added}<Badge variant="secondary" class="ml-1">management</Badge>{/if}
										</Table.Cell>
										<Table.Cell class="max-w-xl">
											<div class="flex flex-wrap gap-1">
												{#each Object.entries(m.settings) as [k, v] (k)}
													<span class="rounded bg-muted px-1.5 py-0.5 font-mono text-xs"
														>{k}={setting(v)}</span
													>
												{:else}
													<span class="text-muted-foreground">None</span>
												{/each}
											</div>
										</Table.Cell>
										<Table.Cell class="text-right align-top whitespace-nowrap">
											<Button
												size="xs"
												variant="outline"
												disabled={may.addAddressSettings === false}
												onclick={() => edit(m.match, m.settings)}>Edit</Button
											>
											<Button
												size="xs"
												variant="outline"
												disabled={may.removeAddressSettings === false}
												onclick={() => (confirm = { kind: 'remove-settings', match: m.match })}
												>Remove</Button
											>
										</Table.Cell>
									</Table.Row>
								{:else}
									<Table.Row>
										<Table.Cell colspan={3} class="text-muted-foreground"
											>No address settings.</Table.Cell
										>
									</Table.Row>
								{/each}
							</Table.Body>
						</Table.Root>
					</div>
				{/if}
			</section>
		</Tabs.Content>

		<Tabs.Content value="diverts" class="flex flex-col gap-2 pt-2">
			<div class="flex justify-end">
				<Button size="sm" disabled={may.createDivert === false} onclick={() => editDivert()}
					>Create a divert</Button
				>
			</div>
			{#if listError}
				<p class="text-sm text-destructive">{listError}</p>
			{:else if !listed}
				<Skeleton class="h-24 w-full" />
			{:else if !listed.diverts.length}
				<p class="text-sm text-muted-foreground">No divert.</p>
			{:else}
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>Name</Table.Head>
								<Table.Head>Address</Table.Head>
								<Table.Head>Forwarding address</Table.Head>
								<Table.Head>Exclusive</Table.Head>
								<Table.Head>Filter</Table.Head>
								<Table.Head>Routing type</Table.Head>
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each listed.diverts as r (r)}
								{@const a = attrs[r]}
								<Table.Row>
									<Table.Cell>
										<a class="hover:underline" href={explorerHref(params.node, r)}
											>{a ? setting(a.UniqueName) : r}</a
										>
									</Table.Cell>
									<Table.Cell>{a ? setting(a.Address) : ''}</Table.Cell>
									<Table.Cell>{a ? setting(a.ForwardingAddress) : ''}</Table.Cell>
									<Table.Cell>{a ? (a.Exclusive ? 'Yes' : 'No') : ''}</Table.Cell>
									<Table.Cell class="font-mono text-xs">{a ? setting(a.Filter) : ''}</Table.Cell>
									<Table.Cell>{a ? setting(a.RoutingType) : ''}</Table.Cell>
									<Table.Cell class="text-right whitespace-nowrap">
										<Button
											size="xs"
											variant="outline"
											disabled={!a || may.updateDivert === false}
											onclick={() => editDivert(r)}>Edit</Button
										>
										<Button
											size="xs"
											variant="outline"
											disabled={!a || may.destroyDivert === false}
											onclick={() =>
												(confirm = { kind: 'destroy-divert', name: String(a?.UniqueName ?? '') })}
											>Destroy</Button
										>
									</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>

		<Tabs.Content value="acceptors" class="flex flex-col gap-2 pt-2">
			{#if listError}
				<p class="text-sm text-destructive">{listError}</p>
			{:else if !listed}
				<Skeleton class="h-24 w-full" />
			{:else if !listed.acceptors.length}
				<p class="text-sm text-muted-foreground">
					No acceptor{local ? '' : ' that this node lists over management messages'}.
				</p>
			{:else}
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>Name</Table.Head>
								<Table.Head>Parameters</Table.Head>
								<Table.Head>State</Table.Head>
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each listed.acceptors as r (r)}
								{@const a = attrs[r]}
								<Table.Row>
									<Table.Cell>
										<a class="hover:underline" href={explorerHref(params.node, r)}
											>{a ? setting(a.Name) : r}</a
										>
									</Table.Cell>
									<Table.Cell class="max-w-md">
										<div class="flex flex-wrap gap-1">
											{#each Object.entries((a?.Parameters ?? {}) as Record<string, unknown>) as [k, v] (k)}
												<span class="rounded bg-muted px-1.5 py-0.5 font-mono text-xs"
													>{k}={setting(v)}</span
												>
											{/each}
										</div>
									</Table.Cell>
									<Table.Cell>
										{#if a?.Started === true}<Badge>Started</Badge>{:else if a}<Badge
												variant="secondary">Stopped</Badge
											>{/if}
									</Table.Cell>
									<Table.Cell class="text-right whitespace-nowrap">
										{#if a?.Started === true}
											<Button
												size="xs"
												variant="outline"
												disabled={acceptorMay[r]?.stop === false}
												onclick={() => acceptor(r, 'stop')}>Stop</Button
											>
										{:else if a}
											<Button
												size="xs"
												variant="outline"
												disabled={acceptorMay[r]?.start === false}
												onclick={() => acceptor(r, 'start')}>Start</Button
											>
										{/if}
										<Button
											size="xs"
											variant="outline"
											disabled={!a || acceptorMay[r]?.reload === false}
											onclick={() => acceptor(r, 'reload')}>Reload</Button
										>
									</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>

		<Tabs.Content value="export" class="flex flex-col gap-6 pt-2">
			<section class="flex flex-col gap-2" aria-labelledby="export-heading">
				<h2 id="export-heading" class="font-semibold">Export as broker properties</h2>
				<p class="max-w-2xl text-sm text-muted-foreground">
					The broker writes its configuration file as broker properties (<code
						>exportConfigAsProperties</code
					>) to <code>broker_config_as_properties.txt</code> in its temporary directory. Passwords are
					redacted.
				</p>
				<div class="flex gap-2">
					<Button
						disabled={exporting || may.exportConfigAsProperties === false}
						onclick={exportProperties}>Export</Button
					>
					{#if exported?.text}
						<Button variant="outline" onclick={download}>Download</Button>
					{/if}
				</div>
				{#if exported}
					{#if exported.text != null}
						<p class="text-sm text-muted-foreground">Written to <code>{exported.file}</code>.</p>
						<pre
							class="max-h-[60vh] overflow-auto rounded-md border bg-muted/40 p-3 font-mono text-xs"
							aria-label="Exported configuration">{exported.text}</pre>
					{:else}
						<p class="text-sm text-muted-foreground">
							Exported on that node, to its own temporary directory, which only it can read.
						</p>
					{/if}
				{/if}
			</section>
			<section class="flex flex-col gap-2" aria-labelledby="reload-heading">
				<h2 id="reload-heading" class="font-semibold">Reload</h2>
				<p class="max-w-2xl text-sm text-muted-foreground">
					The broker reads its configuration file again and applies what can change as it runs:
					addresses and queues, address and security settings, diverts, bridges, and acceptors when
					configured to (<code>reloadConfigurationFile</code>).
				</p>
				<div>
					<Button
						variant="outline"
						disabled={may.reloadConfigurationFile === false}
						onclick={() => (confirm = { kind: 'reload' })}>Reload the configuration</Button
					>
				</div>
			</section>
		</Tabs.Content>
	</Tabs.Root>
</div>

{#if editing && settings}
	<AddressSettingsEditor
		bind:open={editorOpen}
		match={editing.match}
		settings={editing.settings}
		names={settings.names}
		onsaved={saved}
	/>
{/if}
<DivertEditor bind:open={divertOpen} {divert} onsaved={() => listTick++} />

<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
	<AlertDialog.Content>
		{#if confirm}
			<AlertDialog.Header>
				<AlertDialog.Title>
					{#if confirm.kind === 'remove-settings'}Remove the settings of {confirm.match}?
					{:else if confirm.kind === 'destroy-divert'}Destroy divert {confirm.name}?
					{:else}Reload the configuration file?{/if}
				</AlertDialog.Title>
				<AlertDialog.Description>
					{#if confirm.kind === 'remove-settings'}
						Its addresses take their settings from the other matches.
					{:else if confirm.kind === 'destroy-divert'}
						Messages reaching its address are no longer diverted.
					{:else}
						What the file says now replaces what can change as the broker runs.
					{/if}
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action onclick={confirmed}>
					{confirm.kind === 'reload'
						? 'Reload'
						: confirm.kind === 'destroy-divert'
							? 'Destroy'
							: 'Remove'}
				</AlertDialog.Action>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
