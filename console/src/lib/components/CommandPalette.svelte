<script lang="ts">
	import * as Command from '#lib/components/ui/command/index.js';
	import InboxIcon from '@lucide/svelte/icons/inbox';
	import WaypointsIcon from '@lucide/svelte/icons/waypoints';
	import SunIcon from '@lucide/svelte/icons/sun';
	import MoonIcon from '@lucide/svelte/icons/moon';
	import MonitorIcon from '@lucide/svelte/icons/monitor';
	import LogOutIcon from '@lucide/svelte/icons/log-out';
	import { goto } from '$app/navigation';
	import { setMode } from 'mode-watcher';
	import { api } from '#lib/api/index.js';
	import { NAVIGATION } from '#lib/navigation.js';
	import { listingHref } from '#lib/nav.js';
	import { session } from '#lib/state/session.svelte.js';

	let { node, open = $bindable(false) }: { node: string; open?: boolean } = $props();

	let search = $state('');
	let found = $state<{ kind: 'queues' | 'addresses'; name: string }[]>([]);

	function onkeydown(e: KeyboardEvent) {
		if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
			e.preventDefault();
			open = !open;
		}
	}

	// Queues and addresses whose names hold what is typed.
	$effect(() => {
		const text = search.trim();
		if (!open || text.length < 2) {
			found = [];
			return;
		}
		let stale = false;
		const timer = setTimeout(async () => {
			const query = {
				filters: [{ field: 'name', operation: 'CONTAINS' as const, value: text }],
				order: 'asc' as const,
				sort: 'name',
				page: 1,
				size: 8
			};
			const [queues, addresses] = await Promise.all([
				session.may('listQueues') ? api.list('queues', query).catch(() => null) : null,
				session.may('listAddresses') ? api.list('addresses', query).catch(() => null) : null
			]);
			if (stale) return;
			found = [
				...(queues?.data ?? []).map((r) => ({ kind: 'queues' as const, name: r.name })),
				...(addresses?.data ?? []).map((r) => ({ kind: 'addresses' as const, name: r.name }))
			];
		}, 200);
		return () => {
			stale = true;
			clearTimeout(timer);
		};
	});

	function run(f: () => unknown) {
		open = false;
		search = '';
		f();
	}

	function show(kind: 'queues' | 'addresses', name: string) {
		const params = new URLSearchParams({ filter: `name:EQUALS:${name}` });
		run(() => goto(listingHref(node, kind, params)));
	}
</script>

<svelte:document {onkeydown} />

<Command.Dialog bind:open title="Command palette" description="Go to a page or run a command">
	<Command.Input placeholder="Go to a page, a queue or an address…" bind:value={search} />
	<Command.List>
		<Command.Empty>Nothing matches.</Command.Empty>
		{#if found.length}
			<Command.Group heading="Found">
				{#each found as f (f.kind + f.name)}
					<Command.Item
						value={`${f.kind} ${f.name}`}
						keywords={[search]}
						onSelect={() => show(f.kind, f.name)}
					>
						{#if f.kind === 'queues'}<InboxIcon />{:else}<WaypointsIcon />{/if}
						<span class="truncate">{f.name}</span>
						<span class="ml-auto text-xs text-muted-foreground">
							{f.kind === 'queues' ? 'Queue' : 'Address'}
						</span>
					</Command.Item>
				{/each}
			</Command.Group>
		{/if}
		{#each NAVIGATION as group (group.label)}
			{@const items = group.items.filter((i) => !i.needs || session.may(i.needs))}
			{#if items.length}
				<Command.Group heading={group.label}>
					{#each items as item (item.title)}
						<Command.Item
							value={item.title}
							keywords={item.keywords}
							onSelect={() => run(() => goto(item.href(node)))}
						>
							<item.icon />
							{item.title}
						</Command.Item>
					{/each}
				</Command.Group>
			{/if}
		{/each}
		<Command.Separator />
		<Command.Group heading="Commands">
			<Command.Item value="Light theme" onSelect={() => run(() => setMode('light'))}>
				<SunIcon /> Light theme
			</Command.Item>
			<Command.Item value="Dark theme" onSelect={() => run(() => setMode('dark'))}>
				<MoonIcon /> Dark theme
			</Command.Item>
			<Command.Item value="System theme" onSelect={() => run(() => setMode('system'))}>
				<MonitorIcon /> System theme
			</Command.Item>
			<Command.Item value="Log out" onSelect={() => run(() => session.logout())}>
				<LogOutIcon /> Log out
			</Command.Item>
		</Command.Group>
	</Command.List>
</Command.Dialog>
