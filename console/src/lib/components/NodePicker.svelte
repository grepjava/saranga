<script lang="ts">
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import * as Sidebar from '#lib/components/ui/sidebar/index.js';
	import ServerIcon from '@lucide/svelte/icons/server';
	import ServersIcon from '@lucide/svelte/icons/server-cog';
	import ChevronsUpDownIcon from '@lucide/svelte/icons/chevrons-up-down';
	import CheckIcon from '@lucide/svelte/icons/check';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { nodes, ALL } from '#lib/state/nodes.svelte.js';

	let { node }: { node: string } = $props();

	const shown = $derived(nodes.find(node));

	/** The same page on another node. */
	function pick(id: string) {
		const hash = page.url.hash.replace(/^#\/n\/[^/?]+/, `#/n/${encodeURIComponent(id)}`);
		goto(hash);
	}
</script>

<Sidebar.Menu>
	<Sidebar.MenuItem>
		<DropdownMenu.Root>
			<DropdownMenu.Trigger>
				{#snippet child({ props })}
					<Sidebar.MenuButton
						{...props}
						size="lg"
						class="data-[state=open]:bg-sidebar-accent"
						aria-label="Node"
					>
						<div
							class="flex aspect-square size-8 items-center justify-center rounded-md bg-sidebar-accent text-sidebar-accent-foreground"
						>
							<ServerIcon />
						</div>
						<div class="grid flex-1 text-left text-sm leading-tight">
							<span class="truncate font-medium">{shown?.name ?? node}</span>
							<span class="truncate text-xs text-muted-foreground">
								{#if node === ALL}
									{nodes.members.length} nodes
								{:else if shown?.error}
									Not reachable
								{:else}
									{shown?.backup ? 'Backup' : 'Live'}{shown?.version ? ` · ${shown.version}` : ''}
								{/if}
							</span>
						</div>
						<ChevronsUpDownIcon class="ml-auto" />
					</Sidebar.MenuButton>
				{/snippet}
			</DropdownMenu.Trigger>
			<DropdownMenu.Content class="min-w-56" align="start">
				<DropdownMenu.Label class="text-xs text-muted-foreground">Nodes</DropdownMenu.Label>
				{#each nodes.list as n (n.id)}
					{#if n.id === ALL}<DropdownMenu.Separator />{/if}
					<DropdownMenu.Item onSelect={() => pick(n.id)}>
						{#if n.id === ALL}<ServersIcon />{:else}<ServerIcon />{/if}
						<div class="grid">
							<span>{n.name}{n.local && nodes.members.length > 1 ? ' (this broker)' : ''}</span>
							{#if n.error}
								<span class="text-[0.7rem] text-destructive">Not reachable</span>
							{:else if n.primary || n.nodeID}
								<span class="font-mono text-[0.7rem] text-muted-foreground"
									>{n.primary ?? n.nodeID}</span
								>
							{/if}
						</div>
						{#if n.id === node}<CheckIcon class="ml-auto" />{/if}
					</DropdownMenu.Item>
				{/each}
			</DropdownMenu.Content>
		</DropdownMenu.Root>
	</Sidebar.MenuItem>
</Sidebar.Menu>
