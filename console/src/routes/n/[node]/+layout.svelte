<script lang="ts">
	import * as Sidebar from '#lib/components/ui/sidebar/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Separator } from '#lib/components/ui/separator/index.js';
	import SearchIcon from '@lucide/svelte/icons/search';
	import AppSidebar from '#lib/components/AppSidebar.svelte';
	import CommandPalette from '#lib/components/CommandPalette.svelte';
	import ThemeMenu from '#lib/components/ThemeMenu.svelte';
	import UserMenu from '#lib/components/UserMenu.svelte';
	import { nodes } from '#lib/state/nodes.svelte.js';
	import type { LayoutProps } from './$types';

	let { children, params }: LayoutProps = $props();

	let palette = $state(false);

	$effect(() => {
		nodes.load().catch(() => {
			// The pages say what they cannot read; the picker keeps its default.
		});
	});

	const known = $derived(nodes.find(params.node) !== undefined);
</script>

<Sidebar.Provider>
	<AppSidebar node={params.node} />
	<Sidebar.Inset class="min-w-0">
		<header
			class="sticky top-0 z-10 flex h-12 shrink-0 items-center gap-2 border-b bg-background/95 px-3 backdrop-blur"
		>
			<Sidebar.Trigger class="-ml-1" />
			<Separator orientation="vertical" class="mr-1 data-[orientation=vertical]:h-4" />
			<Button
				variant="outline"
				size="sm"
				class="w-full max-w-64 justify-start text-muted-foreground"
				onclick={() => (palette = true)}
			>
				<SearchIcon />
				<span class="truncate">Search…</span>
				<kbd
					class="pointer-events-none ml-auto hidden rounded border bg-muted px-1.5 font-mono text-[0.65rem] sm:inline"
					>Ctrl K</kbd
				>
			</Button>
			<div class="ml-auto flex items-center gap-1">
				<ThemeMenu />
				<UserMenu />
			</div>
		</header>
		<main class="min-w-0 flex-1 p-4 md:p-6">
			{#if known}
				{@render children()}
			{:else}
				<h1 class="text-lg font-semibold">No node {params.node}</h1>
				<p class="mt-1 text-sm text-muted-foreground">Pick a node from the list on the left.</p>
			{/if}
		</main>
	</Sidebar.Inset>
	<CommandPalette node={params.node} bind:open={palette} />
</Sidebar.Provider>
