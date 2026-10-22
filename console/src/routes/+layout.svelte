<script lang="ts">
	import './layout.css';
	import favicon from '#lib/assets/favicon.svg';
	import { ModeWatcher } from 'mode-watcher';
	import { Toaster } from '#lib/components/ui/sonner/index.js';
	import * as Tooltip from '#lib/components/ui/tooltip/index.js';
	import Login from '#lib/components/Login.svelte';
	import { session } from '#lib/state/session.svelte.js';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	let failure = $state<string | null>(null);

	$effect(() => {
		session.check().catch((e: Error) => (failure = e.message));
	});
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Saranga console</title>
</svelte:head>

<ModeWatcher defaultMode="system" />
<Toaster richColors closeButton />

<Tooltip.Provider delayDuration={300}>
	{#if failure}
		<main class="grid min-h-svh place-items-center p-6">
			<div class="max-w-md text-center">
				<h1 class="text-lg font-semibold">The console cannot reach the broker</h1>
				<p class="mt-2 text-sm text-muted-foreground">{failure}</p>
			</div>
		</main>
	{:else if session.state === 'out'}
		<Login />
	{:else if session.state === 'in'}
		{@render children()}
	{/if}
</Tooltip.Provider>
