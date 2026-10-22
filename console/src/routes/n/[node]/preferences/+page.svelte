<script lang="ts">
	import * as Card from '#lib/components/ui/card/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { setMode, userPrefersMode } from 'mode-watcher';
	import { PAGE_SIZES } from '#lib/query.js';
	import { LISTING_KINDS } from '#lib/listings.js';
	import { prefs } from '#lib/state/prefs.svelte.js';

	const select =
		'h-8 w-40 rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30';
</script>

<svelte:head>
	<title>Preferences · Saranga console</title>
</svelte:head>

<div class="flex max-w-2xl flex-col gap-4">
	<h1 class="text-xl font-semibold">Preferences</h1>
	<p class="text-sm text-muted-foreground">These are kept in this browser.</p>
	<Card.Root>
		<Card.Content class="grid gap-5">
			<div class="flex items-center justify-between gap-4">
				<Label for="theme">Theme</Label>
				<select
					id="theme"
					class={select}
					value={userPrefersMode.current}
					onchange={(e) => setMode(e.currentTarget.value as 'light' | 'dark' | 'system')}
				>
					<option value="system">System</option>
					<option value="light">Light</option>
					<option value="dark">Dark</option>
				</select>
			</div>
			<div class="flex items-center justify-between gap-4">
				<Label for="page-size">Rows per page</Label>
				<select
					id="page-size"
					class={select}
					value={prefs.pageSize}
					onchange={(e) => (prefs.pageSize = Number(e.currentTarget.value))}
				>
					{#each PAGE_SIZES as n (n)}
						<option value={n}>{n}</option>
					{/each}
				</select>
			</div>
			<div class="flex items-center justify-between gap-4">
				<div class="grid gap-1">
					<span class="text-sm font-medium">Columns</span>
					<span class="text-xs text-muted-foreground">Show each table's default columns again.</span
					>
				</div>
				<Button
					variant="outline"
					size="sm"
					onclick={() => LISTING_KINDS.forEach((k) => prefs.setColumns(k, undefined))}
				>
					Reset columns
				</Button>
			</div>
		</Card.Content>
	</Card.Root>
</div>
