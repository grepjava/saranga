<script lang="ts">
	import * as Card from '#lib/components/ui/card/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Switch } from '#lib/components/ui/switch/index.js';
	import { setMode, userPrefersMode } from 'mode-watcher';
	import { PAGE_SIZES } from '#lib/query.js';
	import { LISTING_KINDS } from '#lib/listings.js';
	import { BYTES_FORMATS, type BytesFormat } from '#lib/body.js';
	import { prefs } from '#lib/state/prefs.svelte.js';

	const select =
		'h-8 w-40 rounded-lg border border-input bg-background px-2 text-sm dark:bg-input/30';

	/** Whether a pattern is a regular expression. */
	function valid(p: string): boolean {
		try {
			new RegExp(p);
			return true;
		} catch {
			return false;
		}
	}
</script>

<svelte:head>
	<title>Preferences · Saranga console</title>
</svelte:head>

{#snippet row(id: string, label: string, hint?: string)}
	<div class="grid gap-1">
		<Label for={id}>{label}</Label>
		{#if hint}<span class="text-xs text-muted-foreground">{hint}</span>{/if}
	</div>
{/snippet}

<div class="flex max-w-2xl flex-col gap-4">
	<div class="flex items-baseline justify-between gap-3">
		<h1 class="text-xl font-semibold">Preferences</h1>
		<Button variant="ghost" size="sm" onclick={() => prefs.reset()}>Reset all</Button>
	</div>
	<p class="text-sm text-muted-foreground">These are kept in this browser.</p>
	<Card.Root>
		<Card.Header><Card.Title>Display</Card.Title></Card.Header>
		<Card.Content class="grid gap-5">
			<div class="flex items-center justify-between gap-4">
				{@render row('theme', 'Theme')}
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
				{@render row('page-size', 'Rows per page')}
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
	<Card.Root>
		<Card.Header><Card.Title>Messages</Card.Title></Card.Header>
		<Card.Content class="grid gap-5">
			<div class="flex items-center justify-between gap-4">
				{@render row(
					'dlq',
					'Dead-letter queues',
					'A regular expression; their messages can be retried.'
				)}
				<Input
					id="dlq"
					class="w-40 font-mono"
					aria-invalid={!valid(prefs.dlqPattern)}
					value={prefs.dlqPattern}
					onchange={(e) => (prefs.dlqPattern = e.currentTarget.value)}
				/>
			</div>
			<div class="flex items-center justify-between gap-4">
				{@render row(
					'expiry',
					'Expiry queues',
					'A regular expression; their messages can be retried.'
				)}
				<Input
					id="expiry"
					class="w-40 font-mono"
					aria-invalid={!valid(prefs.expiryPattern)}
					value={prefs.expiryPattern}
					onchange={(e) => (prefs.expiryPattern = e.currentTarget.value)}
				/>
			</div>
			<div class="flex items-center justify-between gap-4">
				{@render row('bytes', 'Bytes', 'How the body of a message that is not text reads.')}
				<select
					id="bytes"
					class={select}
					value={prefs.bytesFormat}
					onchange={(e) => (prefs.bytesFormat = e.currentTarget.value as BytesFormat)}
				>
					{#each Object.entries(BYTES_FORMATS) as [value, label] (value)}
						<option {value}>{label}</option>
					{/each}
				</select>
			</div>
		</Card.Content>
	</Card.Root>
	<Card.Root>
		<Card.Header><Card.Title>Diagram and explorer</Card.Title></Card.Header>
		<Card.Content class="grid gap-5">
			<div class="flex items-center justify-between gap-4">
				{@render row('diagram-addresses', 'Addresses in the diagram', 'The most it draws.')}
				<Input
					id="diagram-addresses"
					type="number"
					min="1"
					class="w-40"
					value={prefs.diagramAddresses}
					onchange={(e) => {
						const n = Number(e.currentTarget.value);
						if (Number.isInteger(n) && n > 0) prefs.diagramAddresses = n;
					}}
				/>
			</div>
			<div class="flex items-center justify-between gap-4">
				{@render row(
					'internal',
					'Internal addresses',
					'Show the broker’s own, such as activemq.notifications.'
				)}
				<Switch
					id="internal"
					checked={prefs.showInternal}
					onCheckedChange={(c) => (prefs.showInternal = c)}
				/>
			</div>
		</Card.Content>
	</Card.Root>
</div>
