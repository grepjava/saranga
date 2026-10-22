<!-- One message: its headers, its properties and its body. -->
<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import CodeEditor from '#lib/components/CodeEditor.svelte';
	import { MESSAGE_TYPES, properties, type Message } from '#lib/broker.js';
	import { BYTES_FORMATS, bodyText, pretty, type BytesFormat } from '#lib/body.js';
	import { formatBytes } from '#lib/format.js';
	import { prefs } from '#lib/state/prefs.svelte.js';

	let { message }: { message: Message } = $props();

	let format = $state<BytesFormat>(prefs.bytesFormat);
	let indented = $state(true);

	const body = $derived(bodyText(message, format));
	const formatted = $derived(body.preview ? undefined : pretty(body.text));
	const shown = $derived(indented && formatted ? formatted : body.text);

	const time = (t: number) => (t > 0 ? new Date(t).toLocaleString() : 'never');

	const headers = $derived<[string, string][]>([
		['Message ID', message.messageID],
		['User ID', message.userID ?? ''],
		['Address', message.address],
		['Type', MESSAGE_TYPES[message.type] ?? String(message.type)],
		['Protocol', message.protocol],
		['Durable', String(message.durable)],
		['Priority', String(message.priority)],
		['Timestamp', time(message.timestamp)],
		['Expires', time(message.expiration)],
		['Redelivered', String(message.redelivered)],
		['Large', String(message.largeMessage)],
		['Size', formatBytes(message.persistentSize)]
	]);
	const fields = $derived(properties(message));
</script>

<div class="grid gap-6">
	<section class="grid gap-2">
		<h3 class="text-sm font-medium">Headers</h3>
		<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
			{#each headers as [k, v] (k)}
				<dt class="text-muted-foreground">{k}</dt>
				<dd class="font-mono break-all">{v}</dd>
			{/each}
		</dl>
	</section>
	<section class="grid gap-2">
		<h3 class="text-sm font-medium">Properties</h3>
		{#if fields.length}
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Name</Table.Head>
						<Table.Head>Type</Table.Head>
						<Table.Head>Value</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each fields as p (p.name)}
						<Table.Row>
							<Table.Cell class="font-mono">{p.name}</Table.Cell>
							<Table.Cell class="text-muted-foreground">{p.type}</Table.Cell>
							<Table.Cell class="font-mono break-all whitespace-normal"
								>{String(p.value)}</Table.Cell
							>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		{:else}
			<p class="text-sm text-muted-foreground">None.</p>
		{/if}
	</section>
	<section class="grid gap-2">
		<div class="flex flex-wrap items-center justify-between gap-2">
			<h3 class="text-sm font-medium">
				Body{#if body.preview}<span class="font-normal text-muted-foreground">
						(its first bytes)</span
					>{/if}
			</h3>
			{#if body.preview}
				<select
					aria-label="Bytes"
					class="h-7 rounded-md border border-input bg-background px-2 text-xs dark:bg-input/30"
					bind:value={format}
				>
					{#each Object.entries(BYTES_FORMATS) as [value, label] (value)}
						<option {value}>{label}</option>
					{/each}
				</select>
			{:else if formatted}
				<Button variant="ghost" size="xs" onclick={() => (indented = !indented)}>
					{indented ? 'As sent' : 'Indented'}
				</Button>
			{/if}
		</div>
		<CodeEditor value={shown} readonly label="Body" class="max-h-96" />
	</section>
</div>
