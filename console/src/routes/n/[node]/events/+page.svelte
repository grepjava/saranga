<!--
	The management notifications (activemq.notifications) as they come: the
	broker's last ones first, then each one the live stream brings.
-->
<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import PauseIcon from '@lucide/svelte/icons/pause';
	import PlayIcon from '@lucide/svelte/icons/play';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import type { Notification } from '#lib/api/index.js';
	import { addressHref, eventsHref, hashQuery, queueHref } from '#lib/nav.js';
	import { live } from '#lib/state/live.svelte.js';
	import { nodes, LOCAL } from '#lib/state/nodes.svelte.js';
	import { describe, typeTone } from '#lib/events.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();

	/** The most rows shown. */
	const SHOWN = 500;

	const type = $derived(hashQuery(page.url).get('type') ?? '');
	let text = $state('');
	let paused = $state<Notification[] | null>(null);
	let clearedAfter = $state(0);
	let open = $state<number | null>(null);

	// Looking at the feed sees what came.
	$effect(() => {
		void live.notes;
		live.seen();
	});

	const notes = $derived((paused ?? live.notes).filter((n) => n.seq > clearedAfter));
	const types = $derived([...new Set(notes.map((n) => n.type))].sort());
	const waiting = $derived(
		paused ? live.notes.filter((n) => n.seq > paused!.at(-1)!.seq).length : 0
	);

	const matching = $derived.by(() => {
		const t = text.trim().toLowerCase();
		const out: Notification[] = [];
		for (let i = notes.length - 1; i >= 0 && out.length < SHOWN; i--) {
			const n = notes[i];
			if (type && n.type !== type) continue;
			if (t && !JSON.stringify(n).toLowerCase().includes(t)) continue;
			out.push(n);
		}
		return out;
	});

	function pickType(t: string) {
		goto(eventsHref(params.node, t ? new URLSearchParams({ type: t }) : undefined), {
			replaceState: true
		});
	}

	const time = new Intl.DateTimeFormat(undefined, {
		hour: '2-digit',
		minute: '2-digit',
		second: '2-digit',
		fractionalSecondDigits: 3
	});

	function link(name: string, value: unknown): string | undefined {
		if (typeof value !== 'string' || value === '') return undefined;
		if (name === '_AMQ_Address') return addressHref(params.node, value);
		if (name === '_AMQ_RoutingName') return queueHref(params.node, value);
		return undefined;
	}
</script>

<svelte:head>
	<title>Events · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	{#if params.node !== LOCAL}
		<p class="text-sm text-muted-foreground">
			These are the notifications of {nodes.find(LOCAL)?.name ?? 'this broker'}, the broker you
			logged in to: each broker sends its own.
		</p>
	{/if}
	<div class="flex flex-wrap items-center justify-between gap-3">
		<h1 class="text-xl font-semibold">Events</h1>
		<div class="flex items-center gap-2">
			{#if paused}
				<Button size="sm" variant="outline" onclick={() => (paused = null)}>
					<PlayIcon />
					Resume{waiting ? ` (${waiting} new)` : ''}
				</Button>
			{:else}
				<Button
					size="sm"
					variant="outline"
					disabled={!live.notifying}
					onclick={() => (paused = [...live.notes])}
				>
					<PauseIcon />
					Pause
				</Button>
			{/if}
			<Button
				size="sm"
				variant="outline"
				disabled={notes.length === 0}
				onclick={() => (clearedAfter = live.notes.at(-1)?.seq ?? 0)}
			>
				Clear
			</Button>
		</div>
	</div>

	{#if !live.notifying}
		<Alert.Root>
			<Alert.Title>Your role may not see the notifications</Alert.Title>
			<Alert.Description>
				The feed is what a client consuming from the broker's notification address
				(activemq.notifications) receives; your role needs the consume permission on it.
			</Alert.Description>
		</Alert.Root>
	{:else}
		<div class="flex flex-wrap items-center gap-2">
			<select
				class="h-8 rounded-md border bg-background px-2 text-sm"
				aria-label="Type"
				value={type}
				onchange={(e) => pickType(e.currentTarget.value)}
			>
				<option value="">Every type</option>
				{#each type && !types.includes(type) ? [type, ...types] : types as t (t)}
					<option value={t}>{t}</option>
				{/each}
			</select>
			<Input class="h-8 max-w-64" placeholder="Search…" aria-label="Search" bind:value={text} />
			<span class="text-sm text-muted-foreground tabular-nums">
				{matching.length}{matching.length === SHOWN ? '+' : ''} of {notes.length}
			</span>
		</div>

		<div class="rounded-md border">
			<Table.Root aria-label="Events">
				<Table.Header>
					<Table.Row>
						<Table.Head class="w-8"></Table.Head>
						<Table.Head class="w-32">Time</Table.Head>
						<Table.Head class="w-56">Type</Table.Head>
						<Table.Head>Details</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each matching as n (n.seq)}
						<Table.Row
							class="cursor-pointer"
							onclick={() => (open = open === n.seq ? null : n.seq)}
						>
							<Table.Cell>
								<!-- The row opens on a click; the button is its keyboard's way. -->
								<button
									type="button"
									class="flex rounded-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
									aria-expanded={open === n.seq}
									aria-label="Details"
									onclick={(e) => {
										e.stopPropagation();
										open = open === n.seq ? null : n.seq;
									}}
								>
									<ChevronRightIcon
										class="size-4 text-muted-foreground transition-transform {open === n.seq
											? 'rotate-90'
											: ''}"
									/>
								</button>
							</Table.Cell>
							<Table.Cell class="font-mono text-xs tabular-nums">
								{time.format(n.timestamp)}
							</Table.Cell>
							<Table.Cell>
								<Badge variant={typeTone(n.type)}>{n.type}</Badge>
							</Table.Cell>
							<Table.Cell class="max-w-0 truncate text-sm">{describe(n, live.address)}</Table.Cell>
						</Table.Row>
						{#if open === n.seq}
							<Table.Row class="bg-muted/40 hover:bg-muted/40">
								<Table.Cell></Table.Cell>
								<Table.Cell colspan={3}>
									<dl class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 py-1 text-xs">
										{#each Object.entries(n.properties) as [k, v] (k)}
											<dt class="font-mono text-muted-foreground">{k}</dt>
											<dd class="font-mono break-all">
												{#if link(k, v)}
													<a class="underline" href={link(k, v)}>{v}</a>
												{:else}
													{v === null ? 'null' : String(v)}
												{/if}
											</dd>
										{/each}
									</dl>
								</Table.Cell>
							</Table.Row>
						{/if}
					{:else}
						<Table.Row>
							<Table.Cell colspan={4} class="py-8 text-center text-sm text-muted-foreground">
								{notes.length
									? 'Nothing matches.'
									: 'No notifications yet: they come as clients connect, bind and consume.'}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
		<p class="text-xs text-muted-foreground">
			The broker keeps its last notifications; the page shows the {SHOWN} newest that match.
		</p>
	{/if}
</div>
