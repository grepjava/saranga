<script lang="ts">
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import * as Card from '#lib/components/ui/card/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { resources, type AttributeValue, type View } from '#lib/api/index.js';
	import { live } from '#lib/state/live.svelte.js';
	import { display } from '#lib/explorer.js';
	import { formatBytes, formatDuration, formatNumber } from '#lib/format.js';

	/** What the process reports, as the JDK's platform MBeans name it. */
	const READ: Record<string, string[]> = {
		'java.lang.Runtime': [
			'VmName',
			'VmVendor',
			'VmVersion',
			'Pid',
			'StartTime',
			'Uptime',
			'SystemProperties',
			'InputArguments'
		],
		'java.lang.Memory': ['HeapMemoryUsage', 'NonHeapMemoryUsage'],
		'java.lang.OperatingSystem': [
			'Name',
			'Version',
			'Arch',
			'AvailableProcessors',
			'SystemLoadAverage',
			'CpuLoad',
			'ProcessCpuLoad',
			'TotalMemorySize',
			'FreeMemorySize',
			'OpenFileDescriptorCount',
			'MaxFileDescriptorCount'
		],
		'java.lang.Threading': [
			'ThreadCount',
			'PeakThreadCount',
			'DaemonThreadCount',
			'TotalStartedThreadCount'
		]
	};

	let tick = $state(0);
	let tab = $state('metrics');

	// Each as the live stream reads it every tick.
	const VIEWS = Object.entries(READ).map(
		([resource, names]) => [resource, { kind: 'attributes', resource, names } as View] as const
	);
	$effect(() => {
		const stop = VIEWS.map(([, v]) => live.watch(v));
		return () => stop.forEach((f) => f());
	});
	$effect(() => {
		void tick;
		live.refresh();
	});
	const values = $derived(
		Object.fromEntries(
			VIEWS.map(([r, v]) => [r, (live.value(v)?.value ?? {}) as Record<string, AttributeValue>])
		)
	);
	const error = $derived(
		VIEWS.every(([, v]) => live.value(v)?.error) ? 'The process cannot be read.' : null
	);

	const v = (r: string, a: string) => values[`java.lang.${r}`]?.[a]?.value;
	const n = (r: string, a: string) => {
		const x = v(r, a);
		return typeof x === 'number' && x >= 0 ? x : undefined;
	};
	const percent = (x: number | undefined) =>
		x === undefined ? undefined : `${(x * 100).toFixed(1)} %`;
	const memory = (a: string) =>
		v('Memory', a) as { used?: number; committed?: number; max?: number } | undefined;

	const cards = $derived([
		['Process', [v('Runtime', 'VmName'), v('Runtime', 'VmVersion')].filter(Boolean).join(' ')],
		['Process ID', v('Runtime', 'Pid') === undefined ? undefined : String(v('Runtime', 'Pid'))],
		[
			'Up',
			n('Runtime', 'Uptime') !== undefined ? formatDuration(n('Runtime', 'Uptime')!) : undefined
		],
		[
			'Started',
			n('Runtime', 'StartTime') ? new Date(n('Runtime', 'StartTime')!).toLocaleString() : undefined
		],
		[
			'Memory used',
			memory('HeapMemoryUsage')?.used !== undefined
				? formatBytes(memory('HeapMemoryUsage')!.used)
				: undefined
		],
		['Process CPU', percent(n('OperatingSystem', 'ProcessCpuLoad'))],
		['System CPU', percent(n('OperatingSystem', 'CpuLoad'))],
		['Load average', n('OperatingSystem', 'SystemLoadAverage')?.toFixed(2)],
		['Processors', n('OperatingSystem', 'AvailableProcessors')],
		[
			'System',
			[v('OperatingSystem', 'Name'), v('OperatingSystem', 'Version'), v('OperatingSystem', 'Arch')]
				.filter(Boolean)
				.join(' ')
		],
		[
			'Memory',
			n('OperatingSystem', 'TotalMemorySize') !== undefined
				? `${formatBytes(n('OperatingSystem', 'FreeMemorySize'))} free of ${formatBytes(n('OperatingSystem', 'TotalMemorySize'))}`
				: undefined
		],
		[
			'Open files',
			n('OperatingSystem', 'OpenFileDescriptorCount') !== undefined
				? `${formatNumber(n('OperatingSystem', 'OpenFileDescriptorCount'))} of ${formatNumber(n('OperatingSystem', 'MaxFileDescriptorCount'))}`
				: undefined
		],
		['Threads', n('Threading', 'ThreadCount')],
		['Most threads', n('Threading', 'PeakThreadCount')],
		['Threads started', n('Threading', 'TotalStartedThreadCount')]
	] as [string, unknown][]);

	let propertyFilter = $state('');
	const properties = $derived(
		Object.entries((v('Runtime', 'SystemProperties') as Record<string, string> | undefined) ?? {})
			.filter(([k, val]) => {
				const q = propertyFilter.trim().toLowerCase();
				return !q || k.toLowerCase().includes(q) || String(val).toLowerCase().includes(q);
			})
			.sort(([a], [b]) => a.localeCompare(b))
	);
	const args = $derived((v('Runtime', 'InputArguments') as string[] | undefined) ?? []);

	interface ThreadInfo {
		threadId: number;
		threadName: string;
		threadState: string;
		blockedCount?: number;
		waitedCount?: number;
		[other: string]: unknown;
	}

	let threads = $state<ThreadInfo[] | null>(null);
	let threadError = $state<string | null>(null);

	$effect(() => {
		if (tab !== 'threads') return;
		void tick;
		resources
			.invoke<ThreadInfo[]>('java.lang.Threading', 'dumpAllThreads(boolean,boolean)', [
				false,
				false
			])
			.then((t) => {
				threads = [...t].sort((a, b) => a.threadId - b.threadId);
				threadError = null;
			})
			.catch((e: Error) => {
				threads = null;
				threadError = e.message;
			});
	});
</script>

<svelte:head>
	<title>Runtime · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	<div class="flex items-center justify-between gap-3">
		<h1 class="text-xl font-semibold">Runtime</h1>
		<Button variant="ghost" size="icon-sm" aria-label="Refresh" onclick={() => tick++}>
			<RefreshCwIcon />
		</Button>
	</div>
	{#if error}
		<Alert.Root variant="destructive"><Alert.Description>{error}</Alert.Description></Alert.Root>
	{/if}
	<Tabs.Root bind:value={tab}>
		<Tabs.List>
			<Tabs.Trigger value="metrics">Process</Tabs.Trigger>
			<Tabs.Trigger value="properties">System properties</Tabs.Trigger>
			<Tabs.Trigger value="threads">Threads</Tabs.Trigger>
		</Tabs.List>
		<Tabs.Content value="metrics" class="mt-3 grid gap-4">
			<section
				class="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5"
				aria-label="The process"
			>
				{#each cards as [label, value] (label)}
					<Card.Root class="gap-1 py-3">
						<Card.Header class="px-3"><Card.Description>{label}</Card.Description></Card.Header>
						<Card.Content class="px-3 text-sm font-medium break-words">
							{#if value === undefined || value === ''}
								<span class="font-normal text-muted-foreground">Not reported</span>
							{:else}
								{typeof value === 'number' ? formatNumber(value) : value}
							{/if}
						</Card.Content>
					</Card.Root>
				{/each}
			</section>
			{#if args.length}
				<Card.Root>
					<Card.Header><Card.Title class="text-sm">Arguments</Card.Title></Card.Header>
					<Card.Content>
						<pre class="font-mono text-xs break-all whitespace-pre-wrap">{args.join('\n')}</pre>
					</Card.Content>
				</Card.Root>
			{/if}
		</Tabs.Content>
		<Tabs.Content value="properties" class="mt-3 grid gap-3">
			<Input
				aria-label="Find a property"
				placeholder="Find a property"
				bind:value={propertyFilter}
			/>
			<div class="overflow-x-auto rounded-lg border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Property</Table.Head>
							<Table.Head>Value</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each properties as [k, val] (k)}
							<Table.Row>
								<Table.Cell class="align-top font-mono text-xs">{k}</Table.Cell>
								<Table.Cell class="font-mono text-xs break-all whitespace-pre-wrap">
									{display(val)}
								</Table.Cell>
							</Table.Row>
						{:else}
							<Table.Row>
								<Table.Cell colspan={2} class="py-6 text-center text-muted-foreground">
									{v('Runtime', 'SystemProperties') === undefined
										? 'Not permitted.'
										: 'No property matches.'}
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		</Tabs.Content>
		<Tabs.Content value="threads" class="mt-3 grid gap-3">
			{#if threadError}
				<Alert.Root>
					<Alert.Title>The threads cannot be listed</Alert.Title>
					<Alert.Description>{threadError}</Alert.Description>
				</Alert.Root>
			{:else if threads}
				<div class="overflow-x-auto rounded-lg border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head class="text-right">ID</Table.Head>
								<Table.Head>Name</Table.Head>
								<Table.Head>State</Table.Head>
								<Table.Head class="text-right">Blocked</Table.Head>
								<Table.Head class="text-right">Waited</Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each threads as t (t.threadId)}
								<Table.Row>
									<Table.Cell class="text-right font-mono tabular-nums">{t.threadId}</Table.Cell>
									<Table.Cell class="font-mono text-xs">{t.threadName}</Table.Cell>
									<Table.Cell>{t.threadState}</Table.Cell>
									<Table.Cell class="text-right tabular-nums">{t.blockedCount ?? ''}</Table.Cell>
									<Table.Cell class="text-right tabular-nums">{t.waitedCount ?? ''}</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>
	</Tabs.Root>
</div>
