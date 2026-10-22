<!--
	High availability: each node's policy, its backup and whether the backup
	is in sync, read live from the node; the lock coordinators of the nodes
	that have them; and the actions that make a backup take over: forcing a
	node to fail over, and freezing its replication.
-->
<script lang="ts">
	import * as Table from '#lib/components/ui/table/index.js';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import EllipsisIcon from '@lucide/svelte/icons/ellipsis';
	import { toast } from 'svelte-sonner';
	import { resources, type View } from '#lib/api/index.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { display } from '#lib/explorer.js';
	import { overviewHref } from '#lib/nav.js';
	import { nodes, type Node } from '#lib/state/nodes.svelte.js';
	import { live } from '#lib/state/live.svelte.js';

	const statusView = (n: Node): View => ({ kind: 'broker', node: n.id });
	$effect(() => {
		const stops = nodes.members.map((n) => live.watch(statusView(n)));
		return () => stops.forEach((s) => s());
	});
	function status(n: Node) {
		const v = live.value(statusView(n));
		const attrs = (v?.value as { attributes?: Record<string, unknown> } | undefined)?.attributes;
		return { attrs, error: v?.error };
	}

	const ACTIONS = ['forceFailover', 'freezeReplication'] as const;
	type Action = (typeof ACTIONS)[number];

	// What each node lets the user do, and its lock coordinators.
	let may = $state<Record<string, Allowed>>({});
	let locks = $state<Record<string, Record<string, unknown>[] | null>>({});
	$effect(() => {
		for (const n of nodes.members) {
			const id = n.id;
			allowed('broker', ACTIONS, id).then((a) => (may[id] = a));
			resources
				.invoke<string>('broker', 'listLockCoordinatorsAsJSON()', [], id)
				.then((j) => (locks[id] = JSON.parse(j)))
				.catch(() => (locks[id] = null));
		}
	});

	/** Freezing replication does something only on a node replicating with quorum voting. */
	function replicates(policy: unknown): boolean {
		return typeof policy === 'string' && policy.endsWith('w/quorum voting');
	}

	const CONFIRM: Record<Action, { title: string; text: string; verb: string }> = {
		forceFailover: {
			title: 'Force a failover',
			text: 'The node stops, telling its backup to take over: a replicating backup is told, and a shared store backup takes the lock. Clients fail over to the backup. The node does not start again by itself.',
			verb: 'Fail over'
		},
		freezeReplication: {
			title: 'Freeze replication',
			text: 'The connection of the backup replicating this node fails. The backup no longer has a copy in sync, and may take over if it cannot reach this node.',
			verb: 'Freeze'
		}
	};
	let confirm = $state<{ node: Node; action: Action } | null>(null);

	async function run(node: Node, action: Action) {
		confirm = null;
		try {
			const r = await resources.invoke(`broker`, `${action}()`, [], node.id);
			if (action === 'freezeReplication' && r === false)
				toast.info(`${node.name} does not replicate with quorum voting: nothing to freeze`);
			else toast.success(`${CONFIRM[action].title}: ${node.name}`);
		} catch (e) {
			toast.error(`${CONFIRM[action].title} failed`, { description: (e as Error).message });
		}
	}

	const lockColumns = (l: Record<string, unknown>[]) => (l.length ? Object.keys(l[0]) : []);
</script>

<svelte:head>
	<title>High availability · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-6">
	<h1 class="text-xl font-semibold">High availability</h1>

	<section class="flex flex-col gap-2" aria-labelledby="pairs">
		<h2 id="pairs" class="font-semibold">Primaries and backups</h2>
		<div class="overflow-x-auto rounded-md border">
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Node</Table.Head>
						<Table.Head>Policy</Table.Head>
						<Table.Head>Role</Table.Head>
						<Table.Head>Connector</Table.Head>
						<Table.Head>Backup</Table.Head>
						<Table.Head>Backup group</Table.Head>
						<Table.Head>Replica</Table.Head>
						<Table.Head><span class="sr-only">Actions</span></Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each nodes.members as n (n.id)}
						{@const s = status(n)}
						{@const a = s.attrs}
						<Table.Row>
							<Table.Cell class="font-medium">
								<a class="hover:underline" href={overviewHref(n.id)}>{n.name}</a>
							</Table.Cell>
							{#if s.error}
								<Table.Cell colspan={6} class="text-sm text-destructive">{s.error}</Table.Cell>
							{:else if !a}
								<Table.Cell colspan={6}><Skeleton class="h-5 w-full" /></Table.Cell>
							{:else}
								<Table.Cell>{display(a.HAPolicy) || '–'}</Table.Cell>
								<Table.Cell>
									{#if a.Backup === true}<Badge variant="secondary">Backup</Badge>{:else}<Badge
											>Live</Badge
										>{/if}
								</Table.Cell>
								<Table.Cell class="font-mono text-xs"
									>{n.primary ?? (n.local ? 'here' : '–')}</Table.Cell
								>
								<Table.Cell class="font-mono text-xs">
									{#if n.backupAt}{n.backupAt}{:else}<span class="text-muted-foreground">None</span
										>{/if}
								</Table.Cell>
								<Table.Cell>{n.backupGroup ?? '–'}</Table.Cell>
								<Table.Cell>
									{#if a.SharedStore === true}
										Shared store
									{:else if a.ReplicaSync === true}
										<Badge>In sync</Badge>
									{:else if typeof a.HAPolicy === 'string' && a.HAPolicy.startsWith('Replication')}
										<Badge variant="secondary">Not in sync</Badge>
									{:else}
										<span class="text-muted-foreground">–</span>
									{/if}
								</Table.Cell>
							{/if}
							<Table.Cell class="text-right">
								<DropdownMenu.Root>
									<DropdownMenu.Trigger>
										{#snippet child({ props })}
											<Button
												{...props}
												variant="ghost"
												size="icon-sm"
												aria-label={`Actions on ${n.name}`}
												disabled={!!s.error}
											>
												<EllipsisIcon />
											</Button>
										{/snippet}
									</DropdownMenu.Trigger>
									<DropdownMenu.Content align="end">
										<DropdownMenu.Item
											disabled={may[n.id]?.forceFailover === false}
											onSelect={() => (confirm = { node: n, action: 'forceFailover' })}
										>
											Force a failover…
										</DropdownMenu.Item>
										<DropdownMenu.Item
											disabled={may[n.id]?.freezeReplication === false || !replicates(a?.HAPolicy)}
											onSelect={() => (confirm = { node: n, action: 'freezeReplication' })}
										>
											Freeze replication…
										</DropdownMenu.Item>
									</DropdownMenu.Content>
								</DropdownMenu.Root>
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	</section>

	<section class="flex flex-col gap-2" aria-labelledby="locks">
		<h2 id="locks" class="font-semibold">Lock coordinators</h2>
		{#each nodes.members as n (n.id)}
			{@const l = locks[n.id]}
			{#if l && l.length}
				<h3 class="text-sm font-medium">{n.name}</h3>
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								{#each lockColumns(l) as k (k)}<Table.Head>{k}</Table.Head>{/each}
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each l as row, i (i)}
								<Table.Row>
									{#each lockColumns(l) as k (k)}
										<Table.Cell class="text-xs">{display(row[k])}</Table.Cell>
									{/each}
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		{/each}
		{#if nodes.members.every((n) => locks[n.id] !== undefined && !locks[n.id]?.length)}
			<p class="text-sm text-muted-foreground">No node has a lock coordinator.</p>
		{/if}
	</section>
</div>

<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
	<AlertDialog.Content>
		{#if confirm}
			<AlertDialog.Header>
				<AlertDialog.Title
					>{CONFIRM[confirm.action].title} on {confirm.node.name}?</AlertDialog.Title
				>
				<AlertDialog.Description>{CONFIRM[confirm.action].text}</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action
					class="bg-destructive text-white hover:bg-destructive/90"
					onclick={() => confirm && run(confirm.node, confirm.action)}
				>
					{CONFIRM[confirm.action].verb}
				</AlertDialog.Action>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
