<script lang="ts">
	import * as Sidebar from '#lib/components/ui/sidebar/index.js';
	import Logo from './Logo.svelte';
	import NodePicker from './NodePicker.svelte';
	import { NAVIGATION } from '#lib/navigation.js';
	import { session } from '#lib/state/session.svelte.js';
	import { ALL } from '#lib/state/nodes.svelte.js';
	import { page } from '$app/state';

	let { node }: { node: string } = $props();

	/** The path of the page shown, from the fragment. */
	const current = $derived(page.url.hash.replace(/\?.*$/, ''));
</script>

<Sidebar.Root collapsible="icon">
	<Sidebar.Header>
		<div class="flex items-center gap-2 px-1 py-1.5">
			<Logo class="size-7 shrink-0" />
			<span class="truncate font-semibold group-data-[collapsible=icon]:hidden">Saranga</span>
		</div>
		<NodePicker {node} />
	</Sidebar.Header>
	<Sidebar.Content>
		{#each NAVIGATION as group (group.label)}
			{@const items = group.items.filter(
				(i) => (!i.needs || session.may(i.needs)) && (node !== ALL || i.everyNode)
			)}
			{#if items.length}
				<Sidebar.Group>
					<Sidebar.GroupLabel>{group.label}</Sidebar.GroupLabel>
					<Sidebar.Menu>
						{#each items as item (item.title)}
							{@const href = item.href(node)}
							<Sidebar.MenuItem>
								<Sidebar.MenuButton isActive={current === href} tooltipContent={item.title}>
									{#snippet child({ props })}
										<a {href} {...props} aria-current={current === href ? 'page' : undefined}>
											<item.icon />
											<span>{item.title}</span>
										</a>
									{/snippet}
								</Sidebar.MenuButton>
							</Sidebar.MenuItem>
						{/each}
					</Sidebar.Menu>
				</Sidebar.Group>
			{/if}
		{/each}
	</Sidebar.Content>
	<Sidebar.Rail />
</Sidebar.Root>
