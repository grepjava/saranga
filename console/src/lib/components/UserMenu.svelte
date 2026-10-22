<script lang="ts">
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import CircleUserIcon from '@lucide/svelte/icons/circle-user';
	import LogOutIcon from '@lucide/svelte/icons/log-out';
	import { session } from '#lib/state/session.svelte.js';
</script>

<DropdownMenu.Root>
	<DropdownMenu.Trigger>
		{#snippet child({ props })}
			<Button {...props} variant="ghost" size="sm" aria-label="User">
				<CircleUserIcon />
				<span class="hidden sm:inline">{session.me?.user}</span>
			</Button>
		{/snippet}
	</DropdownMenu.Trigger>
	<DropdownMenu.Content align="end" class="min-w-48">
		<DropdownMenu.Label>
			<div class="font-medium">{session.me?.user}</div>
			<div class="text-xs font-normal text-muted-foreground">
				{session.me?.roles.join(', ') || 'No roles'}
			</div>
		</DropdownMenu.Label>
		<DropdownMenu.Separator />
		<DropdownMenu.Item onSelect={() => session.logout()}>
			<LogOutIcon />
			Log out
		</DropdownMenu.Item>
	</DropdownMenu.Content>
</DropdownMenu.Root>
