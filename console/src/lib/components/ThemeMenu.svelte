<script lang="ts">
	import * as DropdownMenu from '#lib/components/ui/dropdown-menu/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import SunIcon from '@lucide/svelte/icons/sun';
	import MoonIcon from '@lucide/svelte/icons/moon';
	import MonitorIcon from '@lucide/svelte/icons/monitor';
	import { setMode, userPrefersMode } from 'mode-watcher';

	const choices = [
		{ mode: 'light', label: 'Light', icon: SunIcon },
		{ mode: 'dark', label: 'Dark', icon: MoonIcon },
		{ mode: 'system', label: 'System', icon: MonitorIcon }
	] as const;
</script>

<DropdownMenu.Root>
	<DropdownMenu.Trigger>
		{#snippet child({ props })}
			<Button {...props} variant="ghost" size="icon" aria-label="Theme">
				<SunIcon class="scale-100 rotate-0 transition-all dark:scale-0 dark:-rotate-90" />
				<MoonIcon class="absolute scale-0 rotate-90 transition-all dark:scale-100 dark:rotate-0" />
			</Button>
		{/snippet}
	</DropdownMenu.Trigger>
	<DropdownMenu.Content align="end">
		<DropdownMenu.RadioGroup
			value={userPrefersMode.current}
			onValueChange={(v) => setMode(v as 'light' | 'dark' | 'system')}
		>
			{#each choices as c (c.mode)}
				<DropdownMenu.RadioItem value={c.mode} closeOnSelect>
					<c.icon />
					{c.label}
				</DropdownMenu.RadioItem>
			{/each}
		</DropdownMenu.RadioGroup>
	</DropdownMenu.Content>
</DropdownMenu.Root>
