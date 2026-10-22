<script lang="ts">
	import { cn } from '#lib/utils.js';

	let { class: className = '' }: { class?: string } = $props();
</script>

<!-- Three waves: messages in flight. -->
<svg viewBox="0 0 32 32" class={cn('text-primary', className)} aria-hidden="true">
	<rect width="32" height="32" rx="8" fill="currentColor" />
	<g fill="none" stroke="white" stroke-width="2.6" stroke-linecap="round">
		<path d="M7 11c3-3 6 3 9 0s6-3 9 0" />
		<path d="M7 16.5c3-3 6 3 9 0s6-3 9 0" opacity="0.8" />
		<path d="M7 22c3-3 6 3 9 0s6-3 9 0" opacity="0.6" />
	</g>
</svg>
