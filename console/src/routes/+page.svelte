<script lang="ts">
	import { goto } from '$app/navigation';
	import { overviewHref } from '#lib/nav.js';
	import { LOCAL } from '#lib/state/nodes.svelte.js';

	$effect(() => {
		goto(overviewHref(LOCAL), { replace: true });
	});
</script>
