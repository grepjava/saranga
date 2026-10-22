<!--
	A resource's counters over time: the broker's history of them (the last
	hour), then a point each tick of the live stream. With `rate`, each series
	is its change per second, for counters that only grow.
-->
<script lang="ts">
	import { untrack } from 'svelte';
	import Chart from './Chart.svelte';
	import { live as history, type View } from '#lib/api/index.js';
	import { prepend, rates } from '#lib/series.js';
	import { attributeValues, live } from '#lib/state/live.svelte.js';

	let {
		resource,
		series,
		rate = false,
		height = 160,
		format = (v: number) => String(v)
	}: {
		/** `broker`, `queue.<name>`, `address.<name>`. */
		resource: string;
		/** The attributes charted, with their labels. */
		series: { name: string; label: string }[];
		rate?: boolean;
		height?: number;
		format?: (v: number) => string;
	} = $props();

	/** How far back the chart reaches. */
	const WINDOW_MS = 3600_000;

	const names = $derived(series.map((s) => s.name));
	const view = $derived<View>({ kind: 'attributes', resource, names });

	$effect(() => live.watch(view));

	let data = $state.raw<number[][]>([[]]);

	$effect(() => {
		const r = resource;
		const wanted = names;
		const v = view;
		const perSecond = rate;
		let times: number[] = [];
		let values: (number | null)[][] = wanted.map(() => []);
		let stale = false;

		const draw = () => {
			const cut = times.findIndex((t) => t >= times[times.length - 1] - WINDOW_MS);
			if (cut > 0) {
				times = times.slice(cut);
				values = values.map((s) => s.slice(cut));
			}
			const shown = perSecond ? values.map((s) => rates(times, s)) : values;
			data = [times.map((t) => t / 1000), ...(shown as number[][])];
		};
		draw();

		history
			.history([r])
			.then((h) => {
				const past = h[r];
				if (stale || !past?.times?.length) return;
				({ times, values } = prepend(
					{ times: past.times, attributes: past.attributes ?? {} },
					wanted,
					times,
					values
				));
				draw();
			})
			.catch(() => {
				// Without its history, the chart starts now.
			});

		const off = live.onTick((t) => {
			const now = attributeValues(untrack(() => live.value(v)));
			if (!now) return;
			times.push(t);
			wanted.forEach((name, i) => {
				const x = now[name];
				values[i].push(typeof x === 'number' ? x : null);
			});
			draw();
		});
		return () => {
			stale = true;
			off();
		};
	});
</script>

<Chart {data} labels={series.map((s) => s.label)} {height} {format} />
