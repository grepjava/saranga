<!--
	A time series in uPlot: `data` is the times (in seconds) and a series per
	label. Colours come from the theme's chart tokens.
-->
<script lang="ts">
	import { onMount } from 'svelte';
	import uPlot from 'uplot';
	import 'uplot/dist/uPlot.min.css';

	let {
		data,
		labels,
		height = 180,
		format = (v: number) => String(v)
	}: {
		data: number[][];
		labels: string[];
		height?: number;
		format?: (v: number) => string;
	} = $props();

	let host: HTMLDivElement;
	let plot: uPlot | undefined;

	function colour(i: number): string {
		const v = getComputedStyle(host)
			.getPropertyValue(`--chart-${(i % 5) + 1}`)
			.trim();
		return v || 'currentColor';
	}

	function build(width: number) {
		plot?.destroy();
		const axis = {
			stroke: getComputedStyle(host).getPropertyValue('--muted-foreground').trim(),
			grid: { stroke: getComputedStyle(host).getPropertyValue('--border').trim(), width: 1 },
			ticks: { show: false }
		};
		plot = new uPlot(
			{
				width,
				height,
				legend: { show: labels.length > 1 },
				cursor: { drag: { x: false, y: false } },
				axes: [axis, { ...axis, values: (_u, vals) => vals.map((v) => format(v)), size: 60 }],
				series: [
					{},
					...labels.map((label, i) => ({
						label,
						stroke: colour(i),
						width: 2,
						value: (_u: uPlot, v: number | null) => (v === null ? '' : format(v))
					}))
				]
			},
			data as uPlot.AlignedData,
			host
		);
	}

	onMount(() => {
		build(host.clientWidth);
		const observer = new ResizeObserver(() => {
			if (plot && host.clientWidth && plot.width !== host.clientWidth)
				plot.setSize({ width: host.clientWidth, height });
		});
		observer.observe(host);
		return () => {
			observer.disconnect();
			plot?.destroy();
		};
	});

	$effect(() => {
		const d = data;
		plot?.setData(d as uPlot.AlignedData);
	});

	// New series need a new plot.
	$effect(() => {
		const n = labels.length;
		if (plot && plot.series.length !== n + 1) build(host.clientWidth);
	});
</script>

<div
	bind:this={host}
	class="w-full text-xs"
	style:height="{height + (labels.length > 1 ? 32 : 0)}px"
></div>
