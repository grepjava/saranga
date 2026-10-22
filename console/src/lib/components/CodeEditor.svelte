<!--
	A CodeMirror 6 editor for a message body: JSON or XML highlighted when the
	text is either, and colours from the theme's tokens, so it follows light
	and dark.
-->
<script lang="ts">
	import { onMount } from 'svelte';
	import { EditorState, Compartment } from '@codemirror/state';
	import {
		EditorView,
		keymap,
		lineNumbers,
		highlightActiveLine,
		placeholder as placeholderText
	} from '@codemirror/view';
	import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
	import { syntaxHighlighting, HighlightStyle, bracketMatching } from '@codemirror/language';
	import { json } from '@codemirror/lang-json';
	import { xml } from '@codemirror/lang-xml';
	import { tags } from '@lezer/highlight';

	let {
		value = $bindable(''),
		readonly = false,
		label,
		placeholder = '',
		class: className = ''
	}: {
		value?: string;
		readonly?: boolean;
		label: string;
		placeholder?: string;
		class?: string;
	} = $props();

	let host: HTMLDivElement;
	let view: EditorView | undefined;
	const language = new Compartment();
	const editable = new Compartment();

	function languageOf(text: string) {
		const t = text.trimStart();
		if (t.startsWith('{') || t.startsWith('[')) return json();
		if (t.startsWith('<')) return xml();
		return [];
	}

	const highlight = HighlightStyle.define([
		{ tag: [tags.string, tags.attributeValue], color: 'var(--chart-2)' },
		{ tag: [tags.number, tags.bool, tags.null], color: 'var(--chart-1)' },
		{ tag: [tags.propertyName, tags.tagName], color: 'var(--chart-4)' },
		{ tag: [tags.attributeName], color: 'var(--chart-5)' },
		{ tag: [tags.comment], color: 'var(--muted-foreground)', fontStyle: 'italic' }
	]);

	const theme = EditorView.theme({
		'&': {
			fontSize: '0.8125rem',
			backgroundColor: 'transparent',
			color: 'var(--foreground)',
			height: '100%'
		},
		'.cm-scroller': { fontFamily: 'var(--font-mono, ui-monospace, monospace)' },
		'.cm-gutters': {
			backgroundColor: 'transparent',
			color: 'var(--muted-foreground)',
			border: 'none'
		},
		'.cm-activeLine, .cm-activeLineGutter': { backgroundColor: 'var(--muted)' },
		'&.cm-focused': { outline: 'none' },
		'.cm-cursor': { borderLeftColor: 'var(--foreground)' },
		'.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
			backgroundColor: 'var(--accent) !important'
		}
	});

	onMount(() => {
		view = new EditorView({
			parent: host,
			state: EditorState.create({
				doc: value,
				extensions: [
					lineNumbers(),
					highlightActiveLine(),
					history(),
					bracketMatching(),
					keymap.of([...defaultKeymap, ...historyKeymap]),
					syntaxHighlighting(highlight),
					theme,
					EditorView.lineWrapping,
					EditorView.contentAttributes.of({ 'aria-label': label }),
					placeholderText(placeholder),
					language.of(languageOf(value)),
					editable.of([EditorState.readOnly.of(readonly), EditorView.editable.of(!readonly)]),
					EditorView.updateListener.of((u) => {
						if (!u.docChanged) return;
						const text = u.state.doc.toString();
						value = text;
						u.view.dispatch({ effects: language.reconfigure(languageOf(text)) });
					})
				]
			})
		});
		return () => view?.destroy();
	});

	// The value set from outside, such as a body formatted or a message to resend.
	$effect(() => {
		const v = value;
		if (view && v !== view.state.doc.toString()) {
			view.dispatch({
				changes: { from: 0, to: view.state.doc.length, insert: v },
				effects: language.reconfigure(languageOf(v))
			});
		}
	});

	$effect(() => {
		const r = readonly;
		view?.dispatch({
			effects: editable.reconfigure([EditorState.readOnly.of(r), EditorView.editable.of(!r)])
		});
	});
</script>

<div
	bind:this={host}
	class="min-h-32 overflow-auto rounded-lg border border-input bg-background dark:bg-input/30 {className}"
></div>
