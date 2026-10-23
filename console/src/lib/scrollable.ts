// A region that scrolls is reached by the keyboard only if it can take the
// focus (WCAG 2.1.1, axe's scrollable-region-focusable): while it overflows,
// it can; otherwise it is left out of the tab order.

import type { Attachment } from 'svelte/attachments';

export const scrollable: Attachment<HTMLElement> = (node) => {
	const update = () => {
		const overflows =
			node.scrollWidth > node.clientWidth + 1 || node.scrollHeight > node.clientHeight + 1;
		if (overflows) node.tabIndex = 0;
		else node.removeAttribute('tabindex');
	};
	const resized = new ResizeObserver(update);
	resized.observe(node);
	// What it holds changes its size without the region's changing.
	const changed = new MutationObserver(update);
	changed.observe(node, { childList: true, subtree: true, characterData: true });
	update();
	return () => {
		resized.disconnect();
		changed.disconnect();
	};
};
