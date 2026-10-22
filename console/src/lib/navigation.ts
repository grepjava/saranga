// The console's pages, as the sidebar and the command palette offer them.

import type { Component } from 'svelte';
import LayoutDashboardIcon from '@lucide/svelte/icons/layout-dashboard';
import PlugIcon from '@lucide/svelte/icons/plug';
import LayersIcon from '@lucide/svelte/icons/layers';
import ArrowDownToLineIcon from '@lucide/svelte/icons/arrow-down-to-line';
import ArrowUpFromLineIcon from '@lucide/svelte/icons/arrow-up-from-line';
import WaypointsIcon from '@lucide/svelte/icons/waypoints';
import InboxIcon from '@lucide/svelte/icons/inbox';
import Settings2Icon from '@lucide/svelte/icons/settings-2';
import type { Listing } from '#lib/api/index.js';
import { LISTINGS } from '#lib/listings.js';
import { listingHref, overviewHref, preferencesHref } from '#lib/nav.js';

export interface NavItem {
	title: string;
	icon: Component;
	href: (node: string) => string;
	/** The broker operation the page needs; without it the page is not offered. */
	needs?: string;
	keywords?: string[];
}

export interface NavGroup {
	label: string;
	items: NavItem[];
}

const LIST_OPERATIONS: Record<Listing, string> = {
	connections: 'listConnections',
	sessions: 'listSessions',
	consumers: 'listConsumers',
	producers: 'listProducers',
	addresses: 'listAddresses',
	queues: 'listQueues'
};

function listing(kind: Listing, icon: Component, keywords: string[] = []): NavItem {
	return {
		title: LISTINGS[kind].title,
		icon,
		href: (node) => listingHref(node, kind),
		needs: LIST_OPERATIONS[kind],
		keywords
	};
}

export const NAVIGATION: NavGroup[] = [
	{
		label: 'Broker',
		items: [
			{
				title: 'Overview',
				icon: LayoutDashboardIcon,
				href: overviewHref,
				keywords: ['status', 'home']
			}
		]
	},
	{
		label: 'Clients',
		items: [
			listing('connections', PlugIcon, ['clients']),
			listing('sessions', LayersIcon),
			listing('consumers', ArrowDownToLineIcon, ['subscribers', 'receivers']),
			listing('producers', ArrowUpFromLineIcon, ['senders', 'publishers'])
		]
	},
	{
		label: 'Messaging',
		items: [
			listing('addresses', WaypointsIcon, ['topics', 'destinations']),
			listing('queues', InboxIcon, ['destinations', 'subscriptions'])
		]
	},
	{
		label: 'Console',
		items: [
			{
				title: 'Preferences',
				icon: Settings2Icon,
				href: preferencesHref,
				keywords: ['settings', 'theme', 'page size']
			}
		]
	}
];
