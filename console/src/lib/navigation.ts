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
import NetworkIcon from '@lucide/svelte/icons/network';
import FolderTreeIcon from '@lucide/svelte/icons/folder-tree';
import CpuIcon from '@lucide/svelte/icons/cpu';
import BellIcon from '@lucide/svelte/icons/bell';
import ServerCogIcon from '@lucide/svelte/icons/server-cog';
import ShieldCheckIcon from '@lucide/svelte/icons/shield-check';
import CableIcon from '@lucide/svelte/icons/cable';
import {
	clusterHref,
	diagramHref,
	federationHref,
	haHref,
	eventsHref,
	explorerHref,
	listingHref,
	overviewHref,
	preferencesHref,
	runtimeHref
} from '#lib/nav.js';

export interface NavItem {
	title: string;
	icon: Component;
	href: (node: string) => string;
	/** The broker operation the page needs; without it the page is not offered. */
	needs?: string;
	keywords?: string[];
	/** It shows every node at once: offered across every node. */
	everyNode?: boolean;
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
		keywords,
		everyNode: true
	};
}

/** The routes that show every node at once, which `all` offers. */
export const EVERY_NODE = ['/n/[node]', '/n/[node]/[listing]', '/n/[node]/cluster', '/n/[node]/ha'];

export const NAVIGATION: NavGroup[] = [
	{
		label: 'Broker',
		items: [
			{
				title: 'Overview',
				icon: LayoutDashboardIcon,
				href: overviewHref,
				keywords: ['status', 'home'],
				everyNode: true
			},
			{
				title: 'Diagram',
				icon: NetworkIcon,
				href: diagramHref,
				needs: 'listAddresses',
				keywords: ['topology', 'graph', 'cluster']
			},
			{
				title: 'Events',
				icon: BellIcon,
				href: (node) => eventsHref(node),
				keywords: ['notifications', 'feed', 'activemq.notifications']
			}
		]
	},
	{
		label: 'Cluster',
		items: [
			{
				title: 'Nodes',
				icon: ServerCogIcon,
				href: clusterHref,
				keywords: ['cluster', 'topology', 'redistribution', 'load balancing'],
				everyNode: true
			},
			{
				title: 'High availability',
				icon: ShieldCheckIcon,
				href: haHref,
				keywords: ['ha', 'backup', 'replication', 'failover', 'shared store', 'lock'],
				everyNode: true
			},
			{
				title: 'Bridges and connections',
				icon: CableIcon,
				href: federationHref,
				keywords: ['bridges', 'federation', 'broker connections', 'mirror', 'amqp']
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
		label: 'Tools',
		items: [
			{
				title: 'Explorer',
				icon: FolderTreeIcon,
				href: (node) => explorerHref(node),
				keywords: ['jmx', 'mbeans', 'attributes', 'operations', 'resources']
			},
			{
				title: 'Runtime',
				icon: CpuIcon,
				href: runtimeHref,
				keywords: ['system properties', 'threads', 'memory', 'process']
			}
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
