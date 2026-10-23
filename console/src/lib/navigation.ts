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
import SlidersHorizontalIcon from '@lucide/svelte/icons/sliders-horizontal';
import KeyRoundIcon from '@lucide/svelte/icons/key-round';
import ArrowLeftRightIcon from '@lucide/svelte/icons/arrow-left-right';
import MailWarningIcon from '@lucide/svelte/icons/mail-warning';
import {
	clusterHref,
	configurationHref,
	deadLettersHref,
	diagramHref,
	federationHref,
	haHref,
	eventsHref,
	explorerHref,
	listingHref,
	overviewHref,
	preferencesHref,
	runtimeHref,
	securityHref,
	transactionsHref
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
	/** It shows the console's own process, which management messages cannot read: offered for this broker only. */
	thisBroker?: boolean;
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
		label: 'Administration',
		items: [
			{
				title: 'Dead letters',
				icon: MailWarningIcon,
				href: (node) => deadLettersHref(node),
				needs: 'listQueues',
				keywords: ['dlq', 'dead letter queue', 'expiry', 'retry', 'failed']
			},
			{
				title: 'Configuration',
				icon: SlidersHorizontalIcon,
				href: (node) => configurationHref(node),
				needs: 'getAddressSettingsAsJSON',
				keywords: [
					'address settings',
					'diverts',
					'acceptors',
					'reload',
					'export',
					'broker properties'
				]
			},
			{
				title: 'Security',
				icon: KeyRoundIcon,
				href: (node) => securityHref(node),
				needs: 'getRolesAsJSON',
				keywords: ['users', 'roles', 'permissions', 'security settings', 'rbac']
			},
			{
				title: 'Transactions',
				icon: ArrowLeftRightIcon,
				href: transactionsHref,
				needs: 'listPreparedTransactionDetailsAsJSON',
				keywords: ['xa', 'prepared', 'heuristic', 'commit', 'rollback']
			}
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
				keywords: ['system properties', 'threads', 'memory', 'process'],
				thisBroker: true
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
