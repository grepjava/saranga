<!--
	A node's security: the basic security manager's users, the roles each
	security match grants, and a checker of what a role or a user may do on
	an address. The caches of logins and permissions can be cleared.
-->
<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { toast } from 'svelte-sonner';
	import * as AlertDialog from '#lib/components/ui/alert-dialog/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Checkbox } from '#lib/components/ui/checkbox/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Skeleton } from '#lib/components/ui/skeleton/index.js';
	import SecuritySettingsEditor from '#lib/components/SecuritySettingsEditor.svelte';
	import { admin, LOCAL, type Role, type SecuritySettings } from '#lib/api/index.js';
	import { PERMISSIONS, security, type User } from '#lib/broker.js';
	import { allowed, type Allowed } from '#lib/can.js';
	import { hashQuery, securityHref } from '#lib/nav.js';
	import { grantedBy, permissionLabel, roleList } from '#lib/security.js';
	import type { PageProps } from './$types';

	let { params }: PageProps = $props();
	const local = $derived(params.node === LOCAL);

	const TABS = ['users', 'roles', 'check'] as const;
	const tab = $derived.by(() => {
		const t = hashQuery(page.url).get('tab');
		return TABS.find((x) => x === t) ?? 'users';
	});
	function show(t: string) {
		goto(securityHref(params.node, new URLSearchParams({ tab: t })), {
			replaceState: true
		});
	}

	let may = $state<Allowed>({});
	$effect(() => {
		allowed('broker', [
			'addUser',
			'resetUser',
			'removeUser',
			'addSecuritySettings',
			'removeSecuritySettings',
			'clearAuthenticationCache',
			'clearAuthorizationCache'
		]).then((a) => (may = a));
	});

	// Users.
	let users = $state<User[] | null>(null);
	let usersError = $state<string | null>(null);
	let usersTick = $state(0);
	$effect(() => {
		void usersTick;
		security
			.users()
			.then(
				(u) => (
					(users = u.sort((a, b) => a.username.localeCompare(b.username))),
					(usersError = null)
				)
			)
			.catch((e: Error) => (usersError = e.message));
	});

	let userForm = $state<{
		existing: boolean;
		user: string;
		password: string;
		roles: string;
		plaintext: boolean;
	} | null>(null);
	let userOpen = $state(false);
	let userError = $state<string | null>(null);
	function editUser(u?: User) {
		userForm = {
			existing: !!u,
			user: u?.username ?? '',
			password: '',
			roles: u?.roles.join(', ') ?? '',
			plaintext: false
		};
		userError = null;
		userOpen = true;
	}
	async function saveUser(e: SubmitEvent) {
		e.preventDefault();
		const f = userForm;
		if (!f) return;
		userError = null;
		try {
			if (f.existing) {
				await security.resetUser(f.user, f.password || null, roleList(f.roles), f.plaintext);
				toast.success(`Changed user ${f.user}`);
			} else {
				await security.addUser(f.user.trim(), f.password, roleList(f.roles), f.plaintext);
				toast.success(`Added user ${f.user.trim()}`);
			}
			userOpen = false;
			usersTick++;
		} catch (err) {
			userError = (err as Error).message;
		}
	}

	// Security settings.
	let settings = $state<SecuritySettings | null>(null);
	let settingsError = $state<string | null>(null);
	let settingsTick = $state(0);
	$effect(() => {
		void settingsTick;
		if (!local) return;
		admin
			.securitySettings()
			.then((s) => ((settings = s), (settingsError = null)))
			.catch((e: Error) => (settingsError = e.message));
	});
	let editing = $state<{ match: string; roles: Role[] } | null>(null);
	let editorOpen = $state(false);
	function edit(match = '', roles: Role[] = []) {
		editing = { match, roles };
		editorOpen = true;
	}

	// The checker.
	const query = $derived(hashQuery(page.url));
	let address = $state(untrackedQuery('address'));
	let who = $state(untrackedQuery('user'));
	function untrackedQuery(name: string): string {
		return hashQuery(page.url).get(name) ?? '';
	}
	let checked = $state<{
		address: string;
		match?: string | null;
		roles: Role[];
		user?: User;
	} | null>(null);
	let checkError = $state<string | null>(null);
	async function check() {
		const a = address.trim();
		checkError = null;
		try {
			const [roles, applying] = await Promise.all([
				security.roles(a) as Promise<Role[]>,
				local ? admin.securitySettings(a).then((s) => s.applying) : Promise.resolve(undefined)
			]);
			const user = who.trim() ? users?.find((u) => u.username === who.trim()) : undefined;
			if (who.trim() && !user) throw new Error(`No user ${who.trim()} is listed`);
			checked = { address: a, match: applying, roles, user };
			const q = new URLSearchParams({ tab: 'check', address: a });
			if (who.trim()) q.set('user', who.trim());
			goto(securityHref(params.node, q), { replaceState: true });
		} catch (err) {
			checkError = (err as Error).message;
		}
	}
	$effect(() => {
		// A shared link checks at once.
		if (tab === 'check' && query.get('address') && !checked && !checkError) check();
	});
	const granted = $derived(checked ? grantedBy(checked.roles, checked.user?.roles) : null);

	// What needs confirming.
	let confirm = $state<
		{ kind: 'remove-user'; user: string } | { kind: 'remove-settings'; match: string } | null
	>(null);
	async function confirmed() {
		const c = confirm;
		confirm = null;
		if (!c) return;
		try {
			if (c.kind === 'remove-user') {
				await security.removeUser(c.user);
				toast.success(`Removed user ${c.user}`);
				usersTick++;
			} else {
				await security.removeSecuritySettings(c.match);
				toast.success(`Removed the roles of ${c.match}`);
				settingsTick++;
			}
		} catch (e) {
			toast.error('It could not be done', { description: (e as Error).message });
		}
	}

	async function clear(which: 'authentication' | 'authorization') {
		try {
			await (which === 'authentication'
				? security.clearAuthenticationCache()
				: security.clearAuthorizationCache());
			toast.success(`Cleared the ${which} cache`);
		} catch (e) {
			toast.error(`The ${which} cache could not be cleared`, {
				description: (e as Error).message
			});
		}
	}

	function grants(r: Role): string[] {
		return PERMISSIONS.filter((p) => r[p] === true).map(permissionLabel);
	}
</script>

<svelte:head>
	<title>Security · Saranga console</title>
</svelte:head>

<div class="flex flex-col gap-4">
	<div class="flex flex-wrap items-center justify-between gap-2">
		<h1 class="text-xl font-semibold">Security</h1>
		<div class="flex gap-2">
			<Button
				size="sm"
				variant="outline"
				disabled={may.clearAuthenticationCache === false}
				onclick={() => clear('authentication')}>Clear the login cache</Button
			>
			<Button
				size="sm"
				variant="outline"
				disabled={may.clearAuthorizationCache === false}
				onclick={() => clear('authorization')}>Clear the permission cache</Button
			>
		</div>
	</div>

	<Tabs.Root value={tab} onValueChange={show}>
		<Tabs.List>
			<Tabs.Trigger value="users">Users</Tabs.Trigger>
			<Tabs.Trigger value="roles">Roles</Tabs.Trigger>
			<Tabs.Trigger value="check">Check permissions</Tabs.Trigger>
		</Tabs.List>

		<Tabs.Content value="users" class="flex flex-col gap-2 pt-2">
			<div class="flex items-center justify-between gap-2">
				<p class="text-sm text-muted-foreground">
					The users of the basic security manager, or of the properties login module's files.
				</p>
				<Button size="sm" disabled={!users || may.addUser === false} onclick={() => editUser()}
					>Add a user</Button
				>
			</div>
			{#if usersError}
				<p class="text-sm text-destructive">{usersError}</p>
			{:else if !users}
				<Skeleton class="h-24 w-full" />
			{:else}
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>User</Table.Head>
								<Table.Head>Roles</Table.Head>
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each users as u (u.username)}
								<Table.Row>
									<Table.Cell class="font-mono">{u.username}</Table.Cell>
									<Table.Cell>
										<div class="flex flex-wrap gap-1">
											{#each u.roles as r (r)}<Badge variant="secondary">{r}</Badge>{/each}
										</div>
									</Table.Cell>
									<Table.Cell class="text-right whitespace-nowrap">
										<Button
											size="xs"
											variant="outline"
											disabled={may.resetUser === false}
											onclick={() => editUser(u)}>Edit</Button
										>
										<Button
											size="xs"
											variant="outline"
											disabled={may.removeUser === false}
											onclick={() => (confirm = { kind: 'remove-user', user: u.username })}
											>Remove</Button
										>
									</Table.Cell>
								</Table.Row>
							{:else}
								<Table.Row>
									<Table.Cell colspan={3} class="text-muted-foreground">No user.</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>

		<Tabs.Content value="roles" class="flex flex-col gap-2 pt-2">
			{#if !local}
				<p class="text-sm text-muted-foreground">
					The matches are listed for the broker you are logged in to. Check another node's
					permissions on an address under Check permissions.
				</p>
			{:else if settingsError}
				<p class="text-sm text-destructive">{settingsError}</p>
			{:else if !settings}
				<Skeleton class="h-24 w-full" />
			{:else}
				<div class="flex items-center justify-between gap-2">
					<p class="text-sm text-muted-foreground">
						{settings.enabled
							? 'The most specific match of an address decides its permissions.'
							: 'Security is disabled: these are kept, but anyone may do anything.'}
					</p>
					<Button size="sm" disabled={may.addSecuritySettings === false} onclick={() => edit()}
						>Add security settings</Button
					>
				</div>
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>Match</Table.Head>
								<Table.Head>Roles</Table.Head>
								<Table.Head><span class="sr-only">Actions</span></Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each settings.matches as m (m.match)}
								<Table.Row>
									<Table.Cell class="align-top font-mono">{m.match}</Table.Cell>
									<Table.Cell>
										<ul class="flex flex-col gap-1">
											{#each m.roles as r (r.name)}
												<li>
													<span class="font-mono">{r.name}</span>:
													<span class="text-muted-foreground"
														>{grants(r).join(', ') || 'nothing'}</span
													>
												</li>
											{:else}
												<li class="text-muted-foreground">No role</li>
											{/each}
										</ul>
									</Table.Cell>
									<Table.Cell class="text-right align-top whitespace-nowrap">
										<Button
											size="xs"
											variant="outline"
											disabled={may.addSecuritySettings === false}
											onclick={() => edit(m.match, m.roles)}>Edit</Button
										>
										<Button
											size="xs"
											variant="outline"
											disabled={may.removeSecuritySettings === false}
											onclick={() => (confirm = { kind: 'remove-settings', match: m.match })}
											>Remove</Button
										>
									</Table.Cell>
								</Table.Row>
							{:else}
								<Table.Row>
									<Table.Cell colspan={3} class="text-muted-foreground"
										>No security settings.</Table.Cell
									>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>

		<Tabs.Content value="check" class="flex flex-col gap-4 pt-2">
			<form
				class="grid max-w-2xl gap-2 sm:grid-cols-[1fr_1fr_auto] sm:items-end"
				onsubmit={(e) => {
					e.preventDefault();
					check();
				}}
			>
				<div class="grid gap-1.5">
					<Label for="check-address">Address</Label>
					<Input id="check-address" required autocomplete="off" bind:value={address} />
				</div>
				<div class="grid gap-1.5">
					<Label for="check-user">User (optional)</Label>
					<Input id="check-user" autocomplete="off" list="check-users" bind:value={who} />
					<datalist id="check-users">
						{#each users ?? [] as u (u.username)}<option value={u.username}></option>{/each}
					</datalist>
				</div>
				<Button type="submit" variant="outline" disabled={!address.trim()}>Check</Button>
			</form>
			{#if checkError}<p class="text-sm text-destructive" role="alert">{checkError}</p>{/if}
			{#if checked && granted}
				<p class="text-sm text-muted-foreground">
					{#if checked.match}
						The roles of {checked.match} apply to {checked.address}.
					{:else if checked.match === null}
						No match applies to {checked.address}: nobody may do anything there.
					{/if}
					{#if checked.user}
						{checked.user.username} holds {checked.user.roles.join(', ') || 'no role'}.
					{/if}
				</p>
				<div class="overflow-x-auto rounded-md border">
					<Table.Root>
						<Table.Header>
							<Table.Row>
								<Table.Head>Permission</Table.Head>
								<Table.Head>{checked.user ? `${checked.user.username} may` : 'Granted'}</Table.Head>
								<Table.Head>By the roles</Table.Head>
							</Table.Row>
						</Table.Header>
						<Table.Body>
							{#each PERMISSIONS as p (p)}
								<Table.Row>
									<Table.Cell>{permissionLabel(p)}</Table.Cell>
									<Table.Cell>
										{#if granted[p].length}<Badge>Yes</Badge>{:else}<Badge variant="secondary"
												>No</Badge
											>{/if}
									</Table.Cell>
									<Table.Cell class="font-mono text-xs">{granted[p].join(', ') || '–'}</Table.Cell>
								</Table.Row>
							{/each}
						</Table.Body>
					</Table.Root>
				</div>
			{/if}
		</Tabs.Content>
	</Tabs.Root>
</div>

{#if editing}
	<SecuritySettingsEditor
		bind:open={editorOpen}
		match={editing.match}
		roles={editing.roles}
		onsaved={() => settingsTick++}
	/>
{/if}

<Dialog.Root bind:open={userOpen}>
	<Dialog.Content class="sm:max-w-md">
		{#if userForm}
			<Dialog.Header>
				<Dialog.Title>{userForm.existing ? `User ${userForm.user}` : 'Add a user'}</Dialog.Title>
				<Dialog.Description>
					{userForm.existing
						? 'Leave the password empty to keep it.'
						: 'The password is stored hashed unless asked otherwise.'}
				</Dialog.Description>
			</Dialog.Header>
			<form id="user" class="grid gap-4" onsubmit={saveUser}>
				<div class="grid gap-1.5">
					<Label for="user-name">User</Label>
					<Input
						id="user-name"
						required
						autocomplete="off"
						readonly={userForm.existing}
						bind:value={userForm.user}
					/>
				</div>
				<div class="grid gap-1.5">
					<Label for="user-password">Password</Label>
					<Input
						id="user-password"
						type="password"
						required={!userForm.existing}
						autocomplete="new-password"
						bind:value={userForm.password}
					/>
				</div>
				<div class="grid gap-1.5">
					<Label for="user-roles">Roles, separated by commas</Label>
					<Input id="user-roles" autocomplete="off" bind:value={userForm.roles} />
				</div>
				<Label class="font-normal">
					<Checkbox bind:checked={userForm.plaintext} /> Store the password as plain text
				</Label>
				{#if userError}<p class="text-sm text-destructive" role="alert">{userError}</p>{/if}
			</form>
			<Dialog.Footer>
				<Button variant="outline" onclick={() => (userOpen = false)}>Cancel</Button>
				<Button
					type="submit"
					form="user"
					disabled={!userForm.user.trim() || (!userForm.existing && !userForm.password)}
					>{userForm.existing ? 'Save' : 'Add'}</Button
				>
			</Dialog.Footer>
		{/if}
	</Dialog.Content>
</Dialog.Root>

<AlertDialog.Root open={confirm !== null} onOpenChange={(o) => !o && (confirm = null)}>
	<AlertDialog.Content>
		{#if confirm}
			<AlertDialog.Header>
				<AlertDialog.Title>
					{confirm.kind === 'remove-user'
						? `Remove user ${confirm.user}?`
						: `Remove the roles of ${confirm.match}?`}
				</AlertDialog.Title>
				<AlertDialog.Description>
					{confirm.kind === 'remove-user'
						? 'They can no longer log in.'
						: 'Its addresses take their permissions from the next most specific match.'}
				</AlertDialog.Description>
			</AlertDialog.Header>
			<AlertDialog.Footer>
				<AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
				<AlertDialog.Action onclick={confirmed}>Remove</AlertDialog.Action>
			</AlertDialog.Footer>
		{/if}
	</AlertDialog.Content>
</AlertDialog.Root>
