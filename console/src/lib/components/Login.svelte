<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Card from '#lib/components/ui/card/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import Logo from './Logo.svelte';
	import { ApiError } from '#lib/api/index.js';
	import { session } from '#lib/state/session.svelte.js';

	let user = $state('');
	let password = $state('');
	let busy = $state(false);
	let error = $state<string | null>(null);

	function explain(e: unknown): string {
		if (!(e instanceof ApiError)) return String(e);
		switch (e.status) {
			case 401:
				return 'The user name or the password is wrong.';
			case 403:
				return 'This user may not use the console: it holds none of the console roles.';
			case 429:
				return 'Too many failed logins. Wait a moment, then try again.';
			default:
				return e.message;
		}
	}

	async function submit(event: SubmitEvent) {
		event.preventDefault();
		busy = true;
		error = null;
		try {
			await session.login(user, password);
		} catch (e) {
			error = explain(e);
			password = '';
		} finally {
			busy = false;
		}
	}
</script>

<main class="grid min-h-svh place-items-center bg-muted/40 p-4">
	<Card.Root class="w-full max-w-sm">
		<Card.Header class="items-center text-center">
			<Logo class="mx-auto size-10" />
			<Card.Title class="text-xl">Saranga console</Card.Title>
			<Card.Description>Log in with a user of the broker.</Card.Description>
		</Card.Header>
		<Card.Content>
			<form class="grid gap-4" onsubmit={submit}>
				{#if session.ended && !error}
					<Alert.Root>
						<Alert.Description>{session.ended}</Alert.Description>
					</Alert.Root>
				{/if}
				{#if error}
					<Alert.Root variant="destructive">
						<Alert.Description>{error}</Alert.Description>
					</Alert.Root>
				{/if}
				<div class="grid gap-2">
					<Label for="user">User</Label>
					<Input id="user" name="user" autocomplete="username" required bind:value={user} />
				</div>
				<div class="grid gap-2">
					<Label for="password">Password</Label>
					<Input
						id="password"
						name="password"
						type="password"
						autocomplete="current-password"
						bind:value={password}
					/>
				</div>
				<Button type="submit" disabled={busy || !user}>
					{busy ? 'Logging in…' : 'Log in'}
				</Button>
			</form>
		</Card.Content>
	</Card.Root>
</main>
