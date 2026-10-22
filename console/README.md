# The Saranga console

Saranga's own web console: a single-page app in Svelte 5 and SvelteKit 3,
with shadcn-svelte on Tailwind CSS 4. The broker embeds the built pages
(`build/`) and serves them at `/saranga/`, with their API at
`/saranga/api/v1`. The design and the plan are in
[docs/console-design.md](../docs/console-design.md).

Running the console needs no Node.js; building it does (Node 24).

```sh
npm ci
npm run build          # build/, which the next cargo build embeds
cargo build --release -p saranga-cli   # from the repository's root
```

A Rust build without `build/` embeds a page that says how to build the
console, and warns. `SARANGA_CONSOLE_REQUIRED=1` makes it fail instead;
the release builds and CI set it.

## Working on it

```sh
SARANGA_URL=http://localhost:8161 npm run dev
```

serves the pages with hot reload at `http://localhost:5173/saranga/` and
sends the API to the broker at `SARANGA_URL`.

| Command                           |                                                           |
| --------------------------------- | --------------------------------------------------------- |
| `npm run check`                   | Type-checks everything                                    |
| `npm run lint` / `npm run format` | Checks / applies the formatting                           |
| `npm test`                        | Unit tests (Node) and component tests (Chromium)          |
| `npm run test:e2e`                | Playwright against a running broker at `SARANGA_URL`      |
| `npm run api`                     | Regenerates `src/lib/api/schema.d.ts` from `openapi.json` |

`src/lib/api/openapi.json` is the broker's own description of the API.
The broker's tests check that it is current; after changing the API, run
`SARANGA_UPDATE_OPENAPI=1 cargo test -p saranga-management the_console_has_this_document`
and then `npm run api`.

`tests/interop/console/api.sh` checks the API and the pages against a
fresh instance, and runs the Playwright tests too when `node_modules` is
there.

## Layout

- `src/routes/`: the pages. Routes are in the URL's fragment
  (`#/n/local/queues?filter=name:CONTAINS:orders`), and every page's URL
  names its node.
- `src/lib/api/`: the typed client.
- `src/lib/state/`: the session, the nodes and the preferences, as runes
  classes.
- `src/lib/components/`: the shell and `DataTable`; `ui/` holds the
  shadcn-svelte components as the registry wrote them, and
  `npx shadcn-svelte@1.7.0 add <name>` adds more.
- `e2e/`: the Playwright tests.
