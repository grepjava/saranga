# Saranga console: plan and design

Status: draft, for review. This expands the 1.1 entry in [FUTURE.md](../FUTURE.md).

## Goal

Saranga gets a web console of its own. It does everything Artemis's console does, and it also manages a cluster as a whole, which Artemis's console cannot. The console is built into the `saranga` binary. It sits next to Artemis's `console.war`, which Saranga keeps serving through its Jolokia layer, and does not replace it.

## Where we start

The pieces the console builds on are already there:

- **The control logic** is in `crates/saranga-server/src/management/`. It covers the resources `broker`, `address.`, `queue.`, `divert.`, `acceptor.`, `bridge.`, `broadcastgroup.`, `brokerconnection.` and `clusterconnection.`. The console calls into it and copies none of it.
- **The web server** is Axum in `saranga-management`. It already serves `/api`, `/metrics`, `/console/jolokia` and Artemis's `console.war` (`console.rs`).
- **The security** already exists:
  - the JAAS login;
  - the console role (`hawtio.roles`);
  - `management.xml`'s `<authorisation>` (`access.rs`);
  - the `mops.` RBAC guard (`rbac.rs`);
  - the audit log.
- **Missing operations.** Some operations the console's extra features need have no implementation yet:
  - `forceFailover`, `freezeReplication` and `scaleDown`;
  - `start/stopBrokerConnection`;
  - `exportConfigAsProperties`;
  - `listPreparedTransactionDetailsAsJSON` and `listHeuristicRolledBackTransactions`;
  - `addConnector` and `removeConnector`.

  A page that needs one of these gets it implemented as Artemis implements it, in the same stage. No button is ever shown for something the broker cannot do.
- **Gaps found while building.** `closeSessionWithID` closed only Core sessions: for STOMP, AMQP, MQTT and OpenWire it answered false, while Artemis closes the server side of any session. Artemis's console can close any session, so Stage 3 implemented it for every protocol, as Artemis does: the session's consumers are disconnected and it closes as failed, then
  - AMQP ends the session (End), after detaching its consumers' links with `amqp:resource-deleted`; the connection stays;
  - STOMP sends each subscription an ERROR, which closes a 1.2 connection; a 1.0 or 1.1 connection keeps the closed session, which still sends but refuses a SUBSCRIBE (AMQ229232, under AMQ339016);
  - MQTT tells the client nothing, and disconnects it at its next packet (AMQ834015);
  - OpenWire fails the connection if the session had consumers; otherwise the closed session refuses new consumers (AMQ229232).

## What Artemis's console does (parity checklist)

Artemis's console is `artemis-console` 1.8.0: hawtio-next 4.7, React and PatternFly 5, on Jolokia. Everything below is in scope.

| Area | Artemis console |
|---|---|
| Status | Broker info, address memory, disk use, acceptors, cluster connections, lock coordinators, live and backup counts |
| Connections, Sessions, Consumers, Producers | Tables with a column chooser, field filters (contains, does not contain, equals, greater than, less than), sorting and server-side paging. Connections, sessions and consumers can be closed. |
| Addresses | Create (anycast or multicast), delete, send a message |
| Queues | Create (routing type, durable, filter, max consumers, purge on no consumers, other settings), delete, purge, browse |
| Messages | Browse (headers, properties, body, bytes display format). Delete, move, copy, retry from a DLQ, resend. Send with headers and properties, a body editor, and a choice of user. |
| Broker diagram | Topology graph: brokers, backups, addresses and queues, with internal ones optional |
| JMX tree | Every MBean's attributes, operations and charts |
| Runtime | System properties, metrics, threads |
| Preferences | Page size, diagram size, DLQ and expiry regexes, bytes format |
| Security | Login with the admin role. Buttons are enabled by `canInvoke`. |

Its known weaknesses are where we can do better:

- It shows one broker at a time. The cluster appears only as a diagram.
- Every view is a polled snapshot.
- Two security layers have to be kept in step.
- Errors are hidden (`allowErrorDetails=false`).
- There is no read-only role out of the box.

## What the Saranga console adds

1. **A cluster view.** Every node in the cluster, discovered from the topology.
   - Health per node.
   - Queues and addresses aggregated across nodes: total depth, and consumers per node.
   - Where a queue's messages and consumers are. This is where the redistribution and load-balancing settings show their effect.
   - Any page can be scoped to one node or to all of them.
   - Brokers outside a cluster can be added as saved connections.
2. **HA.** Primary and backup pairs, the replication sync state, the lock manager and shared store, and the failover history. Actions: force failover, and freeze replication.
3. **Bridges, federation, broker connections and AMQP mirrors.** Their state and their bridge metrics, with start and stop.
4. **Live data.** Server-sent events push counters, rates and depths, so tables and charts update without polling.
   - The server keeps an hour of history in a ring buffer, so a chart has data the moment it opens.
   - A feed of the notifications on `activemq.notifications`: consumers created and closed, bindings added, addresses full, and so on.
5. **A DLQ workbench.** Browse a DLQ grouped by original queue and by failure reason. Retry in bulk by filter. Export.
6. **Bulk message operations.** Act on a selection or on a filter expression: move, copy, retry, delete, expire, change priority.
   - Scheduled and delivering messages.
   - Export messages to JSON, and import them.
7. **Configuration editors.** Each one uses the operations that change the setting at runtime:
   - address settings: the effective settings for an address, and which match supplies each value;
   - security settings;
   - diverts;
   - the basic security manager's users;
   - acceptors (start, stop, reload);
   - a configuration reload.
8. **Operations.**
   - Prepared and heuristic XA transactions, with commit, roll back and forget.
   - Paging and memory per address, and the producers blocked on a full address.
   - Slow consumers and consumer lag.
   - Message counters with their history.
   - The journal and disk.
   - The critical analyzer.
   - The effective configuration, exported as properties.
9. **The resource explorer.** The equivalent of the JMX tree: every resource's attributes and operations, from `catalog.rs`, with typed parameter forms. Charts of any numeric attribute.
10. **Permissions on the page.** The console asks the active guard what the user may do, and shows each action as available or explains why it is not. Errors carry the broker's own text (`AMQ2290xx`).
11. **Usability.**
    - A command palette for jumping to any queue, address or node.
    - Every view and filter has a URL that can be shared.
    - Any table can be exported to CSV or JSON.
    - Phone-width layout, light and dark themes, and keyboard navigation.

## Architecture

```
browser ──HTTPS── saranga (the node you logged into)
  Svelte SPA        ├─ /saranga/            embedded static files
                    ├─ /saranga/api/v1/...  console API (JSON)
                    ├─ /saranga/api/v1/events  SSE stream
                    └─ peer client ──Core── other nodes' activemq.management
```

### Server side (Rust, `saranga-management`)

- **A new module, `api.rs`, mounted at `/saranga/api/v1`.** It has JSON endpoints of two kinds:
  - typed ones where a page needs a shape of its own: the session, `/me`, the six listings (`GET /connections?filter=&sort=&page=&size=`), the closes (`DELETE /connections/{id}`) and the broker's status;
  - and the resources, which serve everything else, as Jolokia serves Artemis's console: `GET /resources`, `GET /resources/{resource}` (attributes with values, operations with the guard's answer), `GET .../attributes?name=`, `PUT .../attributes/{name}` and `POST .../operations` (`{"operation", "arguments"}`). The pages call the operations Artemis's console calls, by the same signatures (`browse(int,int,java.lang.String)`, `moveMessage(long,java.lang.String)`, `createQueue(java.lang.String,boolean)`...), through typed wrappers in `console/src/lib/broker.ts`. Planned as one endpoint per action (`POST /queues/{name}/messages:move`), this way the explorer and the pages share one path, and an operation the broker gains needs no new endpoint.

  Each handler goes through the Jolokia agent (`Agent::call`, `read_attribute`, `write_attribute`, `visible`), so the guard, the parameter conversion and the audit are exactly Jolokia's. The existing `/api` stays as it is.
- **Node scoping.** `/saranga/api/v1/nodes/{nodeId}/...` sends the same request to a peer, and `nodes/*` sends it to every node and merges the answers. The local node is the default.
- **A peer client** (see decision 1). It connects to each peer's acceptor, taken from the topology, and sends management messages to the peer's `activemq.management`.
  - Management messages are Artemis's own protocol, so this works with Artemis nodes in a mixed cluster too.
  - Each node enforces its own RBAC on the request.
  - Saranga peers also answer `browse` over management messages. Artemis peers browse with `listMessagesAsJSON`.
- **Sessions.**
  - A form login against the broker's JAAS domain, with the console role required, as for Artemis's console.
  - The session cookie is `HttpOnly`, `SameSite=Strict`, and `Secure` under TLS.
  - Every request that changes something must carry a header saying the console sent it.
  - Idle sessions time out. Logins are throttled as `AuthenticationThrottler` throttles them.
  - Every action is written to the audit log under the user.
- **Capabilities.** `GET /me` gives the user, their roles, and the guard's answer for each kind of action. `POST /me/can` checks particular operations on particular resources. This plays the part of `canInvoke`.
- **Events.** `GET /events` is an SSE stream:
  - a stats tick every 2 s, carrying only the views the page subscribed to (`PUT /events/{stream}`), and of those only the ones that changed;
  - management notifications;
  - topology changes, as a `topology` view.

  The history ring buffer is sampled from the same counters `/metrics` reads (`GET /history`).
- **The API is typed.** `api_schema.rs` describes the API as OpenAPI 3.1, built from tables in the crate's own style. It uses no serde or utoipa: the crate has its own `Json` type and keeps its dependencies few. The listings' field enums come from the broker's views (`view_fields`), so they cannot drift. Each listing's row has exact types (`<Kind>Row`: mostly strings, as Artemis's views write `toString`), and the console suite checks every live row against them. The document is committed as `console/src/lib/api/openapi.json`, which a Rust test keeps current, and `openapi-typescript` generates the TypeScript client from it, so building the console needs no running broker.
- **Embedding.** `build.rs` embeds `console/build`, each file gzipped, with its ETag (no `rust-embed`: `flate2` and `sha2` are already in the workspace).
  - `pages.rs` serves `/saranga/`: gzip when the browser takes it, 304 for a current ETag, `_app/immutable/` cached for good and the rest revalidated. Its Content-Security-Policy allows the app's own scripts and the hashes of SvelteKit's inline start-up script, which `build.rs` computes.
  - A build without the built pages (a Rust-only build, with no Node) embeds a stub page that says how to build the console, and prints a warning. `SARANGA_CONSOLE_REQUIRED=1` makes that an error; the release builds and CI set it.
  - Running the console needs no Node.js.

### Client side (`console/`, latest Svelte)

- **Svelte 5 with runes** (`$state`, `$derived`, `$effect`, `$props`, snippets) and SvelteKit 3, which was released on 2026-10-01 (SvelteKit 2 was current when this was planned). The build uses `adapter-static` with hash routing, so every page is `index.html` and the broker needs no fallback, plus TypeScript 6 and Vite 8. Every version is pinned exactly: Svelte 5.57, SvelteKit 3.1, Tailwind 4.3, bits-ui 2.20, shadcn-svelte 1.7, Vitest 5 and Playwright 1.64.
- **State.** Shared state lives in `.svelte.ts` modules made of runes classes: the session, the selected node, preferences and the event stream. No legacy stores.
- **The UI kit is shadcn-svelte** (bits-ui on Tailwind CSS 4), with its components copied into the repo (see decision 2). The theme's tokens are CSS variables, for light and dark.
- **Tables.** One `DataTable` component holds:
  - server-side paging, sorting and filtering, with the filter syntax of Artemis's `list*` operations;
  - the column chooser, row selection, bulk actions and export;
  - virtual scrolling for long pages.
- **Charts** use uPlot, which is small and fast for live time series.
- **The topology graph** uses Svelte Flow (`@xyflow/svelte`).
- **The body editor** is CodeMirror 6, which is lighter than the Monaco editor Artemis uses.
- **Tests.** Vitest and Testing Library test the components. Playwright tests the console end to end against a running `saranga`.

### Pages

The URL of every page carries its node scope, for example `/saranga/#/n/all/queues?filter=...`.

| Page | Contents |
|---|---|
| Overview | Status, with a node card per node in cluster scope, plus live charts |
| Connections, Sessions, Consumers, Producers | Parity tables, with live counters |
| Addresses | Address details: bindings, diverts, settings in effect, paging, send |
| Queues | Queue details: stats, consumers, messages, settings, counters |
| Messages | Browse a queue, with bulk actions |
| DLQ workbench | |
| Cluster | Topology graph, nodes, cluster connections, redistribution |
| HA | Primary and backup pairs, replication, failover |
| Federation and connections | Bridges, federation, broker connections, mirrors |
| Security | Users, roles, security settings, a permission checker |
| Configuration | Address settings, diverts, acceptors, export, reload |
| Transactions | Prepared and heuristic XA transactions |
| Explorer | Every resource's attributes and operations |
| Events | Notification feed |
| Preferences | Page size, DLQ and expiry patterns, bytes format, theme |

## Stages

Each stage is committed on its own, with its tests.

1. **The API foundation.**
   - `console_api`, sessions, capabilities and OpenAPI.
   - The list endpoints for connections, sessions, consumers, producers, addresses and queues.
   - Tests for each endpoint and each role.
2. **The shell.**
   - The SvelteKit project, the generated client, and the embedding.
   - Login, the layout, the node picker, the theme, `DataTable`, and the command palette.
   - CI builds it.
3. **Parity pages.** Overview, the four tables, addresses, queues, messages (browse, send, move, copy, retry, resend, delete), the explorer, the diagram and preferences. After this stage the console matches Artemis's.
4. **Live data.** The SSE stream, the history buffer, the charts and the notification feed.
5. **Cluster and HA.**
   - The peer client, node scoping and aggregated views.
   - The topology and HA pages, and the federation and connections page.
   - `forceFailover`, `freezeReplication` and `start/stopBrokerConnection`, implemented as Artemis does.
   - Tested against a mixed cluster with an Artemis 2.57 node.
6. **Administration.**
   - The address settings, security, users, diverts and acceptors editors.
   - Transactions, with the XA listing operations implemented.
   - Configuration export and reload, with `exportConfigAsProperties`.
   - The DLQ workbench, bulk operations, and message export and import.
7. **Exit check.**
   - Playwright runs every page with every role (admin, read-only, none), on one node and on a cluster.
   - Then accessibility and phone-width checks.
   - The guide's management page, COMPAT.md and FUTURE.md are updated.

## Progress

- **Stage 1 (the API foundation) is done.**
  - `crates/saranga-management/src/api.rs` serves `/saranga/api/v1`, and `api_schema.rs` serves its OpenAPI document. They cover sessions, `/me` and `/me/can`, the views, the broker's status, the six listings, and the three closes.
  - Every call goes through the Jolokia agent's guard, parameter conversion and audit. The login is the Artemis console's (`console::login_user`), with its own cookie on `/saranga`.
  - `-Dsaranga.console.enabled=false` turns it off.
  - `tests/interop/console/api.sh` checks every endpoint as admin, viewer and a user without a console role: 56 checks, plus the off switch. It passes on Windows, and against the `saranga` podman jail on FreeBSD 15.1, checked from another host on the LAN.
- **Stage 2 (the shell) is done.**
  - `console/` is the SvelteKit 3 project: the login, the layout with the sidebar and the node picker (one node until Stage 5), light and dark themes, the command palette (Ctrl+K: pages, queues and addresses, the theme, logging out) and preferences.
  - `DataTable` pages, sorts and filters on the broker, with the URL holding the query. It has a column chooser (remembered), selection with bulk actions, CSV export of the page or of every matching row, and virtual scrolling past 150 rows. The six listings use it, with links between them and the closes for connections, sessions and consumers. The overview shows the broker's status, read every 5 s until Stage 4's stream.
  - The rows have exact types, and the suite checks every live row against them.
  - The broker embeds the pages (`build.rs`, `pages.rs`) and serves them at `/saranga/`. Where Artemis's `console.war` is missing, `/` goes there.
  - The tests: unit tests (Node), component tests (Chromium), and ten Playwright tests against a running broker, which `api.sh` runs after its checks. CI's `console` job runs them all, and the release builds the pages once and embeds them in every binary and image.
  - It passes on Windows (76 checks, 10 browser tests) and against the FreeBSD jail (75, with Artemis's `console.war` mounted, and 10).
- **Stage 3 (parity) is done.** The console now does what Artemis's console 1.8 does.
  - The API's resources (`GET /resources...`, `PUT` an attribute, `POST` an operation) serve the explorer and every action of the pages, through the agent's guard, conversion and audit.
  - The overview adds the acceptors, the cluster connections, the lock coordinators and the topology's live and backup counts.
  - The queue page: counters, consumers, attributes, and the messages, browsed a page at a time with a selector, each with its headers, properties and body (bytes in the format the preferences choose). They can be moved, copied, expired, sent to the dead-letter address, deleted, retried on a dead-letter or expiry queue, and resent; the queue purged, paused, disabled, its counter reset, deleted. The address page: counters, queues, producers, attributes, pausing, a queue created on it.
  - Creating addresses and queues (with every setting Artemis's console offers), and deleting them from the tables.
  - Sending a message with string properties and a CodeMirror 6 body editor (JSON and XML highlighted and formatted), as the logged-in user or another. Sending as the logged-in user needed the port of `SecurityStoreImpl`'s management branch: with no user and no password, a management send authenticates and is authorized as the caller's own subject, which the Jolokia agent now runs each call as (`management::as_subject`, Artemis's `Subject.doAs`).
  - The explorer: every resource the guard shows, by kind, its attributes (writable ones edited, numeric ones charted with uPlot every two seconds) and its operations, each with a form typed by its parameters and the guard's answer.
  - The diagram (Svelte Flow): the broker, its topology's members and backups, its addresses and their queues, internal ones optional. The runtime: the process, its system properties and its threads.
  - Preferences add the dead-letter and expiry patterns, the bytes format, the diagram's size and the internal addresses.
  - `closeSessionWithID` closes a session of every protocol (see above).
  - Found on the way: shadcn-svelte's components style `data-checked`, `data-open`, `data-active` and `data-horizontal`, which bits-ui 2 does not set; `shadcn-svelte init` imports `shadcn-svelte/tailwind.css`, whose custom variants map them to `data-state` and `data-orientation`. Stage 2 skipped `init`, so checked, open and active states were unstyled; the import is now there, and Stage 2's patch of the sidebar is undone.
  - The tests: unit tests of the body formats and the explorer's types, component tests of the operation form and the queue picker, and 18 Playwright tests, 8 of them new: sending, browsing, moving and deleting messages; purging; creating and deleting an address; the explorer; the diagram; the runtime; the preferences; and what a viewer may not do. `api.sh` adds checks of the resources as admin and viewer, of sending as oneself, and of closing a STOMP session.
  - It passes on Windows (96 checks, 18 browser tests) and against the FreeBSD jail (95, and 18).
- **Stage 4 (live data) is done.**
  - `crates/saranga-management/src/live.rs`: `GET /events`, a stream of server-sent events, one per browser tab. Its `hello` names it; `PUT /events/{stream}` sets the views it watches (the broker's status, the topology, some attributes of a resource, a page of a listing); every two seconds the broker reads each view as the user, through the guard and audit as a request is, and sends those that changed. It also brings each management notification, when the user may consume from the notification address (the same check a client subscribing makes), and `end` when the session ends; the stream does not keep its session from idling out. A browser coming back sends the last notification it saw (`Last-Event-ID`) and gets those it missed.
  - The history: the broker's counters every two seconds for an hour, and those of its addresses and queues every 30 seconds (the ones whose settings enable metrics, as `/metrics` scrapes them, the first 1,000 of each by name), one time line per kind with a series per resource and attribute. `GET /history` answers the series the guard lets the user read.
  - The notifications: the console listens to the broker's (`ManagementService.addNotificationListener`, which `saranga-server` already had for MQTT), keeps the last 1,000, and `GET /notifications` answers them.
  - The console: one `EventSource` while logged in (`state/live.svelte.ts`), whose views the pages watch for as long as they show them; the overview, the queue and address pages, the runtime, the explorer's charts, the mini-listings and the listings (with a Live switch, on by default) now follow it instead of polling. `LiveChart` draws a resource's history then each tick: the overview charts connections and consumers, messages, message rates and address memory; a queue its depth, its rates and its consumers; an address its routing rates and its size. The events page lists the notifications, by type (in the URL) and searchable, each with its properties and links to its queue and address, with pause and clear; the header shows whether the stream is live and how many notifications came since the feed was looked at.
  - The tests: unit tests of the rates, the history's merge with the live samples and the feed's descriptions, five Playwright tests (the overview, a listing and a queue page following the broker, the events page, a viewer), and 30 `api.sh` checks of the stream (its views, its notifications, its replay, its end), the history and the notifications, as admin, viewer and nobody.
  - It passes on Windows (126 checks, 23 browser tests) and against the FreeBSD jail (125, and 23).

- **Stage 5 (cluster and HA) is done.**
  - `crates/saranga-management/src/peers.rs`: the peer client. A user's link to a node is a Core connection and a session with a requestor (moved from the CLI to `saranga_server::management::requestor`, with correlation IDs), made with the user's own credentials, which the console's session keeps in memory; it closes after two minutes unused, when it fails and at logout. `GET /nodes` lists the topology; `/nodes/{node}/...` is the API again for another node, rewritten to a fresh request that carries the node; `/nodes/all/{listing}` merges every node's rows, each with its node; a live view may name a node or `all`, and the views are read together. A node that is missing, unreachable or refuses the user fails the request as a whole.
  - Another node's guard decides when asked: `/me`, `/me/can` and an operation's `allowed` answer `null` there, and the console offers the action. Its resources are named by the broker's `*Names` attributes and `listBrokerConnections`, described from the catalog; an attribute is written by its setter. Its queues are browsed with a Core browser on the user's link, since a management message's `browse` answers Java-serialized `CompositeData`; the messages are written as the queue's own `browse` writes them.
  - The broker implements `forceFailover` and `freezeReplication` as Artemis does (`start/stopBrokerConnection` already were).
  - The console: the API client sends a page's calls to its node (a request middleware, `scopedPath`), the layout makes the pages again for another node, the node picker lists the topology and **Every node**, and the sidebar offers across every node only the pages that span them. New pages: **Nodes** (each node's live status, the cluster connections, each queue across the nodes), **High availability** (policies, backups, sync, lock coordinators, force failover and freeze replication behind a confirmation) and **Bridges and connections** (broker connections, core bridges and cluster connections, started and stopped).
  - The tests: unit tests of the peers' scopes and refusals and of the client's scoping, replication tests of `forceFailover` and `freezeReplication`, and `tests/interop/console/cluster.sh`: two Saranga nodes and Artemis 2.57.0 clustered, 49 API checks (`cluster.py`) and seven Playwright tests of the cluster pages, which CI runs too.
  - It passes on Windows (49 checks and 7 browser tests across the cluster; 126 checks and 23 browser tests on one broker), and on FreeBSD 15.1 with three Saranga jails clustered on the podman bridge (49, 7) and the `saranga` jail alone (125, 23).

- **Stage 6 (administration) is done.**
  - The broker implements `exportConfigAsProperties`: the configuration it runs on exported as `artemis properties` exports one, to `broker_config_as_properties.txt` in `java.io.tmpdir`, which `artemis run` now sets to the instance's `tmp` as `bin/artemis` does (on Windows Java's own, as `artemis.cmd` leaves it). Before Stage 7 the export took in the broker properties applied at start, as Artemis's does: each entry the broker takes is applied to the export's model too (`saranga_config::configuration::apply_property`), the configuration is kept as read at start and as each reload replaces it, and three cases match Artemis 2.57's export byte for byte (`tests/golden/properties/bp-*`). What Artemis's deployment writes into the configured queues is not exported (COMPAT.md).
  - `crates/saranga-management/src/admin.rs`: what no operation gives. `GET /address-settings` lists this broker's matches (management's marked), every setting's name, type and choices, and for an address the matches that apply and the match each value comes from; `GET /security-settings` lists each match's roles and the match an address takes; `POST /configuration/export` calls the operation and answers the file it wrote. `GET /queues/{name}/export` and `POST /queues/{name}/import` move a queue's messages whole, as `saranga_server::management::export` writes them (every property typed, bodies whole, longs as text), which `browse` cannot; on another node over the user's link (a browser, a producer), so Artemis nodes take part.
  - The console's pages: **Configuration** (address settings with their editor and provenance, diverts, acceptors, export and reload), **Security** (users, each match's roles as a grid, a permission checker for a role or a user, the caches), **Transactions** (prepared XA transactions with their messages, commit and roll back, the heuristic ones; Artemis has no `forget` operation, so neither has the console), **Dead letters** (a DLQ or expiry queue grouped by original queue, original address or expiry, each group retried, moved, exported or deleted by its filter). The queue page adds actions on every message a filter selects, priority, scheduled and delivering messages, and export and import.
  - The tests: unit tests of the export format, the repository's match listing, the permission checker and the DLQ groups; 57 more `api.sh` checks (148), 6 more `cluster.py` checks (55: a queue exported from node 2 and imported into the Artemis node), 8 Playwright tests of the pages and one of them on another node.
  - It passes on Windows (148 checks and 31 browser tests on one broker; 55 and 8 across two Saranga nodes and Artemis 2.57.0), and on FreeBSD 15.1 (147 and 31 in the `saranga` jail; 55 and 8 across three Saranga jails).

## Decisions (confirmed 2026-10-22)

1. **How the console reaches other nodes.** Chosen: management messages over Core, sent with the logged-in user's own credentials, which the server holds in memory for the length of the session.
   - This works with Artemis peers, and each node enforces its own RBAC.
   - The alternatives:
     - Call each peer's web API. This needs every node's web URL, which the topology does not carry, so it would need configuration.
     - Use the cluster user. This is simpler, but every user would act with the cluster's rights.
2. **The UI kit.** Chosen: shadcn-svelte on Tailwind 4. Its components are copied into the repo rather than linked as a dependency, and it is accessible. The alternative is plain CSS with our own components: fewer dependencies, but slower to build.
3. **The URL and whether it is on by default.** Chosen: the console is served at `/saranga` on the first `<web>` binding, on by default. A system property, `saranga.console.enabled=false`, turns it off. `saranga create` still writes Artemis's redirect of `/` to `console`, so the instance's files stay Artemis's; where Artemis's `console.war` is missing, `/` goes to `/saranga/` instead.
