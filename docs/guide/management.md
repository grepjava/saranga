# Management

Saranga answers Artemis's management interface. Management messages sent to `activemq.management` reach the same resources, attributes and operations as on Artemis. The web server in `bootstrap.xml` serves Saranga's REST API, Prometheus metrics, a Jolokia layer and Artemis's own web console. The CLI's `queue`, `address`, `user` and `check` commands drive all of this. There is no JVM, so there is no JMX: Jolokia is answered from Rust, and the Artemis console works through it. This page shows how to use each interface and where it differs from Artemis.

## The interfaces at a glance

| Interface | Where | Needs |
|---|---|---|
| Management messages | the `management-address` (`activemq.management` by default), over Core or AMQP | the `manage` permission on that address |
| REST API (Saranga only) | `/api` on every `<web>` binding | a `manage` user when broker security is on |
| Prometheus metrics | the metrics app's URL, `/metrics` | `<app url="metrics" war="metrics.war"/>` in `bootstrap.xml` |
| Jolokia | `/console/jolokia` | the console's app, and a user with a console role |
| Web console | `/console` | the console's app, and a `console.war` from Artemis |
| Saranga console (Saranga only) | `/saranga/`, and its API at `/saranga/api/v1`, on every `<web>` binding | a user with a console role |
| CLI | `saranga queue`, `address`, `user`, `check` | a Core connection to an acceptor |

Every way in is audited the same way (see [operations.md](operations.md#the-audit-log)).

## Management messages

A message sent to the management address is a request, as in Artemis. The resource, attribute or operation are in the `_AMQ_ResourceName`, `_AMQ_Attribute` and `_AMQ_OperationName` properties, and the parameters are a JSON array in the body. The reply goes to the request's reply-to address with its correlation ID, and `_AMQ_OperationSucceeded` says whether it worked. The resources are `broker`, `address.<name>`, `queue.<name>`, `divert.<name>`, `acceptor.<name>`, `broadcastgroup.<name>`, `bridge.<name>`, `brokerconnection.<name>` and `clusterconnection.<name>`.

Clients that already manage Artemis this way work unchanged: `ClientRequestor` and `ManagementHelper` over Core, `JMSManagementHelper` with a `QueueRequestor`, and AMQP clients such as Qpid JMS. See Artemis's [management](https://activemq.apache.org/components/artemis/documentation/latest/management.html) chapter for the resources and operations.

The request needs the `manage` permission on the management address. With `management-message-rbac` on, each method also needs `view` or `edit` on `<management-rbac-prefix>.<resource>.<method>`, as in Artemis. Notifications go to `management-notification-address` (`activemq.notifications` by default).

Where Saranga differs:

- `getVersion` reports 2.57.0, the Artemis release whose interface Saranga answers, so version checks keep working.
- `browse` answers `no operation`. Artemis returns Java-serialized objects that only a Java client can read. Use `listMessages`, `listMessagesAsJSON`, the REST API or Jolokia instead.
- Dates of the prepared transactions and message counters are in the `en_US` locale, whatever the machine's.

The full list is in [COMPAT.md, Management differences](../../COMPAT.md#management-differences).

## The REST API

Every `<web>` binding serves a REST API at `/api`. Artemis has no such API. It is management messages over HTTP for the broker, addresses, queues and diverts: the same operations, parameter conversion and security, as plain JSON.

| Request | Does |
|---|---|
| `GET /api/broker/attributes/{attribute}` | reads a broker attribute |
| `GET /api/{addresses\|queues\|diverts}/{name}/attributes/{attribute}` | reads an address, queue or divert attribute |
| `POST /api/broker/operations/{operation}` | invokes a broker operation |
| `POST /api/{addresses\|queues\|diverts}/{name}/operations/{operation}` | invokes an operation on a resource |
| `GET /api/queues/{name}/messages` | browses a queue; takes `filter`, `page` and `pageSize` |

An operation's body is a JSON array of its parameters, `[]` for none. A value comes back as JSON. A failure comes back as `{"error": ...}` with status 401 (no login, or a wrong one), 403 (not permitted), 404 (no such resource, attribute or operation) or 400 (the operation failed).

```bash
# Read an attribute
curl -s -u admin:admin http://localhost:8161/api/queues/orders/attributes/MessageCount

# Invoke an operation: remove the messages a filter matches
curl -s -u admin:admin -X POST -H 'Content-Type: application/json' \
  --data '["priority > 4"]' \
  http://localhost:8161/api/queues/orders/operations/removeMessages

# Browse the first page of a queue
curl -s -u admin:admin 'http://localhost:8161/api/queues/orders/messages?page=1&pageSize=20'
```

With broker security on, the user given by HTTP Basic needs the `manage` permission on the management address and the `management-message-rbac` permissions, as a management message would. With security off, no login is asked for, as on the Core acceptors.

## Prometheus metrics

Declare the metrics app on a binding, as with Artemis's metrics plugin:

```xml
<web path="web">
   <binding name="artemis" uri="http://localhost:8161">
      <app name="console" url="console" war="console.war"/>
      <app name="metrics" url="metrics" war="metrics.war"/>
   </binding>
</web>
```

Saranga then serves `/metrics` in the Prometheus text format, with the names, labels and help texts the Artemis plugin exports for the broker, addresses and queues. It needs no `metrics.war` file. An address whose address settings turn off `enable-metrics` is left out.

```bash
curl -s http://localhost:8161/metrics | grep artemis_message_count
```

Where Saranga differs:

- JVM, process and Netty metrics are not exported.
- `<metrics>` and `<metrics-plugin>` in `broker.xml` are read but not acted on. Saranga logs a warning for each. The metrics app works without them.

See [COMPAT.md, Web server differences](../../COMPAT.md#web-server-differences) and Artemis's [metrics](https://activemq.apache.org/components/artemis/documentation/latest/metrics.html) chapter.

## Jolokia

Where `bootstrap.xml` declares the console's app, Saranga serves a Jolokia 2.4.3 layer at `/console/jolokia`. It answers `read`, `write`, `exec`, `search`, `list` and `version`, over GET and POST, with bulk requests, patterns and paths. The MBeans and their metadata come from Artemis's `*Control` interfaces, so object names and operations are Artemis's. Without the console's app there is no Jolokia layer.

The commands in Artemis's management chapter work as written:

```bash
curl -s -H "Origin: http://localhost" -u admin:admin \
  'http://localhost:8161/console/jolokia/read/org.apache.activemq.artemis:broker=%220.0.0.0%22/Active'

curl -s -H "Origin: http://localhost" -u admin:admin \
  'http://localhost:8161/console/jolokia/exec/org.apache.activemq.artemis:broker=%220.0.0.0%22/listConnections/%22%22/-1/-1'
```

`0.0.0.0` is the broker name `saranga create` and `artemis create` write; use your `<name>`. The `Origin` header is needed because `etc/jolokia-access.xml` lists allowed origins and turns on strict checking, as in Artemis.

Logins:

- A user needs one of the console roles. They come from `-Dhawtio.roles`, else `HAWTIO_ROLES` in `etc/artemis.profile` (`-Dhawtio.roles=` in `artemis.profile.cmd` on Windows), else `amq`.
- The user is checked against the broker's JAAS domain, by HTTP Basic or a console session.

There is no JMX connector. `management.xml`'s `<connector>` opens nothing, and tools such as JConsole cannot attach. Jolokia is the only MBean interface.

Where Saranga differs:

- Only Artemis's MBeans exist, plus hawtio's permission checks and five platform MBeans (see below). There are no other `java.lang`, `java.nio`, `JMImplementation`, `jdk.management`, `com.sun.management` or `jolokia` MBeans.
- A failed operation answers status 500 with the error type Artemis's message bundles give its AMQ code. A failure without an AMQ code answers `java.lang.Exception`. `sendMessage`, and `deleteAddress`, `createQueue`, `addAddressSettings` and `createBridge` failing with an ActiveMQ exception, answer `java.lang.IllegalStateException`, as those methods rethrow it.
- `listCache` is ignored.

See [COMPAT.md, Web server differences](../../COMPAT.md#web-server-differences).

### The platform MBeans

The console's Runtime tab reads `java.lang:type=Runtime`, `Memory`, `OperatingSystem`, `Threading` and `ClassLoading`. Saranga answers them from its own process:

- `Runtime` gives the process's start, uptime and arguments, and the system properties: those `artemis run` defines, then the `-D` options.
- `Memory`'s heap is the process's resident memory.
- `OperatingSystem` gives the machine's CPU, memory and load.
- `Threading` lists the threads by name and state on Linux, with no stacks. On other systems it lists none.
- What only a JVM has reads as zero (loaded classes) or fails as unsupported.

## Access control

`etc/management.xml`'s `<authorisation>` decides who may call what over Jolokia and the console, as Artemis's `ArtemisMBeanServerGuard` applies it: the `<allowlist>`, `<default-access>` and `<role-access>` entries, matched by domain, key and method. `saranga create` writes the same file `artemis create` writes. The file is validated as Artemis validates it (see [Schema validation](configuration.md#schema-validation)): an invalid one is reported by every command, and `saranga run` will not start with it. `${...}` in its attribute values are replaced, as in Artemis. A deprecated `<whitelist>` is applied as Artemis applies it, with Artemis's `AMQ224110` warning, and `AMQ224111` when an `<allowlist>` is there too.

Artemis can instead guard MBeans with security settings in `broker.xml`. Saranga does the same when `management.xml` has no `<authorisation>` and the system properties set the RBAC builder:

```bash
# in etc/artemis.profile, JAVA_ARGS
-Djavax.management.builder.initial=org.apache.activemq.artemis.core.server.management.ArtemisRbacMBeanServerBuilder
```

Each attribute and operation then needs `view` or `edit` on `mops.<component>.<name>.<method>` (the prefix is `management-rbac-prefix`). A refusal answers status 403 with `AMQ229032`, and `search`, pattern reads and `list` keep only the MBeans the caller may view.

The REST API and management messages do not read `management.xml`. They use the broker's security settings, as described above. See Artemis's [management](https://activemq.apache.org/components/artemis/documentation/latest/management.html) chapter for both schemes.

## The web console

Saranga serves Artemis's own web console too, besides [its own](#the-saranga-console), but does not ship it. Where `bootstrap.xml` declares the console's app (`<app url="console" war="console.war"/>`, which `create` writes), Saranga looks for a `console.war` directory in the instance's `<web path>` (`web` by default), then in the home's (`ARTEMIS_HOME`). Copy it from an Artemis 2.57.0 distribution, unpacked (built from its tag, as [Migrating](migration.md) explains):

```bash
cp -r apache-artemis-2.57.0/web/console.war /var/lib/mybroker/web/
```

At startup Saranga logs `AMQ241004: Artemis Console available at http://localhost:8161/console`. Without a `console.war` it logs a warning naming the directories it looked in. Jolokia is served either way.

The console behaves as on Artemis. That covers the login form and its session (the `JSESSIONID_<binding>_<port>` cookie), the throttling of repeated failed logins, logout and idle expiry, and hawtio's headers. Two settings are read as `-D` options, in the profile's `JAVA_ARGS` or on `saranga run`:

- `hawtio.sessionTimeout`, in seconds (1800 by default);
- `hawtio.http.strictTransportSecurity`.

`rootRedirectLocation` on `<web>` sends `/` to the console.

Where Saranga differs:

- Only Artemis's settings exist: no Keycloak, no OIDC, no Spring Security.
- The proxy is always disabled.
- Directories inside the `.war` are not listed.
- Sessions live in memory and end when the broker stops.

See [COMPAT.md, Web server differences](../../COMPAT.md#web-server-differences) and Artemis's [management console](https://activemq.apache.org/components/artemis/documentation/latest/management-console.html) chapter.

## The Saranga console

Saranga has a console of its own at `/saranga/` on every `<web>` binding, built into the binary: it needs no `console.war` and no Node.js. It is being built in stages (see [the design](../console-design.md)). So far it has:

- the login, with the same users, console roles and throttling as Artemis's console;
- the broker's overview;
- the connections, sessions, consumers, producers, addresses and queues, as tables the broker pages, sorts and filters, with a column chooser, CSV export and links between them; an admin may close connections, sessions and consumers from them;
- a command palette (Ctrl+K) that goes to any page, queue or address;
- light and dark themes, and preferences kept in the browser.

What a user sees and may do follows the guard, as below: a page or an action the guard refuses is not offered. Each page's URL holds its filters and sort (`/saranga/#/n/local/queues?filter=name:CONTAINS:orders`), so it can be bookmarked or shared. Where Artemis's `console.war` is not installed, `/` goes to `/saranga/`.

### Its API

The console's API is served at `/saranga/api/v1`, and it can be scripted.

It calls the same MBeans as Jolokia and checks them the same way. The login is the console's: a user of the broker's JAAS domain who holds a console role (`HAWTIO_ROLES`). Then `management.xml`'s `<authorisation>`, or the RBAC guard, decides each call. Failed logins are throttled as on the Artemis console. Every call is audited.

| Request | Does |
|---|---|
| `POST /session` with `{"user", "password"}` | logs in and sets the `SARANGA_SESSION_<binding>_<port>` cookie |
| `DELETE /session` | logs out |
| `GET /me` | the user, their roles, and which broker operations they may call |
| `POST /me/can` with `{"checks": [{"resource", "method"}]}` | whether they may call each method, on resources named as for management messages (`queue.orders`) |
| `GET /views` | the fields each listing filters and sorts by |
| `GET /broker` | the broker's status attributes the user may read |
| `GET /{connections\|sessions\|consumers\|producers\|addresses\|queues}` | one page of a listing; takes `filter=field:OPERATION:value` (repeatable), `sort`, `order` (`asc` or `desc`), `page` (from 1) and `size` (100 by default) |
| `DELETE /connections/{id}`, `/sessions/{connection}/{id}`, `/consumers/{session}/{id}` | closes one |
| `GET /openapi.json` | the API as OpenAPI 3.1, without a login |

A filter's `OPERATION` is one of `EQUALS`, `NOT_EQUALS`, `CONTAINS`, `NOT_CONTAINS`, `GREATER_THAN` and `LESS_THAN`, as in the `list*` operations. A request may log in with HTTP Basic instead of the session. A request that changes something, including the login, must carry the header `X-Saranga-Console: 1`, which a cross-site form cannot send. A failure comes back as `{"error", "status"}`, with status 401 (no login, or a wrong one), 403 (not permitted, or no header), 404 (nothing there), 429 (too many failed logins) or 400.

```bash
# The queues that have consumers, busiest first
curl -s -u admin:admin \
  'http://localhost:8161/saranga/api/v1/queues?filter=consumerCount:GREATER_THAN:0&sort=messageCount&order=desc'

# Close a connection
curl -s -u admin:admin -X DELETE -H 'X-Saranga-Console: 1' \
  http://localhost:8161/saranga/api/v1/connections/89ee1971
```

To turn the console off, give `-Dsaranga.console.enabled=false` in the profile's `JAVA_ARGS`. Jolokia and Artemis's console are not affected.

## The web server

`bootstrap.xml`'s `<web>` is read as Artemis reads it: its bindings, each with its URI, TLS key and trust stores and client authentication, and its apps. Every binding serves the REST API, and the apps add the metrics, Jolokia and the console. `webconfig.` system properties change bindings and apps, as with Artemis. Saranga logs `AMQ241001: HTTP Server started at <uri>` for each binding. See [configuration.md](configuration.md) and Artemis's [web server](https://activemq.apache.org/components/artemis/documentation/latest/web-server.html) chapter.

## The CLI

The management commands of the `saranga` CLI have Artemis's options and output. The Artemis CLI's own commands work against Saranga unchanged, so scripts can use either.

```bash
saranga queue stat --url tcp://localhost:61616 --user admin --password admin
saranga queue stat --queueName orders --json
saranga queue create --name orders --address orders --anycast --durable --silent
saranga queue purge --name orders
saranga address show --name orders --bindings
saranga user list
saranga check queue --name orders --up
```

From an instance, run them as `bin/artemis queue stat` (`bin\artemis.cmd` on Windows), or as `saranga shell` to keep one connection for several commands. `saranga help <command>` prints Artemis's help.

The `saranga` CLI's management commands work as Artemis's do. See [cli.md](cli.md) and [COMPAT.md, CLI differences](../../COMPAT.md#cli-differences).
