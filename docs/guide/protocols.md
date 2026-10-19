# Protocols

Saranga speaks the protocols Artemis speaks: Core, AMQP 1.0, OpenWire, STOMP 1.0 to 1.2, and MQTT 3.1, 3.1.1 and 5, over plain TCP, TLS and WebSockets. Acceptors are configured in `broker.xml` exactly as for Artemis, and one port can serve every protocol, because the broker detects each connection's protocol from its first bytes. Messages convert between protocols as Artemis converts them. This page explains how to set up acceptors and TLS, which ports `saranga create` opens, and where each protocol differs from Artemis; the details are in [COMPAT.md](../../COMPAT.md).

## Acceptors

An acceptor is an `<acceptor>` element in `broker.xml` with a URL, as in Artemis:

```xml
<acceptors>
   <acceptor name="artemis">tcp://0.0.0.0:61616?protocols=CORE,AMQP,STOMP,MQTT,OPENWIRE</acceptor>
   <acceptor name="amqp">tcp://0.0.0.0:5672?protocols=AMQP;amqpCredits=1000;amqpLowCredits=300</acceptor>
</acceptors>
```

The URL syntax, the `protocols` parameter and the per-protocol parameters are Artemis's. See [Artemis's transport configuration](https://activemq.apache.org/components/artemis/documentation/latest/configuring-transports.html).

How Saranga treats an acceptor:

- **Protocol detection.** Each connection's protocol is picked from its first 8 bytes, among the protocols the acceptor lists, as Artemis's `ProtocolHandler` picks it. An acceptor without `protocols` serves all of them.
- **Protocols.** Saranga serves all six of Artemis's: `CORE`, `AMQP`, `STOMP`, `HORNETQ`, `MQTT` and `OPENWIRE`. A name that is no protocol at all gets Artemis's `AMQ222203` warning and is ignored.
- **Host names.** A host binds to the first address the name resolves to, IPv4 first, as Java's `InetAddress.getByName` does. So `localhost` means 127.0.0.1.
- **Socket options.** `tcpNoDelay`, `tcpSendBufferSize` and `tcpReceiveBufferSize` are honoured. Netty's own options, such as `useEpoll`, are accepted and have no effect.
- **Address prefixes.** `anycastPrefix` and `multicastPrefix` work on every protocol, as in Artemis.
- **Handshake timeout.** `handshake-timeout` (10 seconds by default) closes a connection that does not send enough bytes to be recognised.
- **Not supported.** The PROXY protocol, and Core over HTTP (`httpEnabled`). Connection routers and protocol interceptors are not supported either.

The startup log names each acceptor and the protocols it serves, with Artemis's message:

    AMQ221020: Started NIO Acceptor at 0.0.0.0:61616 for protocols [CORE,HORNETQ,STOMP,MQTT,AMQP,OPENWIRE]

Acceptors can be stopped, started and reloaded through management, as in Artemis. A configuration reload does not change them; restart the broker. See [Configuration](configuration.md#configuration-reload).

## The ports `saranga create` sets up

`saranga create` writes the acceptors `artemis create` writes:

| Acceptor | Port | Protocols |
|---|---|---|
| `artemis` | 61616 | CORE, AMQP, STOMP, HORNETQ, MQTT, OPENWIRE |
| `amqp` | 5672 | AMQP |
| `stomp` | 61613 | STOMP |
| `hornetq` | 5445 | HORNETQ, STOMP |
| `mqtt` | 1883 | MQTT |

The web server listens on 8161. `--default-port` changes the port of the `artemis` acceptor, `--http-port` the web server's, and `--port-offset` adds a number to every port. `--no-amqp-acceptor`, `--no-stomp-acceptor`, `--no-hornetq-acceptor` and `--no-mqtt-acceptor` leave out the single-protocol acceptors.

```bash
saranga create /var/lib/broker2 --port-offset 100 --no-hornetq-acceptor
```

See [The CLI](cli.md) for the other options.

## TLS

TLS on an acceptor is configured with Artemis's parameters. Saranga uses rustls, not Java's TLS. Each connection does the TLS handshake first, then a WebSocket upgrade if it asks for one, then protocol detection, so one TLS port can serve every protocol.

```xml
<acceptor name="tls">tcp://0.0.0.0:61617?sslEnabled=true;keyStorePath=${artemis.instance}/etc/broker.p12;keyStorePassword=ENC(...);keyStoreType=PKCS12;protocols=CORE,AMQP,STOMP,MQTT,OPENWIRE</acceptor>
```

What is supported:

- **Keystores and truststores:** `JKS`, `PKCS12`, and the PEM types of the `pem-keystore` provider that Artemis ships: `PEM`, `PEMCA` and `PEMCFG`. `JKS` is the default type, and a PKCS#12 file given as `JKS` is read too, as Java reads it. `keyStoreAlias` picks the key. A path can be a file path or a `file:` URL.
- **Passwords** can be masked (`ENC(...)`), with the broker's `mask-password` and `password-codec` or the acceptor's own `activemq.usemaskedpassword` and `activemq.passwordcodec`. See [Security](security.md).
- **Client certificates:** `needClientAuth` and `wantClientAuth`, with a truststore, and a certificate revocation list (`crlPath`).
- **SNI:** `sniHost`, a regular expression matched against the host name a client asks for. A client that asks for no name is let in, as in Java.
- **Protocols and ciphers:** `enabledProtocols` takes `TLSv1.2` and `TLSv1.3`; `enabledCipherSuites` takes the standard names of the suites rustls has (for example `TLS_AES_128_GCM_SHA256` or `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`). A name Saranga does not have is logged and skipped. If none is left, the acceptor fails.
- **Reloading the stores:** `sslAutoReload=true`, or management's acceptor `reload`.
- **Handshake timeout:** `sslHandshakeTimeout`, in seconds (10 by default).

What is not supported:

- `trustManagerFactoryPlugin`, `sslContext`, `crcOptions`, `ocspResponderURL` and `useDefaultSslContext`. An acceptor that sets one of them fails, and the broker does not start.
- `verifyHost` on an acceptor is logged and ignored.

Logins with client certificates are described in [Security](security.md). See also [Artemis's TLS configuration](https://activemq.apache.org/components/artemis/documentation/latest/configuring-transports.html).

## WebSockets

Every acceptor answers WebSocket clients on the same port, as Artemis's does: an HTTP upgrade request is answered with the WebSocket handshake, and the protocol is then detected again inside the WebSocket. The subprotocols offered are those of the acceptor's protocols: `v10.stomp`, `v11.stomp`, `v12.stomp`, `mqtt`, `mqttv3.1` and `amqp`.

Clients connect as they do to Artemis, for example `ws://broker:61616` for Paho, and `amqpws://broker:61616` for Qpid JMS. The parameters are Artemis's:

- `webSocketMaxFramePayloadLength` (65536 by default). A larger frame from a client closes the connection with status 1009. The deprecated `stompMaxFramePayloadLength` is still read, with Artemis's `AMQ221038` message.
- `webSocketEncoderType`: `binary` (the default) or `text`.

Where Saranga differs:

- One broker write is one WebSocket message, which can hold several protocol frames. Clients read the payloads as a stream, so this does not affect them.
- A request that is not a GET, or an HTTP request without an upgrade, gets `403 Forbidden`.
- Compression (`webSocketCompressionSupported`) and the old draft-00 handshake are not supported.

See [COMPAT.md](../../COMPAT.md#websocket-differences).

## Core

Core is Artemis's own protocol, used by the Artemis JMS client and by the CLI. Saranga puts on the wire what Artemis puts there, quirks included, so Artemis's 2.x clients work unchanged. For 1.x clients, add `anycastPrefix=jms.queue.;multicastPrefix=jms.topic.` to the acceptor, as Artemis's `broker.xml` comment says; Saranga then puts the old prefixes back on query results and delivered messages. See [Artemis's Core documentation](https://activemq.apache.org/components/artemis/documentation/latest/core.html).

## HornetQ

HornetQ 2.x clients connect to the `hornetq` acceptor on 5445, as they do to Artemis. Its `anycastPrefix=jms.queue.;multicastPrefix=jms.topic.` maps their queue and topic names. The `HORNETQ` protocol is Core with HornetQ's handshake, which a client may leave out, and with HornetQ's names converted as Artemis's interceptors convert them:

- **Properties.** A message a HornetQ client sends has its `_HQ_` properties renamed to Artemis's `_AMQ_` ones, and a message it receives has them renamed back. These are the properties for duplicate detection, scheduled delivery, groups, last-value queues, large messages, expiry and a message's origin.
- **Filters.** `HQPriority`, `HQDurable`, `HQTimestamp`, `HQUserID` and `HQExpiration` in a consumer's or a queue's filter become Artemis's `AMQ` identifiers.

A HornetQ client uses only queues and topics that exist, so create them in `broker.xml` or through management. As in Artemis, a message delivered to a HornetQ client carries the old prefix on its address, but Saranga leaves the queued message as it is; see [COMPAT.md](../../COMPAT.md#artemis-wire-quirks-saranga-reproduces-on-purpose).

## AMQP 1.0

AMQP clients connect as they do to Artemis: SASL PLAIN, ANONYMOUS and EXTERNAL, links and their credit, anonymous relay, dynamic and FQQN sources and targets, durable and shared subscriptions named as Artemis names them, selectors, every outcome, and transactions. AMQP messages are kept in their own encoding in queues, the journal and pages, as Artemis keeps them, so either broker reads the other's data. The acceptor parameters are Artemis's, for example `amqpCredits`, `amqpLowCredits`, `amqpMinLargeMessageSize`, `amqpDuplicateDetection`, `amqpIdleTimeout` and `saslMechanisms`. A full address holds producers back as on Artemis: under `BLOCK` a link gets no more credit until there is room, past `max-size-bytes-reject-threshold` sends are rejected, and a send refused because the address is full drains the link's credit (`amqpDrainOnTransientDeliveryErrors`, `amqpLinkQuiesceTimeout`, `amqpUseModifiedForTransientDeliveryErrors`). See [Artemis's AMQP documentation](https://activemq.apache.org/components/artemis/documentation/latest/amqp.html).

The main differences:

- The SASL mechanisms SCRAM-SHA-256/512, GSSAPI, OAUTHBEARER and XOAUTH2 are not supported. A client that asks for one gets outcome `sys`.
- Core messages always go out converted to AMQP. Core-message tunnelling is not offered.
- An AMQP large message is held whole in memory while the broker holds it.
- The Open frame's `product` is `saranga`.

Some Artemis errors are not copied: for example, a failed link attach refuses only that link instead of closing the connection. See [COMPAT.md](../../COMPAT.md#amqp-differences). AMQP broker connections, federation and mirroring are covered in [Clustering](clustering.md).

## OpenWire

ActiveMQ 5 clients connect as they do to Artemis: every wire-format version, tight and loose encoding, the prefetch model and `MessagePull`, every acknowledgement type, local and XA transactions, durable subscriptions, browsers, virtual topics and advisories. The acceptor parameters are Artemis's, for example `supportAdvisory`, `suppressInternalManagementObjects` and `virtualTopicConsumerWildcards`. A full address holds producers back as on Artemis: under `BLOCK` a send that waits for an answer, or one with a producer window, is answered once there is room, and the connection stops being read after an asynchronous send; under `FAIL` the send gets `ResourceAllocationException`. See [Artemis's OpenWire documentation](https://activemq.apache.org/components/artemis/documentation/latest/openwire.html).

The main differences:

- `brokerName` defaults to the host name from `HOSTNAME`, `COMPUTERNAME` or `/etc/hostname`, then to the node ID.
- A queue browser gets a snapshot of the queue when it is created.
- The slow-consumer advisory, `ConsumerControl` for prefetch 0, cluster client updates, and the `openwireMaxPacketChunkSize` back-pressure are not there.

See [COMPAT.md](../../COMPAT.md#openwire-differences).

## STOMP

STOMP 1.0, 1.1 and 1.2 work as in Artemis: heart-beating, the `auto`, `client` and `client-individual` acknowledgement modes, NACK, transactions, receipts, durable and no-local subscriptions, selectors and large messages. The acceptor parameters are Artemis's, for example `stompEnableMessageId`, `stompMinLargeMessageSize`, `stompConsumerWindowSize` and `disableStompServerHeader`. See [Artemis's STOMP documentation](https://activemq.apache.org/components/artemis/documentation/latest/stomp.html).

The main differences:

- The `server` header of CONNECTED is `Saranga/<version>`.
- Frame interceptors are not supported.
- The connection TTL closes a silent connection as soon as it runs out, rather than at the next check.
- STOMP connections have no flow control, so while the disk is full a send fails the connection with `AMQ229119`, as in Artemis.

See [COMPAT.md](../../COMPAT.md#stomp-differences).

## MQTT

MQTT 3.1, 3.1.1 and 5 work as in Artemis: persistent sessions, retained messages, wills, shared subscriptions, QoS 0, 1 and 2, and the MQTT 5 properties. Session state is kept where Artemis keeps it (`$sys.mqtt.sessions` and the journal), so sessions survive a move between the brokers. The acceptor parameters are Artemis's, for example `allowLinkStealing`, `defaultMqttSessionExpiryInterval`, `receiveMaximum`, `topicAliasMaximum`, `serverKeepAlive` and `maximumPacketSize`. See [Artemis's MQTT documentation](https://activemq.apache.org/components/artemis/documentation/latest/mqtt.html).

The main differences:

- Two Artemis bugs are not copied: retained messages reach MQTT 5 wildcard subscriptions, and a clean start deletes the client's packet-ID records.
- A second CONNECT on a connection closes it.
- The connection TTL closes a silent connection as soon as it runs out.
- Interceptors and connection routers are not supported.

See [COMPAT.md](../../COMPAT.md#mqtt-differences).

## Conversion between protocols

A message sent with one protocol can be received with any other, as in Artemis, and Saranga converts it the way Artemis does: a JMS receiver gets a text or a bytes message as it would from Artemis. MQTT user properties reach only MQTT receivers, as in Artemis. See [Artemis's protocol interoperability](https://activemq.apache.org/components/artemis/documentation/latest/protocols-interoperability.html).

Conversion from AMQP to Core differs in two cases: an `amqp-value` body of an unusual type becomes a bytes message holding the encoded section, and an application property that Core has no type for is left out instead of failing the conversion. See [COMPAT.md](../../COMPAT.md#amqp-differences).

## Tested clients

Each of these clients runs the same test suite against Artemis 2.57.0 and against Saranga, and passes on both:

| Protocol | Clients |
|---|---|
| Core | the Artemis JMS client; the Artemis 1.5.6 JMS client, with the 1.x prefixes |
| AMQP 1.0 | Qpid JMS; Qpid Proton's Python binding (`python-qpid-proton` 0.40.0); AMQPNetLite 2.5.4 (.NET); fe2o3-amqp 0.18.2 (Rust) |
| OpenWire | the ActiveMQ 5.19.10 client |
| STOMP | raw sockets, and the JDK's WebSocket client |
| MQTT | Eclipse Paho 1.2.5, for 3.1.1 and 5 |
| WebSockets | Paho (`ws://`), Qpid JMS (`amqpws://`), STOMP and OpenWire |

A conversion suite sends from each of Core JMS, Qpid JMS, the ActiveMQ 5 client, STOMP and MQTT 5 to each of them, 25 pairs in all. The suites are in [tests/interop](https://github.com/grepjava/saranga/blob/main/tests/interop), and Phase 7 in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md#phase-7-protocols) lists what each covers.
