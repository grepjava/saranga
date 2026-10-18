# Saranga user guide

Saranga is a message broker written in Rust, inspired by Apache ActiveMQ Artemis 2.57.0 and built to work interchangeably with it. It takes the same `broker.xml`, speaks the same protocols, reads and writes the same data files, and answers to the same CLI. This guide covers what you need to run it. Where Saranga behaves as Artemis does, the guide says so and links [Artemis's documentation](https://activemq.apache.org/components/artemis/documentation/latest/), and spends its words on what is different. Every deliberate difference is listed in [COMPAT.md](../../COMPAT.md).

## Contents

- [Getting started](getting-started.md): build Saranga, create an instance, run it, and send and receive a message.
- [The CLI](cli.md): every `saranga` command and its `artemis` counterpart.
- [Configuration](configuration.md): the instance's files, `broker.xml`, broker properties, system properties and reloading.
- [Protocols](protocols.md): acceptors, Core, AMQP 1.0, OpenWire, STOMP, MQTT, WebSockets and TLS.
- [Security](security.md): authentication, authorisation, TLS, masked passwords, management access and the audit log.
- [Storage](storage.md): the journal, paging, large messages, disk limits, retention and the data tools.
- [High availability](ha.md): shared store, replication, lock managers, failover and failback.
- [Clustering](clustering.md): cluster connections, discovery, bridges, federation, broker connections and mirroring.
- [Management](management.md): the management API, Jolokia, the web console and the CLI's management commands.
- [Operations](operations.md): logging, the critical analyzer, network checks, plugins, upgrades and running as a service.
- [Migrating from Artemis](migration.md): moving an Artemis instance to Saranga and back, with or without downtime.
