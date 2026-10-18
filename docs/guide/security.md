# Security

Saranga secures a broker the way Artemis does, from the same files: `bootstrap.xml` picks the security manager, `login.config` holds the JAAS domains, `broker.xml`'s `<security-settings>` grant permissions to roles, acceptor URLs carry the TLS settings, and `management.xml` guards the web console. An Artemis instance's security configuration works unchanged. This page shows how to do each task with Saranga and where it differs; Artemis's [security chapter](https://activemq.apache.org/components/artemis/documentation/latest/security.html) remains the reference for the settings themselves. The differences are listed in full in [COMPAT.md's security section](../../COMPAT.md#security-differences).

## Turning security on and off

Security is on unless `broker.xml` says `<security-enabled>false</security-enabled>`, as in Artemis. `saranga create` asks whether to allow anonymous logins, or takes `--allow-anonymous` or `--require-login`, and writes the same `login.config`, users, roles and security settings that `artemis create` writes:

```bash
saranga create mybroker --user admin --password secret --role amq --require-login
```

The security manager exists even with security off, so you can add users through management before you turn security on.

`saranga run --config broker.xml` reads the `login.config` and `bootstrap.xml` next to that `broker.xml`. A broker started without any configuration has no security.

## Choosing a security manager

`bootstrap.xml` names the security manager. Saranga supports the two that Artemis ships:

- **The JAAS security manager**, `<jaas-security domain="activemq"/>`. Logins go through the named entry of the instance's `etc/login.config`. An optional `certificate-domain` attribute names a second entry, used when the client presented a TLS certificate.
- **The basic security manager**, `ActiveMQBasicSecurityManager`. It keeps users and roles in the bindings journal, with no JAAS files. Its bootstrap properties (`bootstrapUser`, `bootstrapPassword` and `bootstrapRole`, or `bootstrapUserFile` and `bootstrapRoleFile`) add or update users at startup. `saranga create --security-manager basic` writes this configuration.

The basic manager's users are stored in the same records Artemis uses, so they move with the data directory in both directions. As in Artemis, the basic manager has no users without persistence.

Any other `<security-manager>` class stops the broker from starting. See [COMPAT.md](../../COMPAT.md#security-differences).

### `securityconfig.` system properties

The `securityconfig.` system properties choose the security manager at startup over what `bootstrap.xml` says, as Artemis's do: `securityconfig.jaas.domain` and `securityconfig.jaas.certificateDomain` select the JAAS manager with those domains; `securityconfig.manager.className` and `securityconfig.manager.properties.<key>` select a security manager and set its properties. Pass them with `-D` on `saranga run`, or in the profile's `JAVA_ARGS` so that `bin/artemis run` passes them:

```bash
saranga run -D securityconfig.manager.className=org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager \
  -D securityconfig.manager.properties.bootstrapUser=admin \
  -D securityconfig.manager.properties.bootstrapPassword=secret \
  -D securityconfig.manager.properties.bootstrapRole=amq
```

Artemis's own `artemis run` does not accept `-D` options (`Unknown option`), so with Artemis use `JAVA_ARGS`. The profile route works with both. See [COMPAT.md's configuration section](../../COMPAT.md#configuration-differences).

## Authentication with JAAS

Saranga reads `login.config` in the JAAS syntax Artemis uses: named entries, each a list of login modules with a control flag (`required`, `requisite`, `sufficient` or `optional`) and options. The flags and the login sequence behave as in the JDK. `${...}` placeholders in the file resolve from system properties and the environment, as in `broker.xml`. Relative file names in module options are taken from the `etc` directory, or from the module's `baseDir` option.

These login modules are supported, with Artemis's class names and options:

| Module | What it does |
|---|---|
| `PropertiesLoginModule` | Users and passwords from one properties file, roles from another. `reload=true` re-reads the files when they change. Passwords may be plain, `ENC(...)` hashes, or masked with a codec named by `org.apache.activemq.jaas.properties.password.codec`. |
| `GuestLoginModule` | Logs everyone in as one guest user and role (`org.apache.activemq.jaas.guest.user`, `.role`). With `credentialsInvalidate=true`, a client that sends a password is not treated as a guest. |
| `TextFileCertificateLoginModule` | Maps the subject DN of a TLS client certificate to a user through a users file (exact DN or `/regex/`), and users to roles through a roles file. `normalise` is not supported and is ignored with a warning. |
| `ExternalCertificateLoginModule` | Takes the certificate's subject DN as the user name; roles can come from subject alternative name URIs (`sanUriRolePrefix`). |
| `LDAPLoginModule` | Checks the password by binding to the directory and collects roles from the user's entry and from role searches, with nested groups (`expandRoles`). GSSAPI binds are not supported. |

`Krb5LoginModule`, `KubernetesLoginModule`, `PrincipalConversionLoginModule` and custom Java modules are not supported. An entry that names one gets a warning at startup, and logins that reach that module fail. Saranga also warns at startup when the domain `bootstrap.xml` names has no entry in `login.config`.

`jaasConfigs` broker properties replace the realm of the same name in `login.config`, as in Artemis. Neither `login.config` nor `jaasConfigs` is reloaded while the broker runs.

An acceptor's `securityDomain` parameter makes its clients log in through another entry, as Artemis's per-acceptor security domains do.

Authentication and authorisation results are cached as in Artemis: `authentication-cache-size`, `authorization-cache-size` and `security-invalidation-interval` in `broker.xml` apply. `populate-validated-user` and `reject-empty-validated-user` apply as well.

### Protocol notes

Every protocol logs in through the same security manager. AMQP clients use SASL: Saranga offers `PLAIN` and `ANONYMOUS` by default and accepts `EXTERNAL` with a client certificate; SCRAM-SHA-256/512, GSSAPI, OAUTHBEARER and XOAUTH2 are not supported. See [protocols.md](protocols.md) and [COMPAT.md's AMQP section](../../COMPAT.md#amqp-differences).

## Managing users

With `PropertiesLoginModule` or the basic security manager, manage users with `saranga user`, which has the same subcommands and options as `artemis user`. It works against a running broker through management, so it needs the credentials of a user allowed to manage it:

```bash
saranga user add --user admin --password secret \
  --user-command-user alice --user-command-password alicepw --role amq,consumers
saranga user list --user admin --password secret
saranga user reset --user admin --password secret --user-command-user alice --role amq
saranga user rm --user admin --password secret --user-command-user alice
```

On Windows, run the same commands as `bin\saranga.cmd user ...` from the instance, or `saranga.exe user ...`.

Passwords are stored as `ENC(...)` hashes unless you pass `--plaintext`. With the JAAS manager, the command edits the users and roles files named by the domain's `PropertiesLoginModule`, keeping their comments and order as Artemis does; with the basic manager, it changes the records in the journal. The management operations behind it (`addUser`, `listUser`, `resetUser`, `removeUser`) are also available over management messages, the REST API and Jolokia. See [management.md](management.md).

The Artemis CLI's `artemis user` works against Saranga too. Saranga's CLI connects over plain Core only: a URL with `sslEnabled=true` is refused. See [cli.md](cli.md) and [COMPAT.md's CLI section](../../COMPAT.md#cli-differences).

## Authorisation with security settings

Saranga applies `<security-settings>` as Artemis does: each `<security-setting match="...">` grants permissions to roles on the addresses its match covers, and the most specific match wins, using the broker's wildcard syntax (`#`, `*` and the configured delimiter). The permissions are Artemis's twelve: `send`, `consume`, `createAddress`, `deleteAddress`, `createDurableQueue`, `deleteDurableQueue`, `createNonDurableQueue`, `deleteNonDurableQueue`, `manage`, `browse`, `view` and `edit`. Matches on fully qualified queue names (`address::queue`) and `<role-mapping from="..." to="..."/>` work too.

```xml
<security-settings>
   <security-setting match="orders.#">
      <permission type="send" roles="producers"/>
      <permission type="consume" roles="consumers"/>
      <permission type="browse" roles="consumers,auditors"/>
   </security-setting>
</security-settings>
```

Security settings can also come from `securityRoles` broker properties, and can be added and removed at runtime through management, which keeps them in the journal. A configuration reload swaps them.

Two things to know:

- A role named `*` grants nothing, as in Artemis 2.57, even though it looks like a wildcard.
- `<security-setting-plugin>` is not supported; it produces a warning and is ignored.

See [configuration.md](configuration.md) for the configuration files and [COMPAT.md](../../COMPAT.md#security-differences) for the differences.

## TLS on acceptors

TLS is set on an acceptor's URL with Artemis's parameters, and works on any acceptor:

```xml
<acceptor name="artemis-tls">tcp://0.0.0.0:61617?sslEnabled=true;keyStorePath=${artemis.instance}/etc/broker.ks;keyStorePassword=ENC(...);protocols=CORE,AMQP,STOMP,MQTT,OPENWIRE</acceptor>
```

Saranga reads `keyStorePath`, `keyStoreType`, `keyStorePassword`, `keyPassword`, `keyStoreAlias`, `trustStorePath`, `trustStoreType`, `trustStorePassword`, `crlPath`, `needClientAuth`, `wantClientAuth`, `enabledProtocols`, `enabledCipherSuites`, `sniHost`, `sslAutoReload` and `sslHandshakeTimeout`.

- **Keystores.** `JKS` (the default, which also reads a PKCS#12 file, as Java does), `PKCS12`, and the PEM types of the `pem-keystore` provider Artemis ships: `PEM`, `PEMCA` and `PEMCFG`. Existing Artemis keystores can be used as they are.
- **Protocols and suites.** TLS 1.2 and 1.3 only. Name cipher suites by their IANA names, as Java does (`TLS_AES_128_GCM_SHA256`). Names Saranga does not support are ignored with a warning; if none is left, the acceptor fails to start.
- **Not supported.** `trustManagerFactoryPlugin`, `sslContext`, `crcOptions`, `ocspResponderURL` and `useDefaultSslContext` stop the acceptor from starting. `verifyHost` is ignored on acceptors, with a warning.
- **Reload.** With `sslAutoReload=true`, the keystore, truststore and CRL files are watched and reloaded when they change.

Artemis's [transport chapter](https://activemq.apache.org/components/artemis/documentation/latest/configuring-transports.html) describes the parameters.

### Client certificates

To authenticate clients by certificate:

1. Set `needClientAuth=true` (or `wantClientAuth=true`) and a `trustStorePath` holding the CA certificates you trust. Client authentication without trusted certificates is a configuration error.
2. Add an entry to `login.config` with `TextFileCertificateLoginModule` or `ExternalCertificateLoginModule`. A `PropertiesLoginModule` placed after a certificate module can add roles for the user the certificate identified.
3. Name that entry as the `certificate-domain` of `<jaas-security>` in `bootstrap.xml`, or as the acceptor's `securityDomain`.

```xml
<jaas-security domain="activemq" certificate-domain="CertLogin"/>
```

## Masked passwords

Passwords in the configuration can be masked as `ENC(...)`, as in Artemis. `saranga mask` produces the value with the same options and output as `artemis mask`:

```bash
saranga mask secret
saranga mask --key mykey secret
saranga mask --hash secret
```

Write the printed result as `ENC(<result>)`. Without `--hash`, the default codec's two-way algorithm is used; it prints Artemis's deprecation warning (`AMQ202017`). The key comes from `--key`, or from the `ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_KEY` environment variable, as with Artemis. `--hash` gives a one-way hash, which suits user passwords in `artemis-users.properties`.

Masked values are resolved where Artemis resolves them: `cluster-password`, bridge and broker-connection passwords, acceptor keystore and truststore passwords (honouring `mask-password`, `password-codec` and the acceptor's `activemq.usemaskedpassword` and `activemq.passwordcodec`), broker properties, `bootstrap.xml`'s web bindings (`passwordCodec`), the LDAP module's `connectionPassword`, and the users file of `PropertiesLoginModule`.

Only Artemis's default codec, `org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec`, is available: a `password-codec` that names another class is an error. See Artemis's [masking chapter](https://activemq.apache.org/components/artemis/documentation/latest/masking-passwords.html).

## Management access control

Management messages are authorised by the `manage` permission on the management address, as in Artemis. With `management-message-rbac` on, each operation also needs `view` or `edit` (by `view-permission-method-match-pattern`) on `<management-rbac-prefix>.<resource>.<method>`. Saranga's REST API at `/api` applies the same checks to the user named by HTTP Basic, and asks for no login when broker security is off. See [management.md](management.md).

The web console's Jolokia layer is guarded as Artemis's MBeans are:

- **`management.xml`'s `<authorisation>`**, with its `allowlist`, `default-access` and `role-access`, as Artemis's guard applies them. Without `management.xml`, Artemis's default list applies. Saranga has no JMX, so `management.xml`'s JMX connector settings do not apply.
- **The RBAC guard.** Without an `<authorisation>`, and with `-Djavax.management.builder.initial=org.apache.activemq.artemis.core.server.management.ArtemisRbacMBeanServerBuilder` in the profile's `JAVA_ARGS`, the security settings on `mops.` addresses guard the MBeans as Artemis's RBAC handler does. A refusal answers status 403.

See Artemis's [management chapter](https://activemq.apache.org/components/artemis/documentation/latest/management.html) and [COMPAT.md's web server section](../../COMPAT.md#web-server-differences).

## The web console's login

Where `bootstrap.xml` declares the console app, Saranga serves Artemis's own `console.war` from the instance's `web` directory or the home's, with its login form. Users log in through the broker's security manager and must hold one of the console roles: `-Dhawtio.roles`, `HAWTIO_ROLES` in `artemis.profile`, or `-Dhawtio.roles=` in `artemis.profile.cmd` on Windows; the default is `amq`. Repeated failed logins are throttled, and idle sessions end after `hawtio.sessionTimeout` (1800 seconds by default). Jolokia also takes HTTP Basic credentials for a user who holds a console role.

Only Artemis's settings exist: there is no Keycloak, OIDC or Spring Security, and the proxy is always disabled. Sessions are held in memory and end with the broker.

TLS on the web server is set on the `<binding>` in `bootstrap.xml`, with `keyStorePath`, `keyStorePassword`, `trustStorePath`, `clientAuth` and the other attributes Artemis uses; `saranga create --ssl-key`, `--ssl-key-password`, `--use-client-auth`, `--ssl-trust` and `--ssl-trust-password` write them. See [management.md](management.md) and Artemis's [web server chapter](https://activemq.apache.org/components/artemis/documentation/latest/web-server.html).

## The audit log

The three audit loggers, `org.apache.activemq.audit.base`, `.resource` and `.message`, are off until `log4j2.properties` turns them on, as in Artemis. Set their levels to `INFO` in the instance's `etc/log4j2.properties`; the instance's file writes them to `log/audit.log`, and the change is picked up while the broker runs.

```properties
logger.audit_base.level = INFO
logger.audit_resource.level = INFO
logger.audit_message.level = INFO
```

The lines carry Artemis's codes, texts and callers (`user(roles)@address`). Management calls are audited whichever way they arrive: management messages, the REST API or Jolokia. Objects in messages show Saranga's values, some events Artemis logs are not logged (MBean registration and the JMX guard among them), and security failures are logged without a stack trace. See [operations.md](operations.md) for logging and [COMPAT.md's audit log section](../../COMPAT.md#audit-log-differences) for the full list.

## Security plugins

`<broker-plugins>` can select Saranga's port of `BrokerMessageAuthorizationPlugin`, by its Java class name and with its properties, as in Artemis. A message that carries a required role (the plugin's `ROLE_PROPERTY`) goes only to consumers whose user holds that role. A `<broker-plugin>` naming any class Saranga does not port stops the broker from starting, so that what the plugin enforces is never silently dropped. Embedding applications can write plugins in Rust against `saranga_server::plugin::BrokerPlugin`. See Artemis's [broker plugins chapter](https://activemq.apache.org/components/artemis/documentation/latest/broker-plugins.html) and [COMPAT.md's broker plugin section](../../COMPAT.md#broker-plugin-differences).

## Clustering credentials

Cluster connections log in with `cluster-user` and `cluster-password` from `broker.xml`, as in Artemis; the password can be masked. See [clustering.md](clustering.md).
