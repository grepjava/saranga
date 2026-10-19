//! `broker.xml` validated against Artemis's schemas: each document as
//! Artemis 2.57's `XMLUtil.validate` reports it, the messages recorded
//! from it.

use saranga_config::{BrokerConfig, ConfigError, Validation};

fn validate(core: &str) -> Option<String> {
    let xml = format!(
        r#"<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><core xmlns="urn:activemq:core">{core}</core></configuration>"#
    );
    match BrokerConfig::validate(&xml, |_| None, Validation::Deploy { jms: true }) {
        Ok(()) => None,
        Err(ConfigError::Invalid(m)) => Some(m),
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn messages_are_artemis_s() {
    let cases: &[(&str, Option<&str>)] = &[
        (
            r#"<cluster-connections><cluster-connection name="c"></cluster-connection></cluster-connections>"#,
            Some(
                r#"cvc-complex-type.2.4.b: The content of element 'cluster-connection' is not complete. One of '{"urn:activemq:core":address, "urn:activemq:core":connector-ref, "urn:activemq:core":check-period, "urn:activemq:core":connection-ttl, "urn:activemq:core":min-large-message-size, "urn:activemq:core":call-timeout, "urn:activemq:core":retry-interval, "urn:activemq:core":retry-interval-multiplier, "urn:activemq:core":max-retry-interval, "urn:activemq:core":initial-connect-attempts, "urn:activemq:core":reconnect-attempts, "urn:activemq:core":use-duplicate-detection, "urn:activemq:core":forward-when-no-consumers, "urn:activemq:core":message-load-balancing, "urn:activemq:core":max-hops, "urn:activemq:core":confirmation-window-size, "urn:activemq:core":producer-window-size, "urn:activemq:core":call-failover-timeout, "urn:activemq:core":notification-interval, "urn:activemq:core":notification-attempts, "urn:activemq:core":scale-down-connector, "urn:activemq:core":client-id, "urn:activemq:core":topology-scanner-attempts, "urn:activemq:core":discovery-type}' is expected."#,
            ),
        ),
        (
            r##"<security-settings><security-setting match="#"><permission type="send" roles="a"><x/></permission></security-setting></security-settings>"##,
            Some(
                r#"cvc-complex-type.2.1: Element 'permission' must have no character or element information item [children], because the type's content type is empty."#,
            ),
        ),
        (
            r#"<acceptors><acceptor name="a">tcp://h:1<x/></acceptor></acceptors>"#,
            Some(
                r#"cvc-complex-type.2.2: Element 'acceptor' must have no element [children], and the value must be valid."#,
            ),
        ),
        (
            r#"<acceptors>text</acceptors>"#,
            Some(
                r#"cvc-complex-type.2.3: Element 'acceptors' cannot have character [children], because the type's content type is element-only."#,
            ),
        ),
        (
            r#"<name foo="bar">b</name>"#,
            Some(
                r#"cvc-type.3.1.1: Element 'name' is a simple type, so it cannot have attributes, excepting those whose namespace name is identical to 'http://www.w3.org/2001/XMLSchema-instance' and whose [local name] is one of 'type', 'nil', 'schemaLocation' or 'noNamespaceSchemaLocation'. However, the attribute, 'foo' was found."#,
            ),
        ),
        (
            r#"<name><x/></name>"#,
            Some(
                r#"cvc-type.3.1.2: Element 'name' is a simple type, so it must have no element information item [children]."#,
            ),
        ),
        (
            r#"<security-settings><security-setting><permission type="send" roles="a"/></security-setting></security-settings>"#,
            Some(
                r#"cvc-complex-type.4: Attribute 'match' must appear on element 'security-setting'."#,
            ),
        ),
        (
            r#"<name xsi:nil="true">b</name>"#,
            Some(
                r#"cvc-elt.3.1: Attribute 'http://www.w3.org/2001/XMLSchema-instance,nil' must not appear on element 'name', because the {nillable} property of 'name' is false."#,
            ),
        ),
        (
            r#"<name xsi:type="nothing">b</name>"#,
            Some(
                r#"cvc-elt.4.2: Cannot resolve 'nothing' to a type definition for element 'name'."#,
            ),
        ),
        (
            r#"<name xsi:type="q:x">b</name>"#,
            Some(
                r#"UndeclaredPrefix: Cannot resolve 'q:x' as a QName: the prefix 'q' is not declared."#,
            ),
        ),
        (
            r##"<address-settings><address-setting match="#"><address-full-policy>page</address-full-policy></address-setting></address-settings>"##,
            Some(
                r#"cvc-enumeration-valid: Value 'page' is not facet-valid with respect to enumeration '[DROP, FAIL, PAGE, BLOCK]'. It must be a value from the enumeration."#,
            ),
        ),
        (
            r#"<id-cache-size>2147483648</id-cache-size>"#,
            Some(
                r#"cvc-maxInclusive-valid: Value '2147483648' is not facet-valid with respect to maxInclusive '2147483647' for type 'int'."#,
            ),
        ),
        (
            r#"<journal-file-size>-9223372036854775809</journal-file-size>"#,
            None,
        ),
        (
            r#"<discovery-groups><discovery-group name="g"><group-address>a</group-address><group-port>1</group-port></discovery-group><discovery-group name="g"><group-address>a</group-address><group-port>1</group-port></discovery-group></discovery-groups>"#,
            Some(r#"cvc-id.2: There are multiple occurrences of ID value 'g'."#),
        ),
        (
            r#"<addresses xml:space="bogus"/>"#,
            Some(
                r#"cvc-enumeration-valid: Value 'bogus' is not facet-valid with respect to enumeration '[default, preserve]'. It must be a value from the enumeration."#,
            ),
        ),
        (
            r#"<addresses xml:lang="not a lang"/>"#,
            Some(
                r#"cvc-datatype-valid.1.2.3: 'not a lang' is not a valid value of union type '#AnonType_lang'."#,
            ),
        ),
        (
            r#"<persistence-enabled>TRUE</persistence-enabled>"#,
            Some(r#"cvc-datatype-valid.1.2.1: 'TRUE' is not a valid value for 'boolean'."#),
        ),
        (r#"<persistence-enabled></persistence-enabled>"#, None),
        (
            r#"<cluster-connections><cluster-connection name="c"><connector-ref>x</connector-ref><discovery-type/></cluster-connection></cluster-connections>"#,
            Some(
                r#"cvc-elt.2: The value of {abstract} in the element declaration for 'discovery-type' must be false."#,
            ),
        ),
        (
            r#"<x:name xmlns:x="urn:other">b</x:name>"#,
            Some(
                r#"cvc-complex-type.2.4.a: Invalid content was found starting with element '{"urn:other":name}'. One of '{"urn:activemq:core":name, "urn:activemq:core":system-property-prefix, "urn:activemq:core":internal-naming-prefix, "urn:activemq:core":amqp-use-core-subscription-naming, "urn:activemq:core":resolve-protocols, "urn:activemq:core":journal-datasync, "urn:activemq:core":large-message-sync, "urn:activemq:core":persistence-enabled, "urn:activemq:core":scheduled-thread-pool-max-size, "urn:activemq:core":thread-pool-max-size, "urn:activemq:core":graceful-shutdown-enabled, "urn:activemq:core":graceful-shutdown-timeout, "urn:activemq:core":security-enabled, "urn:activemq:core":security-invalidation-interval, "urn:activemq:core":authentication-cache-size, "urn:activemq:core":authorization-cache-size, "urn:activemq:core":journal-lock-acquisition-timeout, "urn:activemq:core":journal-lock-monitor-timeout, "urn:activemq:core":journal-lock-monitor-max-retries, "urn:activemq:core":wild-card-routing-enabled, "urn:activemq:core":management-address, "urn:activemq:core":management-notification-address, "urn:activemq:core":cluster-user, "urn:activemq:core":cluster-password, "urn:activemq:core":password-codec, "urn:activemq:core":mask-password, "urn:activemq:core":log-delegate-factory-class-name, "urn:activemq:core":jmx-management-enabled, "urn:activemq:core":jmx-notification-enabled, "urn:activemq:core":jmx-domain, "urn:activemq:core":jmx-use-broker-name, "urn:activemq:core":message-counter-enabled, "urn:activemq:core":message-counter-sample-period, "urn:activemq:core":message-counter-max-day-history, "urn:activemq:core":connection-ttl-override, "urn:activemq:core":connection-ttl-check-interval, "urn:activemq:core":configuration-file-refresh-period, "urn:activemq:core":temporary-queue-namespace, "urn:activemq:core":uuid-namespace, "urn:activemq:core":async-connection-execution-enabled, "urn:activemq:core":transaction-timeout, "urn:activemq:core":transaction-timeout-scan-period, "urn:activemq:core":message-expiry-scan-period, "urn:activemq:core":message-expiry-thread-priority, "urn:activemq:core":address-queue-scan-period, "urn:activemq:core":id-cache-size, "urn:activemq:core":persist-id-cache, "urn:activemq:core":remoting-incoming-interceptors, "urn:activemq:core":remoting-outgoing-interceptors, "urn:activemq:core":persist-delivery-count-before-delivery, "urn:activemq:core":max-redelivery-records, "urn:activemq:core":populate-validated-user, "urn:activemq:core":reject-empty-validated-user, "urn:activemq:core":mqtt-session-scan-interval, "urn:activemq:core":mqtt-session-state-persistence-timeout, "urn:activemq:core":mqtt-subscription-persistence-enabled, "urn:activemq:core":connectors, "urn:activemq:core":acceptors, "urn:activemq:core":broadcast-groups, "urn:activemq:core":discovery-groups, "urn:activemq:core":diverts, "urn:activemq:core":queues, "urn:activemq:core":lock-coordinators, "urn:activemq:core":bridges, "urn:activemq:core":federations, "urn:activemq:core":ha-policy, "urn:activemq:core":cluster-connections, "urn:activemq:core":broker-connections, "urn:activemq:core":connection-routers, "urn:activemq:core":grouping-handler, "urn:activemq:core":purge-page-folders, "urn:activemq:core":paging-directory, "urn:activemq:core":bindings-directory, "urn:activemq:core":create-bindings-dir, "urn:activemq:core":page-max-concurrent-io, "urn:activemq:core":read-whole-page, "urn:activemq:core":journal-directory, "urn:activemq:core":journal-retention-directory, "urn:activemq:core":node-manager-lock-directory, "urn:activemq:core":create-journal-dir, "urn:activemq:core":journal-type, "urn:activemq:core":journal-buffer-timeout, "urn:activemq:core":journal-device-block-size, "urn:activemq:core":journal-buffer-size, "urn:activemq:core":journal-sync-transactional, "urn:activemq:core":journal-sync-non-transactional, "urn:activemq:core":log-journal-write-rate, "urn:activemq:core":journal-file-size, "urn:activemq:core":journal-min-files, "urn:activemq:core":journal-pool-files, "urn:activemq:core":journal-compact-percentage, "urn:activemq:core":journal-compact-min-files, "urn:activemq:core":journal-max-io, "urn:activemq:core":journal-file-open-timeout, "urn:activemq:core":server-dump-interval, "urn:activemq:core":global-max-messages, "urn:activemq:core":global-max-size-percent-of-jvm-max-memory, "urn:activemq:core":global-max-size, "urn:activemq:core":max-disk-usage, "urn:activemq:core":min-disk-free, "urn:activemq:core":disk-scan-period, "urn:activemq:core":memory-warning-threshold, "urn:activemq:core":memory-measure-interval, "urn:activemq:core":large-messages-directory, "urn:activemq:core":store, "urn:activemq:core":critical-analyzer, "urn:activemq:core":critical-analyzer-timeout, "urn:activemq:core":critical-analyzer-check-period, "urn:activemq:core":critical-analyzer-policy, "urn:activemq:core":page-sync-timeout, "urn:activemq:core":mirror-ack-manager-queue-attempts, "urn:activemq:core":mirror-ack-manager-page-attempts, "urn:activemq:core":mirror-ack-manager-retry-delay, "urn:activemq:core":mirror-ack-manager-warn-unacked, "urn:activemq:core":mirror-page-transaction, "urn:activemq:core":suppress-session-notifications, "urn:activemq:core":literal-match-markers, "urn:activemq:core":view-permission-method-match-pattern, "urn:activemq:core":management-message-rbac, "urn:activemq:core":management-rbac-prefix, "urn:activemq:core":security-settings, "urn:activemq:core":broker-plugins, "urn:activemq:core":metrics-plugin, "urn:activemq:core":metrics, "urn:activemq:core":address-settings, "urn:activemq:core":resource-limit-settings, "urn:activemq:core":connector-services, "urn:activemq:core":addresses, "urn:activemq:core":network-check-list, "urn:activemq:core":network-check-URL-list, "urn:activemq:core":network-check-period, "urn:activemq:core":network-check-timeout, "urn:activemq:core":network-check-NIC, "urn:activemq:core":network-check-ping-command, "urn:activemq:core":network-check-ping6-command, "urn:activemq:core":wildcard-addresses}' is expected."#,
            ),
        ),
    ];
    for (core, expected) in cases {
        assert_eq!(validate(core).as_deref(), *expected, "{core}");
    }
}
