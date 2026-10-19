//! `broker.xml` parsing. Several tests are ported from
//! `FileConfigurationParserTest`; Java passes the snippets to
//! `parseMainConfig` directly, here they are wrapped in `<core>`.

use saranga_config::{
    AddressFullMessagePolicy, BrokerConfig, ConfigError, DiskFullMessagePolicy,
    PageFullMessagePolicy, RoutingType, Warning,
};

fn core(body: &str) -> String {
    format!(
        r#"<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
             <core xmlns="urn:activemq:core">{body}</core>
           </configuration>"#
    )
}

fn parse(body: &str) -> BrokerConfig {
    BrokerConfig::parse_with(&core(body), |_| None).unwrap()
}

fn parse_err(body: &str) -> ConfigError {
    BrokerConfig::parse_with(&core(body), |_| None).unwrap_err()
}

fn settings<'a>(c: &'a BrokerConfig, m: &str) -> &'a saranga_config::AddressSettings {
    &c.address_settings.iter().find(|(k, _)| k == m).unwrap().1
}

#[test]
fn minimal_xml() {
    let c = BrokerConfig::parse("<configuration><core></core></configuration>").unwrap();
    assert_eq!(c, BrokerConfig::default());
    assert_eq!(c.name, "localhost");
    assert_eq!(c.large_messages_directory, "data/largemessages");
}

#[test]
fn duplicate_address_settings() {
    let c = parse(
        r#"<persistence-enabled>false</persistence-enabled>
           <address-settings>
              <address-setting match="foo"><redistribution-delay>123</redistribution-delay></address-setting>
              <address-setting match="foo"><redistribution-delay>456</redistribution-delay></address-setting>
           </address-settings>"#,
    );
    assert!(!c.persistence_enabled);
    assert_eq!(c.address_settings.len(), 1);
    assert_eq!(settings(&c, "foo").redistribution_delay(), 123);
    assert_eq!(
        c.warnings,
        [Warning::DuplicateAddressSetting {
            r#match: "foo".into()
        }]
    );
}

#[test]
fn address_with_no_routing_type() {
    let e = parse_err(r#"<addresses><address name="a"/></addresses>"#);
    assert!(
        matches!(e, ConfigError::AddressWithNoRoutingType(ref a) if a == "a"),
        "{e}"
    );
}

#[test]
fn duplicate_queue() {
    let c = parse(
        r#"<addresses>
              <address name="a"><anycast><queue name="q" max-consumers="0"/></anycast></address>
              <address name="b"><anycast><queue name="q" max-consumers="1"/></anycast></address>
           </addresses>"#,
    );
    assert_eq!(c.addresses[0].queues[0].max_consumers, Some(0));
    assert_eq!(c.addresses[1].queues[0].max_consumers, Some(1));
}

#[test]
fn purge_page_folder() {
    assert_eq!(
        parse("<purge-page-folders>true</purge-page-folders>")
            .journal
            .purge_page_folders,
        Some(true)
    );
    assert_eq!(
        parse("<purge-page-folders>false</purge-page-folders>")
            .journal
            .purge_page_folders,
        Some(false)
    );
}

#[test]
fn wildcard_configuration() {
    let c = parse(
        r#"<wildcard-addresses>
              <routing-enabled>true</routing-enabled>
              <delimiter>/</delimiter>
              <any-words>&gt;</any-words>
           </wildcard-addresses>"#,
    );
    let wc = c.wildcard_addresses;
    assert_eq!(wc.delimiter, '/');
    assert!(wc.routing_enabled);
    assert_eq!(wc.any_words, '>');
    assert_eq!(wc.single_word, '*');

    let off = parse("<wildcard-addresses><enabled>false</enabled></wildcard-addresses>");
    assert!(!off.wildcard_addresses.routing_enabled);
}

#[test]
fn custom_wildcards_drive_the_repository() {
    let c = parse(
        r#"<wildcard-addresses><delimiter>/</delimiter><any-words>&gt;</any-words></wildcard-addresses>
           <address-settings>
              <address-setting match="a/&gt;"><max-delivery-attempts>3</max-delivery-attempts></address-setting>
           </address-settings>"#,
    );
    let repo = c.address_settings_repository().unwrap();
    assert_eq!(repo.get_match("a/b/c").unwrap().max_delivery_attempts(), 3);
    assert_eq!(repo.get_match("a.b").unwrap().max_delivery_attempts(), 10);
}

#[test]
fn parsing_overflow_page_size() {
    for body in [
        r##"<address-settings><address-setting match="#"><page-size-bytes>2147483648</page-size-bytes></address-setting></address-settings>"##,
        "<journal-file-size>2147483648</journal-file-size>",
        "<journal-buffer-size>2147483648</journal-buffer-size>",
    ] {
        assert!(
            matches!(parse_err(body), ConfigError::InvalidValue { .. }),
            "{body}"
        );
    }
}

#[test]
fn max_size_on_address_settings() {
    let c = parse(
        r#"<address-settings><address-setting match="foo"><max-size-messages>123</max-size-messages></address-setting></address-settings>"#,
    );
    assert_eq!(settings(&c, "foo").max_size_messages(), 123);
}

#[test]
fn max_read_address_settings() {
    let c = parse(
        r#"<address-settings><address-setting match="foo">
              <max-read-page-bytes>1k</max-read-page-bytes><max-read-page-messages>33</max-read-page-messages>.
           </address-setting></address-settings>"#,
    );
    let s = settings(&c, "foo");
    assert_eq!(s.max_read_page_bytes(), 1024);
    assert_eq!(s.max_read_page_messages(), 33);
    assert_eq!(s.page_limit_bytes, None);
    assert_eq!(s.page_limit_messages, None);
    assert_eq!(s.page_full_message_policy, None);
}

#[test]
fn page_limit_settings() {
    let c = parse(
        r#"<address-settings><address-setting match="foo">
              <max-read-page-bytes>1k</max-read-page-bytes>
              <prefetch-page-bytes>100M</prefetch-page-bytes>
              <prefetch-page-messages>777</prefetch-page-messages>
              <page-limit-bytes>10G</page-limit-bytes>
              <page-limit-messages>3221225472</page-limit-messages>
              <page-full-policy>FAIL</page-full-policy>
              <max-read-page-messages>33</max-read-page-messages>
           </address-setting></address-settings>"#,
    );
    let s = settings(&c, "foo");
    assert_eq!(s.max_read_page_bytes(), 1024);
    assert_eq!(s.max_read_page_messages(), 33);
    assert_eq!(s.page_limit_bytes, Some(10 * 1024 * 1024 * 1024));
    assert_eq!(s.prefetch_page_bytes(), 100 * 1024 * 1024);
    assert_eq!(s.prefetch_page_messages(), 777);
    assert_eq!(s.page_limit_messages, Some(3 * 1024 * 1024 * 1024));
    assert_eq!(
        s.page_full_message_policy,
        Some(PageFullMessagePolicy::Fail)
    );
}

#[test]
fn max_read_address_settings_all_negative() {
    let c = parse(
        r#"<address-settings><address-setting match="foo">
              <max-read-page-bytes>-1</max-read-page-bytes>
              <max-read-page-messages>-1</max-read-page-messages>
           </address-setting></address-settings>"#,
    );
    let s = settings(&c, "foo");
    assert_eq!(s.max_read_page_bytes(), -1);
    assert_eq!(s.max_read_page_messages(), -1);
}

#[test]
fn prefetch_negative_means_unset() {
    let c = parse(
        r#"<address-settings><address-setting match="foo">
              <prefetch-page-bytes>-1</prefetch-page-bytes>
              <prefetch-page-messages>-1</prefetch-page-messages>
           </address-setting></address-settings>"#,
    );
    let s = settings(&c, "foo");
    assert_eq!(s.prefetch_page_bytes, None);
    assert_eq!(s.prefetch_page_messages, None);
    assert_eq!(s.prefetch_page_bytes(), 20 * 1024 * 1024);
}

#[test]
fn literal_match_markers() {
    let c = parse(
        r#"<literal-match-markers>()</literal-match-markers>
           <address-settings>
              <address-setting match="(foo.#)"><max-delivery-attempts>1</max-delivery-attempts></address-setting>
              <address-setting match="foo.#"><max-delivery-attempts>2</max-delivery-attempts></address-setting>
           </address-settings>"#,
    );
    assert_eq!(c.literal_match_markers, Some(('(', ')')));
    let repo = c.address_settings_repository().unwrap();
    assert_eq!(repo.get_match("foo.#").unwrap().max_delivery_attempts(), 1);
    assert_eq!(
        repo.get_match("foo.bar").unwrap().max_delivery_attempts(),
        2
    );

    assert!(matches!(
        parse_err("<literal-match-markers>(</literal-match-markers>"),
        ConfigError::InvalidValue { .. }
    ));
}

#[test]
fn expected_error_over_max_message_notation() {
    parse_err(
        r#"<address-settings><address-setting match="foo"><max-size-messages>123K</max-size-messages></address-setting></address-settings>"#,
    );
}

#[test]
fn parsing_address_settings_reject_threshold() {
    let c = parse(
        r##"<address-settings><address-setting match="#"><max-size-bytes-reject-threshold>2147483648</max-size-bytes-reject-threshold></address-setting></address-settings>"##,
    );
    assert_eq!(c.address_settings.len(), 1);
    assert_eq!(
        settings(&c, "#").max_size_bytes_reject_threshold(),
        2147483648
    );
}

#[test]
fn parsing_page_sync_timeout() {
    assert_eq!(
        parse("<page-sync-timeout>1000</page-sync-timeout>")
            .journal
            .page_sync_timeout,
        Some(1000)
    );
}

#[test]
fn max_size() {
    let c = parse(
        "<global-max-size>10M</global-max-size><global-max-messages>1000</global-max-messages>
         <global-max-size-percent-of-jvm-max-memory>30</global-max-size-percent-of-jvm-max-memory>",
    );
    assert_eq!(c.global_max_size, Some(10 * 1024 * 1024));
    assert_eq!(c.global_max_messages, 1000);
    assert_eq!(c.global_max_size_percent_of_jvm_max_memory, 30);
}

#[test]
fn global_max_size_minus_one_is_unset() {
    assert_eq!(
        parse("<global-max-size>-1</global-max-size>").global_max_size,
        None
    );
    parse_err("<global-max-size>0</global-max-size>");
}

#[test]
fn configuration_persist_redelivery() {
    assert_eq!(
        parse("<max-redelivery-records>0</max-redelivery-records>")
            .journal
            .max_redelivery_records,
        Some(0)
    );
}

#[test]
fn exception_max_size() {
    parse_err("<global-max-messages>1000K</global-max-messages>");
}

#[test]
fn notations() {
    let c = parse(
        "<global-max-size>100MiB</global-max-size><journal-file-size>10M</journal-file-size>
         <journal-buffer-size>5Mb</journal-buffer-size>",
    );
    assert_eq!(c.global_max_size, Some(100 * 1024 * 1024));
    assert_eq!(c.journal.file_size, Some(10 * 1024 * 1024));
    assert_eq!(c.journal.buffer_size, Some(5 * 1024 * 1024));
}

#[test]
fn parsing_disk_full_policy() {
    let c = parse(
        r#"<address-settings><address-setting match="foo"><disk-full-policy>FAIL</disk-full-policy></address-setting></address-settings>"#,
    );
    assert_eq!(
        settings(&c, "foo").disk_full_message_policy,
        Some(DiskFullMessagePolicy::Fail)
    );
}

#[test]
fn address_setting_children_ignore_case_and_enums_do_not() {
    let c = parse(
        r#"<address-settings><address-setting match="foo"><Address-Full-Policy>BLOCK</Address-Full-Policy></address-setting></address-settings>"#,
    );
    assert_eq!(
        settings(&c, "foo").address_full_message_policy(),
        AddressFullMessagePolicy::Block
    );
    parse_err(
        r#"<address-settings><address-setting match="foo"><address-full-policy>block</address-full-policy></address-setting></address-settings>"#,
    );
}

#[test]
fn address_setting_validators() {
    parse_err(
        r#"<address-settings><address-setting match="f"><redelivery-collision-avoidance-factor>1.5</redelivery-collision-avoidance-factor></address-setting></address-settings>"#,
    );
    parse_err(
        r#"<address-settings><address-setting match="f"><initial-queue-buffer-size>100</initial-queue-buffer-size></address-setting></address-settings>"#,
    );
    parse_err(
        r#"<address-settings><address-setting match="f"><slow-consumer-check-period>0</slow-consumer-check-period></address-setting></address-settings>"#,
    );
    let c = parse(
        r#"<address-settings><address-setting match="f"><initial-queue-buffer-size>128</initial-queue-buffer-size><last-value-queue>TRUE</last-value-queue></address-setting></address-settings>"#,
    );
    assert_eq!(settings(&c, "f").initial_queue_buffer_size, Some(128));
    assert!(settings(&c, "f").default_last_value_queue());
}

#[test]
fn queue_attributes_and_children() {
    let c = parse(
        r#"<addresses>
              <address name="orders">
                 <multicast>
                    <queue name="sub" max-consumers="5" purge-on-no-consumers="true" exclusive="true"
                           last-value-key="k" ring-size="10" delay-before-dispatch="20" enabled="false">
                       <filter string="color='red'"/>
                       <durable>false</durable>
                       <user>bob</user>
                    </queue>
                 </multicast>
                 <anycast/>
              </address>
           </addresses>"#,
    );
    let a = &c.addresses[0];
    assert_eq!(
        a.routing_types,
        [RoutingType::Multicast, RoutingType::Anycast]
    );
    let q = &a.queues[0];
    assert_eq!(q.routing_type, RoutingType::Multicast);
    assert_eq!(q.max_consumers, Some(5));
    assert!(q.purge_on_no_consumers);
    assert_eq!(q.exclusive, Some(true));
    assert_eq!(q.last_value_key.as_deref(), Some("k"));
    assert_eq!(q.ring_size, 10);
    assert_eq!(q.delay_before_dispatch, Some(20));
    assert_eq!(q.enabled, Some(false));
    assert_eq!(q.filter.as_deref(), Some("color='red'"));
    assert!(!q.durable);
    assert_eq!(q.user.as_deref(), Some("bob"));
    assert_eq!(q.last_value, None);

    parse_err(
        r#"<addresses><address name="a"><anycast><queue name="q" max-consumers="-2"/></anycast></address></addresses>"#,
    );
}

#[test]
fn journal_type_and_validators() {
    assert_eq!(
        parse("<journal-type>NIO</journal-type>")
            .journal
            .journal_type,
        saranga_config::JournalType::Nio
    );
    parse_err("<journal-type>nio</journal-type>");
    parse_err("<journal-min-files>0</journal-min-files>");
    parse_err("<journal-compact-percentage>101</journal-compact-percentage>");
    parse_err("<journal-directory>  </journal-directory>");
}

#[test]
fn missing_required_attributes() {
    assert!(matches!(
        parse_err("<address-settings><address-setting/></address-settings>"),
        ConfigError::MissingAttribute { .. }
    ));
    assert!(matches!(
        parse_err("<addresses><address><anycast/></address></addresses>"),
        ConfigError::MissingAttribute { .. }
    ));
}

#[test]
fn malformed_xml_is_an_error() {
    assert!(matches!(
        BrokerConfig::parse("<configuration><core>"),
        Err(ConfigError::Xml(_))
    ));
}

#[test]
fn broadcast_and_discovery_groups() {
    let c = parse(
        r#"<connectors>
             <connector name="a">tcp://localhost:61616</connector>
             <connector name="b">tcp://localhost:61617</connector>
           </connectors>
           <broadcast-groups>
             <broadcast-group name="bg">
               <local-bind-address>127.0.0.1</local-bind-address>
               <local-bind-port>5432</local-bind-port>
               <group-address>231.7.7.7</group-address>
               <group-port>9876</group-port>
               <broadcast-period>1500</broadcast-period>
               <connector-ref>a</connector-ref>
               <connector-ref>b</connector-ref>
             </broadcast-group>
           </broadcast-groups>
           <discovery-groups>
             <discovery-group name="dg">
               <group-address>231.7.7.7</group-address>
               <group-port>9876</group-port>
               <refresh-timeout>5000</refresh-timeout>
             </discovery-group>
             <discovery-group name="dg">
               <group-address>231.7.7.8</group-address>
               <group-port>9877</group-port>
             </discovery-group>
           </discovery-groups>"#,
    );
    let bg = &c.broadcast_groups[0];
    assert_eq!(bg.name, "bg");
    assert_eq!(bg.broadcast_period, 1500);
    // As parseBroadcastGroupConfiguration reads connector-refs: the first,
    // once for each.
    assert_eq!(bg.connectors, ["a", "a"]);
    assert_eq!(bg.endpoint.group_address.as_deref(), Some("231.7.7.7"));
    assert_eq!(bg.endpoint.group_port, 9876);
    assert_eq!(bg.endpoint.local_bind_address.as_deref(), Some("127.0.0.1"));
    assert_eq!(bg.endpoint.local_bind_port, 5432);
    // The second "dg" is not deployed.
    assert_eq!(c.discovery_groups.len(), 1);
    let dg = &c.discovery_groups[0];
    assert_eq!(dg.refresh_timeout, 5000);
    assert_eq!(dg.initial_wait_timeout, 10_000);
    assert_eq!(dg.endpoint.group_port, 9876);
    assert_eq!(dg.endpoint.local_bind_port, -1);
    assert!(c.warnings.iter().any(|w| matches!(
        w,
        Warning::DiscoveryGroupAlreadyDeployed { name } if name == "dg"
    )));
}

#[test]
fn broadcast_group_defaults_and_jgroups() {
    let c = parse(
        r#"<broadcast-groups><broadcast-group name="bg">
             <group-address>231.7.7.7</group-address><group-port>9876</group-port>
           </broadcast-group></broadcast-groups>"#,
    );
    assert_eq!(c.broadcast_groups[0].broadcast_period, 2000);
    assert!(c.broadcast_groups[0].connectors.is_empty());
    let e = parse_err(
        r#"<discovery-groups><discovery-group name="dg">
             <jgroups-file>test-jgroups-file_ping.xml</jgroups-file>
             <jgroups-channel>dg</jgroups-channel>
           </discovery-group></discovery-groups>"#,
    );
    assert!(
        matches!(e, ConfigError::Unsupported { ref element, .. } if element == "discovery-group")
    );
    let e = parse_err(
        r#"<broadcast-groups><broadcast-group name="bg">
             <broadcast-period>0</broadcast-period>
           </broadcast-group></broadcast-groups>"#,
    );
    assert!(matches!(e, ConfigError::InvalidValue { ref name, .. } if name == "broadcast-period"));
}

#[test]
fn bridges() {
    let c = parse(
        r#"<bridges>
             <bridge name="b1">
               <queue-name>sausage-factory</queue-name>
               <forwarding-address>mincing-machine</forwarding-address>
               <filter string="name='aardvark'"/>
               <transformer>
                 <class-name>org.example.Transformer</class-name>
                 <property key="k" value="v"/>
               </transformer>
               <retry-interval>1000</retry-interval>
               <retry-interval-multiplier>2.5</retry-interval-multiplier>
               <max-retry-interval>9000</max-retry-interval>
               <initial-connect-attempts>3</initial-connect-attempts>
               <reconnect-attempts>-1</reconnect-attempts>
               <reconnect-attempts-same-node>4</reconnect-attempts-same-node>
               <use-duplicate-detection>false</use-duplicate-detection>
               <confirmation-window-size>1MB</confirmation-window-size>
               <producer-window-size>-1</producer-window-size>
               <check-period>5000</check-period>
               <connection-ttl>7000</connection-ttl>
               <min-large-message-size>50KB</min-large-message-size>
               <routing-type>ANYCAST</routing-type>
               <concurrency>2</concurrency>
               <pending-ack-timeout>1234</pending-ack-timeout>
               <client-id>cid</client-id>
               <ha>true</ha>
               <user>bridge-user</user>
               <password>secret</password>
               <static-connectors>
                 <connector-ref>remote1</connector-ref>
                 <connector-ref>remote2</connector-ref>
               </static-connectors>
             </bridge>
             <bridge name="b2">
               <queue-name>q</queue-name>
               <transformer-class-name>org.example.Other</transformer-class-name>
               <discovery-group-ref discovery-group-name="dg"/>
             </bridge>
           </bridges>"#,
    );
    let b = &c.bridges[0];
    assert_eq!(b.name, "b1");
    assert_eq!(b.parent_name, "b1");
    assert_eq!(b.queue_name.as_deref(), Some("sausage-factory"));
    assert_eq!(b.forwarding_address.as_deref(), Some("mincing-machine"));
    assert_eq!(b.filter.as_deref(), Some("name='aardvark'"));
    let t = b.transformer.as_ref().unwrap();
    assert_eq!(t.class_name, "org.example.Transformer");
    assert_eq!(t.properties, [("k".to_owned(), "v".to_owned())]);
    assert_eq!(b.retry_interval, 1000);
    assert_eq!(b.retry_interval_multiplier, 2.5);
    assert_eq!(b.max_retry_interval, 9000);
    assert_eq!(b.initial_connect_attempts, 3);
    assert_eq!(b.reconnect_attempts, -1);
    assert_eq!(b.reconnect_attempts_on_same_node, 4);
    assert!(!b.use_duplicate_detection);
    assert_eq!(b.confirmation_window_size, 1024 * 1024);
    assert_eq!(b.producer_window_size, -1);
    assert_eq!(b.client_failure_check_period, 5000);
    assert_eq!(b.connection_ttl, 7000);
    assert_eq!(b.min_large_message_size, 50 * 1024);
    assert_eq!(
        b.routing_type,
        saranga_config::ComponentRoutingType::Anycast
    );
    assert_eq!(b.concurrency, 2);
    assert_eq!(b.pending_ack_timeout, 1234);
    assert_eq!(b.client_id.as_deref(), Some("cid"));
    assert!(b.ha);
    assert_eq!(b.user.as_deref(), Some("bridge-user"));
    assert_eq!(b.password.as_deref(), Some("secret"));
    assert_eq!(
        b.static_connectors.as_deref(),
        Some(&["remote1".to_owned(), "remote2".to_owned()][..])
    );
    assert!(b.configuration_managed);

    let b = &c.bridges[1];
    assert_eq!(b.discovery_group.as_deref(), Some("dg"));
    assert_eq!(b.static_connectors, None);
    assert_eq!(
        b.transformer.as_ref().map(|t| t.class_name.as_str()),
        Some("org.example.Other")
    );
    // BridgeConfiguration's defaults.
    assert_eq!(b.forwarding_address, None);
    assert_eq!(b.retry_interval, 2000);
    assert_eq!(b.confirmation_window_size, 10 * 1024 * 1024);
    assert_eq!(b.producer_window_size, 1024 * 1024);
    assert_eq!(b.reconnect_attempts_on_same_node, 10);
    assert!(b.use_duplicate_detection);
    assert_eq!(b.user.as_deref(), Some("ACTIVEMQ.CLUSTER.ADMIN.USER"));
    assert_eq!(b.password.as_deref(), Some("CHANGE ME!!"));
    assert_eq!(b.routing_type, saranga_config::ComponentRoutingType::Pass);
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);

    // A queue-name given empty is refused; one left out is not (the bridge
    // is not deployed).
    let e = parse_err(r#"<bridges><bridge name="b"><queue-name> </queue-name></bridge></bridges>"#);
    assert!(matches!(e, ConfigError::InvalidValue { ref name, .. } if name == "queue-name"));
    let c = parse(r#"<bridges><bridge name="b"/></bridges>"#);
    assert_eq!(c.bridges[0].queue_name, None);
    let e = parse_err(
        r#"<bridges><bridge name="b"><queue-name>q</queue-name><concurrency>0</concurrency></bridge></bridges>"#,
    );
    assert!(matches!(e, ConfigError::InvalidValue { ref name, .. } if name == "concurrency"));
}

#[test]
fn grouping_handler() {
    let c = parse(
        r#"<grouping-handler name="gh1">
             <type>LOCAL</type>
             <address>jms</address>
             <timeout>3000</timeout>
             <group-timeout>60000</group-timeout>
             <reaper-period>15000</reaper-period>
           </grouping-handler>"#,
    );
    assert_eq!(
        c.grouping_handler,
        Some(saranga_config::GroupingHandlerConfig {
            name: "gh1".into(),
            local: true,
            address: "jms".into(),
            timeout: 3000,
            group_timeout: 60_000,
            reaper_period: 15_000,
        })
    );
    // Defaults; any type but LOCAL is REMOTE.
    let c = parse(r#"<grouping-handler name="gh2"><type>REMOTE</type></grouping-handler>"#);
    let g = c.grouping_handler.unwrap();
    assert!(!g.local);
    assert_eq!(g.address, "");
    assert_eq!(
        (g.timeout, g.group_timeout, g.reaper_period),
        (5000, -1, 30_000)
    );
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
    // The type is required, and the timeouts checked.
    parse_err(r#"<grouping-handler name="g"><address>a</address></grouping-handler>"#);
    parse_err(
        r#"<grouping-handler name="g"><type>LOCAL</type><timeout>0</timeout></grouping-handler>"#,
    );
    parse_err(
        r#"<grouping-handler name="g"><type>LOCAL</type><group-timeout>0</group-timeout></grouping-handler>"#,
    );
}

#[test]
fn federations() {
    use saranga_config::{FederationPolicyConfig, FederationQueueMatcher};
    let c = parse(
        r##"<federations downstream-authorization="a, b,,c">
             <federation name="f1" user="globaluser" password="secret">
               <upstream name="up1" user="westuser" priority-adjustment="2">
                 <static-connectors><connector-ref>c1</connector-ref></static-connectors>
                 <retry-interval>100</retry-interval>
                 <retry-interval-multiplier>2</retry-interval-multiplier>
                 <share-connection>true</share-connection>
                 <policy ref="set"/>
                 <policy ref="set"/>
               </upstream>
               <downstream name="down1">
                 <ha>true</ha>
                 <discovery-group-ref discovery-group-name="dg1"/>
                 <policy ref="qp"/>
                 <upstream-connector-ref>c2</upstream-connector-ref>
               </downstream>
               <policy-set name="set"><policy ref="ap"/><policy ref="qp"/></policy-set>
               <queue-policy name="qp" include-federated="true" priority-adjustment="-3" transformer-ref="t">
                 <include queue-match="q.#" address-match="#"/>
                 <exclude queue-match="q.x" address-match="#"/>
               </queue-policy>
               <address-policy name="ap" max-hops="2" auto-delete="true" auto-delete-delay="10" auto-delete-message-count="-1" enable-divert-bindings="true">
                 <include address-match="news.#"/>
                 <include address-match="news.#"/>
               </address-policy>
               <transformer name="t">
                 <class-name>org.foo.T</class-name>
                 <property key="k" value="v"/>
               </transformer>
             </federation>
           </federations>"##,
    );
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
    assert_eq!(c.federation_downstream_authorization, ["a", "b", "c"]);
    let f = &c.federations[0];
    assert_eq!(f.name, "f1");
    assert_eq!(f.user.as_deref(), Some("globaluser"));
    assert_eq!(f.password.as_deref(), Some("secret"));
    let up = &f.upstreams[0];
    assert_eq!(up.name, "up1");
    assert_eq!(up.policy_refs, ["set"]);
    let uc = &up.connection;
    assert_eq!(uc.username.as_deref(), Some("westuser"));
    assert_eq!(uc.password, None);
    assert_eq!(uc.priority_adjustment, 2);
    assert_eq!(
        uc.static_connectors.as_deref(),
        Some(&["c1".to_owned()][..])
    );
    assert_eq!(uc.retry_interval, 100);
    assert_eq!(uc.retry_interval_multiplier, 2.0);
    // Artemis 2.57 does not read share-connection.
    assert!(!uc.share_connection);
    assert_eq!(
        (
            uc.connection_ttl,
            uc.client_failure_check_period,
            uc.max_retry_interval
        ),
        (60_000, 30_000, 2000)
    );
    assert_eq!(
        (uc.call_timeout, uc.call_failover_timeout),
        (30_000, 30_000)
    );
    assert_eq!(
        (
            uc.initial_connect_attempts,
            uc.reconnect_attempts,
            uc.circuit_breaker_timeout
        ),
        (-1, -1, 30_000)
    );
    let down = &f.downstreams[0];
    assert_eq!(down.upstream_connector_ref, "c2");
    assert!(down.stream.connection.ha);
    assert_eq!(
        down.stream.connection.discovery_group.as_deref(),
        Some("dg1")
    );
    assert_eq!(down.stream.connection.username.as_deref(), Some(""));
    assert!(
        matches!(f.policy("set"), Some(FederationPolicyConfig::Set { policy_refs, .. }) if policy_refs == &["ap", "qp"])
    );
    let Some(FederationPolicyConfig::Queue(q)) = f.policy("qp") else {
        panic!()
    };
    assert!(q.include_federated);
    assert_eq!(q.priority_adjustment, Some(-3));
    assert_eq!(q.transformer_ref.as_deref(), Some("t"));
    assert_eq!(
        q.includes,
        [FederationQueueMatcher {
            queue_match: Some("q.#".into()),
            address_match: Some("#".into())
        }]
    );
    let Some(FederationPolicyConfig::Address(a)) = f.policy("ap") else {
        panic!()
    };
    // max-hops is not read; Artemis reads max-consumers in its place.
    assert_eq!(a.max_hops, 0);
    assert_eq!(a.includes, ["news.#"]);
    assert_eq!(a.auto_delete, Some(true));
    assert_eq!(a.auto_delete_delay, Some(10));
    assert_eq!(a.auto_delete_message_count, Some(-1));
    assert!(a.enable_divert_bindings);
    assert_eq!(f.transformer("t").unwrap().class_name, "org.foo.T");

    let c = parse(
        r##"<federations><federation name="f"><address-policy name="ap" max-consumers="3"/></federation></federations>"##,
    );
    let Some(FederationPolicyConfig::Address(a)) = c.federations[0].policy("ap") else {
        panic!()
    };
    assert_eq!(a.max_hops, 3);
    // A downstream needs its upstream connector.
    parse_err(
        r##"<federations><federation name="f"><downstream name="d"/></federation></federations>"##,
    );
    parse_err(
        r##"<federations><federation name="f"><upstream name="u"><check-period>0</check-period></upstream></federation></federations>"##,
    );
    parse_err(
        r##"<federations><federation name="f"><address-policy name="ap" auto-delete-delay="-1"/></federation></federations>"##,
    );
}

#[test]
fn broker_connections() {
    use saranga_config::{AmqpConnectionElement as E, AmqpQueueMatch};
    let c = parse(
        r##"<broker-connections>
             <amqp-connection uri="tcp://a:5672?amqpCredits=10" name="c1" user="u" password="p" retry-interval="100" reconnect-attempts="3" auto-start="false">
               <sender address-match="out.#"/>
               <receiver queue-name="in"/>
               <peer address-match="both"/>
               <mirror durable="false" sync="true" address-filter="x,!y">
                 <property key="tunnel-core-messages" value="false"/>
               </mirror>
               <federation>
                 <local-address-policy name="lap" max-hops="2" auto-delete="true" auto-delete-delay="10" enable-divert-bindings="true">
                   <include address-match="news.#"/>
                   <include address-match="news.#"/>
                   <exclude address-match="news.x"/>
                   <transformer-class-name>org.foo.T</transformer-class-name>
                 </local-address-policy>
                 <remote-queue-policy name="rqp" include-federated="true" priority-adjustment="-2">
                   <include address-match="#" queue-match="q.#"/>
                   <property key="k" value="v"/>
                 </remote-queue-policy>
                 <property key="receiverCredits" value="5"/>
               </federation>
               <bridge>
                 <bridge-to-address name="bta" remote-address="there" remote-terminus-capabilities="topic,shared" use-durable-subscriptions="true">
                   <include address-match="a"/>
                 </bridge-to-address>
                 <bridge-from-queue name="bfq" priority-adjustment="1" filter="x = 1">
                   <include address-match="b" queue-match="b"/>
                 </bridge-from-queue>
               </bridge>
             </amqp-connection>
             <amqp-connection uri="tcp://b:5672" name="c2"/>
           </broker-connections>"##,
    );
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
    let [c1, c2] = &c.broker_connections[..] else {
        panic!("{:?}", c.broker_connections)
    };
    assert_eq!(c1.name, "c1");
    assert_eq!(c1.uri, "tcp://a:5672?amqpCredits=10");
    assert_eq!((c1.retry_interval, c1.reconnect_attempts), (100, 3));
    assert_eq!(
        (c1.user.as_deref(), c1.password.as_deref()),
        (Some("u"), Some("p"))
    );
    assert!(!c1.auto_start);
    assert_eq!(c1.elements.len(), 6);
    assert!(
        matches!(&c1.elements[0], E::Sender(m) if m.address_match.as_deref() == Some("out.#") && m.queue_name.is_none())
    );
    assert!(matches!(&c1.elements[1], E::Receiver(m) if m.queue_name.as_deref() == Some("in")));
    assert!(matches!(&c1.elements[2], E::Peer(_)));
    let E::Mirror(m) = &c1.elements[3] else {
        panic!()
    };
    assert!(!m.durable && m.sync && m.message_acknowledgements);
    assert_eq!(m.address_filter.as_deref(), Some("x,!y"));
    assert_eq!(
        m.properties,
        [("tunnel-core-messages".into(), "false".into())]
    );
    let E::Federation(f) = &c1.elements[4] else {
        panic!()
    };
    assert_eq!(f.name, "c1");
    let lap = &f.local_address_policies[0];
    assert_eq!(lap.max_hops, 2);
    assert_eq!(lap.includes, ["news.#"]);
    assert_eq!(lap.excludes, ["news.x"]);
    assert_eq!(lap.auto_delete, Some(true));
    assert_eq!(lap.enable_divert_bindings, Some(true));
    assert_eq!(
        lap.transformer.as_ref().map(|t| t.class_name.as_str()),
        Some("org.foo.T")
    );
    let rqp = &f.remote_queue_policies[0];
    assert!(rqp.include_federated);
    assert_eq!(rqp.priority_adjustment, Some(-2));
    assert_eq!(
        rqp.includes,
        [AmqpQueueMatch {
            address_match: "#".into(),
            queue_match: "q.#".into()
        }]
    );
    assert_eq!(rqp.properties, [("k".into(), "v".into())]);
    assert_eq!(f.properties, [("receiverCredits".into(), "5".into())]);
    let E::Bridge(b) = &c1.elements[5] else {
        panic!()
    };
    assert_eq!(b.name, "c1");
    let bta = &b.to_address_policies[0];
    assert_eq!(bta.remote_address.as_deref(), Some("there"));
    assert_eq!(
        bta.remote_terminus_capabilities.as_deref(),
        Some(&["topic".to_owned(), "shared".to_owned()][..])
    );
    assert!(bta.use_durable_subscriptions);
    let bfq = &b.from_queue_policies[0];
    assert_eq!(bfq.priority_adjustment, Some(1));
    assert_eq!(bfq.filter.as_deref(), Some("x = 1"));
    // The defaults.
    assert_eq!((c2.retry_interval, c2.reconnect_attempts), (5000, -1));
    assert!(c2.auto_start && c2.user.is_none() && c2.elements.is_empty());

    let e = parse_err(
        r#"<broker-connections><amqp-connection uri="tcp://a" name="x" retry-interval="0"/></broker-connections>"#,
    );
    assert!(e.to_string().contains("retry-interval"), "{e}");
}
