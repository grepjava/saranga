//! Ported from `AddressSettingsTest` (defaults and merging).

use saranga_config::{
    AddressFullMessagePolicy, AddressSettings, DeletionPolicy, Mergeable, RoutingType,
    SlowConsumerPolicy, SlowConsumerThresholdMeasurementUnit,
};

#[test]
fn defaults() {
    let s = AddressSettings::default();
    assert_eq!(s.dead_letter_address, None);
    assert_eq!(s.expiry_address, None);
    assert_eq!(s.max_delivery_attempts(), 10);
    assert_eq!(s.max_size_bytes(), -1);
    assert_eq!(s.max_size_messages(), -1);
    assert_eq!(s.page_size_bytes(), 10 * 1024 * 1024);
    assert_eq!(s.message_counter_history_day_limit(), 0);
    assert_eq!(s.redelivery_delay(), 0);
    assert_eq!(s.redelivery_multiplier(), 1.0);
    assert_eq!(s.redelivery_collision_avoidance_factor(), 0.0);
    assert_eq!(s.max_redelivery_delay(), 0);
    assert_eq!(s.slow_consumer_threshold(), -1);
    assert_eq!(
        s.slow_consumer_threshold_measurement_unit(),
        SlowConsumerThresholdMeasurementUnit::MessagesPerSecond
    );
    assert_eq!(s.slow_consumer_check_period(), 5);
    assert_eq!(s.slow_consumer_policy(), SlowConsumerPolicy::Notify);
    assert!(s.auto_create_jms_queues());
    assert!(s.auto_delete_jms_queues());
    assert!(s.auto_create_jms_topics());
    assert!(s.auto_delete_jms_topics());
    assert!(s.auto_create_queues());
    assert!(s.auto_delete_queues());
    assert!(!s.auto_delete_created_queues());
    assert!(s.auto_create_addresses());
    assert!(s.auto_delete_addresses());
    assert!(!s.default_purge_on_no_consumers());
    assert_eq!(s.default_max_consumers(), -1);
    assert!(!s.no_expiry());

    assert_eq!(
        s.address_full_message_policy(),
        AddressFullMessagePolicy::Page
    );
    assert_eq!(s.max_read_page_messages(), -1);
    assert_eq!(s.max_read_page_bytes(), 20 * 1024 * 1024);
    assert_eq!(s.prefetch_page_bytes(), 20 * 1024 * 1024);
    assert_eq!(s.prefetch_page_messages(), -1);
    assert_eq!(s.page_cache_max_size(), 5);
    assert_eq!(s.expiry_delay(), -1);
    assert_eq!(s.min_expiry_delay(), -1);
    assert_eq!(s.max_expiry_delay(), -1);
    assert_eq!(s.redistribution_delay(), -1);
    assert!(!s.send_to_dla_on_no_route());
    assert_eq!(s.config_delete_queues(), DeletionPolicy::Off);
    assert_eq!(s.config_delete_addresses(), DeletionPolicy::Off);
    assert_eq!(s.config_delete_diverts(), DeletionPolicy::Off);
    assert_eq!(s.management_browse_page_size(), 200);
    assert_eq!(s.management_message_attribute_size_limit(), 256);
    assert_eq!(s.max_size_bytes_reject_threshold(), -1);
    assert_eq!(s.default_queue_routing_type(), RoutingType::Multicast);
    assert_eq!(s.default_address_routing_type(), RoutingType::Multicast);
    assert_eq!(s.default_consumer_window_size(), 1024 * 1024);
    assert_eq!(s.default_consumers_before_dispatch(), 0);
    assert_eq!(s.default_delay_before_dispatch(), -1);
    assert_eq!(s.default_group_buckets(), -1);
    assert_eq!(s.default_ring_size(), -1);
    assert_eq!(s.retroactive_message_count(), 0);
    assert!(!s.default_last_value_queue());
    assert!(!s.default_exclusive_queue());
    assert!(!s.default_non_destructive());
    assert!(s.enable_metrics());
    assert!(!s.enable_ingress_timestamp());
    assert!(!s.auto_create_dead_letter_resources());
    assert_eq!(s.dead_letter_queue_prefix(), "DLQ.");
    assert_eq!(s.dead_letter_queue_suffix(), "");
    assert!(!s.auto_create_expiry_resources());
    assert_eq!(s.expiry_queue_prefix(), "EXP.");
    assert_eq!(s.expiry_queue_suffix(), "");
    assert_eq!(s.queue_prefetch(), 1000);
    assert_eq!(s.id_cache_size, None);
}

#[test]
fn derived_defaults() {
    let s = AddressSettings {
        page_size_bytes: Some(1024),
        redelivery_delay: Some(300),
        max_read_page_messages: Some(7),
        ..Default::default()
    };
    assert_eq!(s.max_read_page_bytes(), 2048);
    assert_eq!(s.prefetch_page_bytes(), 2048);
    assert_eq!(s.prefetch_page_messages(), 7);
    assert_eq!(s.max_redelivery_delay(), 3000);
}

fn to_merge() -> AddressSettings {
    AddressSettings {
        dead_letter_address: Some("testDLQ".into()),
        expiry_address: Some("testExpiryQueue".into()),
        max_delivery_attempts: Some(1000),
        address_full_message_policy: Some(AddressFullMessagePolicy::Drop),
        max_size_bytes: Some(1001),
        max_size_messages: Some(101),
        message_counter_history_day_limit: Some(1002),
        redelivery_delay: Some(1003),
        page_size_bytes: Some(1004),
        max_size_bytes_reject_threshold: Some(10 * 1024),
        config_delete_diverts: Some(DeletionPolicy::Force),
        expiry_delay: Some(999),
        min_expiry_delay: Some(888),
        max_expiry_delay: Some(777),
        id_cache_size: Some(5),
        initial_queue_buffer_size: Some(256),
        no_expiry: Some(true),
        ..Default::default()
    }
}

fn check_single_merge(s: &AddressSettings) {
    assert_eq!(s.dead_letter_address.as_deref(), Some("testDLQ"));
    assert_eq!(s.expiry_address.as_deref(), Some("testExpiryQueue"));
    assert_eq!(s.max_delivery_attempts(), 1000);
    assert_eq!(s.max_size_bytes(), 1001);
    assert_eq!(s.max_size_messages(), 101);
    assert_eq!(s.message_counter_history_day_limit(), 1002);
    assert_eq!(s.redelivery_delay(), 1003);
    assert_eq!(s.page_size_bytes(), 1004);
    assert_eq!(
        s.address_full_message_policy(),
        AddressFullMessagePolicy::Drop
    );
    assert_eq!(s.max_size_bytes_reject_threshold(), 10 * 1024);
    assert_eq!(s.config_delete_diverts(), DeletionPolicy::Force);
    assert_eq!(s.expiry_delay(), 999);
    assert_eq!(s.min_expiry_delay(), 888);
    assert_eq!(s.max_expiry_delay(), 777);
    assert_eq!(s.id_cache_size, Some(5));
    assert_eq!(s.initial_queue_buffer_size, Some(256));
    assert!(s.no_expiry());
}

#[test]
fn single_merge() {
    let mut s = AddressSettings::default();
    s.merge(&to_merge());
    check_single_merge(&s);
}

#[test]
fn single_merge_copy() {
    check_single_merge(&AddressSettings::default().merge_copy(&to_merge()));
}

fn multiple_merge(copy: bool) {
    let mut s = AddressSettings::default();
    let first = AddressSettings {
        dead_letter_address: Some("testDLQ".into()),
        expiry_address: Some("testExpiryQueue".into()),
        max_delivery_attempts: Some(1000),
        max_size_bytes: Some(1001),
        message_counter_history_day_limit: Some(1002),
        address_full_message_policy: Some(AddressFullMessagePolicy::Drop),
        max_size_bytes_reject_threshold: Some(10 * 1024),
        no_expiry: Some(true),
        ..Default::default()
    };
    let second = AddressSettings {
        expiry_address: Some("testExpiryQueue2".into()),
        max_size_bytes: Some(2001),
        redelivery_delay: Some(2003),
        redelivery_multiplier: Some(2.5),
        ..Default::default()
    };
    for other in [&first, &second] {
        if copy {
            s = s.merge_copy(other);
        } else {
            s.merge(other);
        }
    }
    assert_eq!(s.dead_letter_address.as_deref(), Some("testDLQ"));
    assert_eq!(s.expiry_address.as_deref(), Some("testExpiryQueue"));
    assert_eq!(s.max_delivery_attempts(), 1000);
    assert_eq!(s.max_size_bytes(), 1001);
    assert_eq!(s.message_counter_history_day_limit(), 1002);
    assert_eq!(s.redelivery_delay(), 2003);
    assert_eq!(s.redelivery_multiplier(), 2.5);
    assert_eq!(
        s.address_full_message_policy(),
        AddressFullMessagePolicy::Drop
    );
    assert_eq!(s.max_size_bytes_reject_threshold(), 10 * 1024);
    assert!(s.no_expiry());
}

#[test]
fn multiple_merge_in_place() {
    multiple_merge(false);
}

#[test]
fn multiple_merge_copy() {
    multiple_merge(true);
}

#[test]
fn multiple_merge_all() {
    for copy in [false, true] {
        let mut s = AddressSettings::default();
        let first = AddressSettings {
            dead_letter_address: Some("testDLQ".into()),
            expiry_address: Some("testExpiryQueue".into()),
            max_size_bytes: Some(1001),
            redelivery_delay: Some(1003),
            redelivery_multiplier: Some(1.0),
            address_full_message_policy: Some(AddressFullMessagePolicy::Drop),
            ..Default::default()
        };
        let second = AddressSettings {
            expiry_address: Some("testExpiryQueue2".into()),
            dead_letter_address: Some("testDlq2".into()),
            max_delivery_attempts: Some(2000),
            max_size_bytes: Some(2001),
            message_counter_history_day_limit: Some(2002),
            redelivery_delay: Some(2003),
            redelivery_multiplier: Some(2.0),
            max_redelivery_delay: Some(5000),
            ..Default::default()
        };
        for other in [&first, &second] {
            if copy {
                s = s.merge_copy(other);
            } else {
                s.merge(other);
            }
        }
        assert_eq!(s.dead_letter_address.as_deref(), Some("testDLQ"));
        assert_eq!(s.expiry_address.as_deref(), Some("testExpiryQueue"));
        assert_eq!(s.max_delivery_attempts(), 2000);
        assert_eq!(s.max_size_bytes(), 1001);
        assert_eq!(s.message_counter_history_day_limit(), 2002);
        assert_eq!(s.redelivery_delay(), 1003);
        assert_eq!(s.redelivery_multiplier(), 1.0);
        assert_eq!(s.max_redelivery_delay(), 5000);
        assert_eq!(
            s.address_full_message_policy(),
            AddressFullMessagePolicy::Drop
        );
    }
}

#[test]
fn enum_round_trip() {
    for p in ["DROP", "PAGE", "BLOCK", "FAIL"] {
        assert_eq!(p.parse::<AddressFullMessagePolicy>().unwrap().as_str(), p);
    }
    assert!("page".parse::<AddressFullMessagePolicy>().is_err());
    assert_eq!(RoutingType::Anycast.as_byte(), 1);
    assert_eq!(RoutingType::from_byte(0), Some(RoutingType::Multicast));
    assert_eq!(RoutingType::from_byte(2), None);
}
