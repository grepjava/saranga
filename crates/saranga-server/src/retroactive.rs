//! Retroactive addresses (`retroactive-message-count`), as Artemis's
//! `PostOfficeImpl` makes them: an address whose settings keep a number of
//! messages gets an internal address with a ring queue per routing type,
//! fed by a non-exclusive divert, and a queue made on the address later is
//! given what the ring queue of its routing type holds
//! (`copyRetroactiveMessages`).

use std::collections::HashMap;

use crate::RoutingType;
use crate::postoffice::{
    AddressInfo, DivertDeployment, DivertRoutingType, PostOffice, QueueSpec, RoutingTypes,
};
use crate::queue::{Queue, Selection, SelectionAction};

/// `ResourceNames.RETROACTIVE_SUFFIX`.
const SUFFIX: &str = "retro";

/// The names of an address's retroactive resources (`ResourceNames`).
pub(crate) struct Names {
    pub address: String,
    pub anycast: String,
    pub multicast: String,
    pub divert: String,
}

impl Names {
    pub(crate) fn of(prefix: &str, delimiter: char, address: &str) -> Self {
        let name = |kind: &str| format!("{prefix}{address}{delimiter}{kind}{delimiter}{SUFFIX}");
        Self {
            address: name("address"),
            anycast: name(&format!("queue{delimiter}anycast")),
            multicast: name(&format!("queue{delimiter}multicast")),
            divert: name("divert"),
        }
    }

    pub(crate) fn queue(&self, routing_type: RoutingType) -> &str {
        match routing_type {
            RoutingType::Anycast => &self.anycast,
            RoutingType::Multicast => &self.multicast,
        }
    }
}

/// `ResourceNames.isRetroactiveResource`.
pub fn is_retroactive_resource(prefix: &str, name: &str) -> bool {
    name.starts_with(prefix) && name.ends_with(SUFFIX)
}

/// `ResourceNames.decomposeRetroactiveResourceAddressName`: the address an
/// internal retroactive address serves.
fn served_address<'a>(prefix: &str, delimiter: char, name: &'a str) -> Option<&'a str> {
    let tail = delimiter.len_utf8() + "address".len() + delimiter.len_utf8() + SUFFIX.len();
    name.get(prefix.len()..name.len().checked_sub(tail)?)
}

impl PostOffice {
    fn retro_names(&self, address: &str) -> Names {
        Names::of(self.internal_prefix(), self.wildcard().delimiter, address)
    }

    /// Whether `name` is one of a retroactive address's resources.
    pub fn is_retroactive_resource(&self, name: &str) -> bool {
        is_retroactive_resource(self.internal_prefix(), name)
    }

    /// `addAddressInfo`'s end: an address its settings make retroactive gets
    /// its resources; recovered (`reload`), only the divert, the address and
    /// queues being recovered with it.
    pub(crate) fn retroactive_address_added(&self, info: &AddressInfo, reload: bool) {
        let count = self.settings(&info.name).retroactive_message_count;
        if count <= 0 || info.internal || self.is_retroactive_resource(&info.name) {
            return;
        }
        if let Err(e) = self.create_retroactive_resources(&info.name, count, reload) {
            tracing::warn!("creating the retroactive resources of {}: {e}", info.name);
        }
    }

    fn create_retroactive_resources(
        &self,
        address: &str,
        count: i64,
        reload: bool,
    ) -> crate::Result<()> {
        let names = self.retro_names(address);
        if !reload {
            let mut types = RoutingTypes::of(RoutingType::Multicast);
            types.add(RoutingType::Anycast);
            self.add_address_info(AddressInfo {
                id: 0,
                name: names.address.as_str().into(),
                routing_types: types,
                auto_created: false,
                temporary: false,
                internal: false,
            })?;
            for routing_type in [RoutingType::Multicast, RoutingType::Anycast] {
                let mut spec = QueueSpec::new(names.queue(routing_type), names.address.as_str());
                spec.routing_type = Some(routing_type);
                spec.max_consumers = Some(0);
                spec.ring_size = Some(count);
                self.create_queue(spec, false)?;
            }
        }
        self.deploy_divert(
            &DivertDeployment {
                name: names.divert.clone(),
                routing_name: None,
                address: address.to_owned(),
                forwarding_address: names.address.clone(),
                exclusive: false,
                filter: None,
                routing_type: DivertRoutingType::Pass,
                transformer: None,
            },
            &HashMap::new(),
        )?;
        Ok(())
    }

    /// `removeRetroactiveResources`, once the address is removed.
    pub(crate) fn remove_retroactive_resources(&self, address: &str) {
        let names = self.retro_names(address);
        if self.divert(&names.divert).is_some() {
            self.destroy_divert(&names.divert);
        }
        for routing_type in [RoutingType::Anycast, RoutingType::Multicast] {
            let name = names.queue(routing_type);
            if self.queue(name).is_some()
                && let Err(e) = self.delete_queue(name, true, false)
            {
                tracing::warn!("removing the retroactive queue {name}: {e}");
            }
        }
        if self.address_info(&names.address).is_some() {
            self.remove_address(&names.address);
        }
    }

    /// The repository listener of each retroactive address: its ring queues
    /// keep the count its address's settings give (`setRingSize`).
    pub(crate) fn retroactive_settings_changed(&self) {
        let prefix = self.internal_prefix().to_owned();
        let delimiter = self.wildcard().delimiter;
        for info in self.addresses() {
            if !is_retroactive_resource(&prefix, &info.name) {
                continue;
            }
            let Some(address) = served_address(&prefix, delimiter, &info.name) else {
                continue;
            };
            let count = self.settings(address).retroactive_message_count;
            let names = Names::of(&prefix, delimiter, address);
            for routing_type in [RoutingType::Anycast, RoutingType::Multicast] {
                if let Some(q) = self.queue(names.queue(routing_type))
                    && q.config().ring_size != count
                {
                    q.set_ring_size(count);
                }
            }
        }
    }

    /// `copyRetroactiveMessages`: a queue made on a retroactive address is
    /// given the messages its filter accepts from the ring queue of its
    /// routing type (`rerouteMessages`), which keeps them.
    pub(crate) fn copy_retroactive_messages(&self, target: &Queue) {
        let config = target.config();
        if self.settings(&config.address).retroactive_message_count <= 0 {
            return;
        }
        let names = self.retro_names(&config.address);
        let Some(retro) = self.queue(names.queue(config.routing_type)) else {
            return;
        };
        let sel = Selection::filter(target.filter());
        let action = SelectionAction::Reroute {
            target: target.clone(),
        };
        retro.act_later(sel, action);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_artemis_resource_names() {
        let n = Names::of("$.artemis.internal.", '.', "orders");
        assert_eq!(n.address, "$.artemis.internal.orders.address.retro");
        assert_eq!(n.anycast, "$.artemis.internal.orders.queue.anycast.retro");
        assert_eq!(
            n.multicast,
            "$.artemis.internal.orders.queue.multicast.retro"
        );
        assert_eq!(n.divert, "$.artemis.internal.orders.divert.retro");
        assert!(is_retroactive_resource("$.artemis.internal.", &n.divert));
        assert!(!is_retroactive_resource(
            "$.artemis.internal.",
            "$.artemis.internal.x"
        ));
        assert_eq!(
            served_address("$.artemis.internal.", '.', &n.address),
            Some("orders")
        );
    }
}
