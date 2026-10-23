//! Configuration reload (`ActiveMQServerImpl.reloadConfigurationFile` and
//! `deployReloadableConfigFromConfiguration`), and the address and queue
//! deployment that startup shares with it.
//!
//! A reload swaps the security and address settings, reconciles the
//! diverts with the configured ones, undeploys the addresses and queues
//! that left the configuration where `config-delete-addresses` or
//! `config-delete-queues` is `FORCE`, and deploys the configured addresses
//! and queues, updating the queues that exist, then the acceptors and the
//! protocols' services (AMQP's broker connections).

use std::collections::HashMap;

use saranga_config as cfg;

use crate::postoffice::{DivertDeployment, PostOffice, TransformerFactory};
use crate::{AddressDeployment, Broker};

/// What a reload changes, taken from a freshly read configuration.
pub struct Reload {
    pub address_settings: cfg::HierarchicalRepository<cfg::AddressSettings>,
    pub security_settings: cfg::HierarchicalRepository<cfg::SecuritySetting>,
    /// The core's `<addresses>`: those of a legacy `<jms>` are not read
    /// again, as `reloadConfigurationFile` reads only the core.
    pub addresses: Vec<AddressDeployment>,
    /// The deprecated `<queues>`.
    pub queues: Vec<crate::QueueSpec>,
    pub diverts: Vec<DivertDeployment>,
    pub bridges: Vec<cfg::BridgeConfig>,
    /// The connectors (and discovery groups) bridges may name.
    pub bridge_targets: crate::bridge::Targets,
    /// What the broker properties read reported (`setStatus`).
    pub status: saranga_buffer::json::Json,
    /// The realms of `jaasConfigs`.
    pub jaas_configs: saranga_security::manager::Realms,
    /// The acceptors, when whoever read the configuration can make them.
    pub acceptors: Option<crate::acceptors::AcceptorPlan>,
    /// `<broker-connections>`, for the protocols' services.
    pub broker_connections: Vec<cfg::AmqpBrokerConnectConfig>,
}

impl Reload {
    /// The reloadable parts of a parsed `broker.xml` (with its broker
    /// properties applied).
    pub fn from_file_config(config: &cfg::BrokerConfig) -> cfg::Result<Self> {
        Ok(Self {
            address_settings: config.address_settings_repository()?,
            security_settings: config.security_repository()?,
            addresses: config
                .addresses
                .iter()
                .map(crate::config::deployment)
                .collect(),
            queues: crate::config::legacy_queues(config),
            diverts: config.diverts.iter().map(crate::config::divert).collect(),
            bridges: config.bridges.clone(),
            bridge_targets: crate::bridge::Targets::from_file(config),
            status: config.status.clone(),
            jaas_configs: saranga_security::manager::realms(&config.jaas_configs),
            acceptors: None,
            broker_connections: config.broker_connections.clone(),
        })
    }
}

fn reloading(what: &str) {
    tracing::info!("AMQ221056: Reloading configuration: {what}");
}

impl Broker {
    /// `updateReloadableConfigurationFrom` and
    /// `deployReloadableConfigFromConfiguration`. An acceptor that fails to
    /// start fails the reload, before `AMQ221087`.
    pub async fn reload(self: &std::sync::Arc<Self>, reload: Reload) -> Result<(), String> {
        // parseProperties and setJaasConfigs, before anything deploys.
        if let Some(jaas) = self.security.as_ref().and_then(|s| s.manager().as_jaas()) {
            jaas.reload_jaas_configs(reload.jaas_configs);
        }
        saranga_buffer::status::update(
            saranga_buffer::status::CONFIGURATION_COMPONENT,
            reload.status,
        );
        reloading("security settings");
        if let Some(security) = &self.security {
            security.set_repository(reload.security_settings);
        }

        reloading("address settings");
        self.post_office.swap_settings(reload.address_settings);

        reloading("diverts");
        reconcile_diverts(&self.post_office, &reload.diverts, &self.transformers);

        reloading("addresses");
        undeploy_not_configured(&self.post_office, &reload.addresses);
        deploy_addresses(&self.post_office, &reload.addresses);
        deploy_queue_specs(&self.post_office, &reload.queues);

        self.bridges.set_targets(reload.bridge_targets);
        self.reload_bridges(&reload.bridges).await;

        // Saranga has no connection routers to update.
        reloading("connection routers");

        reloading("protocol services");
        if let Some(plan) = reload.acceptors {
            self.update_acceptors(plan).await?;
        }
        for p in self.protocols.clone() {
            p.update_services(self, &reload.broker_connections)?;
        }

        tracing::info!("AMQ221087: Configuration reload completed");
        Ok(())
    }
}

/// `deployAddressesFromConfiguration`: each address is created or given
/// its configured routing types, with those it has kept until its queues
/// are deployed; each queue is created, or updated if it exists on that
/// address.
pub(crate) fn deploy_addresses(post_office: &PostOffice, addresses: &[AddressDeployment]) {
    let mut deployed = Vec::new();
    for a in addresses {
        tracing::info!(
            "AMQ221080: Deploying address {} supporting {}",
            a.name,
            a.routing_types.describe()
        );
        // `new AddressInfo(address, tobe.getRoutingType())`: the first of
        // the configured routing types alone (in EnumSet order, MULTICAST
        // first), with the current ones added. With none configured it has
        // no set: `getRoutingTypes()` gives a new empty one, so the current
        // routing types added to it are lost, as in Artemis.
        let first = [crate::RoutingType::Multicast, crate::RoutingType::Anycast]
            .into_iter()
            .find(|t| a.routing_types.contains(*t));
        let mut merged = crate::RoutingTypes::default();
        if let Some(first) = first {
            merged.add(first);
            if let Some(current) = post_office.address_info(&a.name) {
                for t in [crate::RoutingType::Anycast, crate::RoutingType::Multicast] {
                    if current.routing_types.contains(t) {
                        merged.add(t);
                    }
                }
            }
        }
        let result = post_office
            .add_or_update_address(&a.name, merged, false, false)
            .and_then(|_| {
                deployed.extend(deploy_queue_specs(post_office, &a.queues).into_iter().map(
                    |(spec, id)| DeployedQueue {
                        address: a.name.clone(),
                        spec,
                        id,
                    },
                ));
                post_office.add_or_update_address(&a.name, a.routing_types, false, false)
            });
        if let Err(e) = result {
            tracing::warn!("AMQ222274: Failed to deploy address {}: {e}", a.name);
        }
    }
    post_office.set_deployed_queues(deployed);
}

/// What deploying a configured queue wrote into its `QueueConfiguration`,
/// which the configuration the broker runs on holds and
/// `exportConfigAsProperties` exports.
#[derive(Debug, Clone)]
pub struct DeployedQueue {
    /// The address configuration that lists it.
    pub address: String,
    /// Its configuration as the deployment left it: the address settings'
    /// defaults applied, and what `createQueue` or an update sets.
    pub spec: crate::QueueSpec,
    /// The journal ID `createQueue` gave it: `None` for a queue that
    /// existed (after a restart) or was not made.
    pub id: Option<i64>,
}

impl DeployedQueue {
    /// Writes what the deployment set into the queue's configuration.
    pub fn write_into(&self, q: &mut saranga_config::configuration::QueueConfiguration) {
        let s = &self.spec;
        q.address = Some(s.address.to_string());
        q.routing_type = s.routing_type.map(|t| t.name().to_owned());
        q.durable = Some(s.durable);
        q.max_consumers = s.max_consumers;
        q.purge_on_no_consumers = s.purge_on_no_consumers;
        q.exclusive = s.exclusive;
        q.last_value = s.last_value;
        q.last_value_key = s.last_value_key.clone();
        q.non_destructive = s.non_destructive;
        q.consumers_before_dispatch = s.consumers_before_dispatch;
        q.delay_before_dispatch = s.delay_before_dispatch;
        q.group_rebalance = s.group_rebalance;
        q.group_rebalance_pause_dispatch = s.group_rebalance_pause_dispatch;
        q.group_buckets = s.group_buckets;
        q.group_first_key = s.group_first_key.clone();
        q.ring_size = s.ring_size;
        q.enabled = s.enabled;
        q.auto_create_address = s.auto_create_address;
        q.auto_delete = s.auto_delete;
        q.auto_delete_delay = s.auto_delete_delay;
        q.auto_delete_message_count = s.auto_delete_message_count;
        q.configuration_managed = s.configuration_managed;
        q.id = self.id;
    }

    /// Writes each deployed queue into the configuration's queue of that
    /// name under its address.
    pub fn write_all(
        deployed: &[DeployedQueue],
        config: &mut saranga_config::configuration::Configuration,
    ) {
        for d in deployed {
            let queue = config
                .address_configurations
                .iter_mut()
                .filter(|a| a.name.as_deref() == Some(d.address.as_str()))
                .flat_map(|a| a.queue_configs.iter_mut())
                .find(|q| *q.name == *d.spec.name);
            if let Some(q) = queue {
                d.write_into(q);
            }
        }
    }
}

/// `deployQueuesFromListQueueConfiguration`: each queue is created on its
/// address, made as needed, or updated if it exists there. Gives each
/// queue's configuration as the deployment left it, as Artemis's deployment
/// leaves the `QueueConfiguration` it was given, and the ID of one it made.
pub(crate) fn deploy_queue_specs(
    post_office: &PostOffice,
    queues: &[crate::QueueSpec],
) -> Vec<(crate::QueueSpec, Option<i64>)> {
    let mut deployed = Vec::new();
    for spec in queues {
        // applyDynamicDefaults, then setAutoCreateAddress(true).
        let mut left = spec.clone();
        left.apply_dynamic_defaults(&post_office.settings(&spec.address));
        left.auto_create_address = Some(true);
        let mut id = None;
        let routing_type = spec.routing_type.map(|t| t.name()).unwrap_or("MULTICAST");
        tracing::info!(
            "AMQ221003: Deploying {routing_type} queue {} on address {}",
            spec.name,
            spec.address
        );
        let existing = post_office.queue(&spec.name);
        let result = match existing {
            Some(q) if q.config().address == spec.address => {
                left.configuration_managed = true;
                left.apply_static_defaults();
                post_office.update_queue(spec.clone()).map(|_| ())
            }
            Some(q) => {
                // The queue exists on a different address: createQueue
                // refuses it, after the setters it is called with.
                left.temporary = false;
                left.auto_created = false;
                left.configuration_managed = true;
                tracing::warn!(
                    "AMQ229019: Queue {} already exists on address {}",
                    spec.name,
                    q.config().address
                );
                Ok(())
            }
            None => {
                left.temporary = false;
                left.auto_created = false;
                left.configuration_managed = true;
                let mut spec = spec.clone();
                spec.auto_create_address = Some(true);
                post_office.create_queue(spec, false).map(|q| {
                    id = Some(q.id());
                })
            }
        };
        if let Err(e) = result {
            tracing::warn!("AMQ222275: Failed to deploy queue {}: {e}", spec.name);
        }
        deployed.push((left, id));
    }
    deployed
}

/// `undeployAddressesAndQueueNotInConfiguration`.
pub(crate) fn undeploy_not_configured(post_office: &PostOffice, addresses: &[AddressDeployment]) {
    let configured: HashMap<&str, &AddressDeployment> =
        addresses.iter().map(|a| (a.name.as_str(), a)).collect();
    for info in post_office.addresses() {
        let settings = post_office.settings(&info.name);
        let queues: Vec<_> = post_office
            .queues()
            .into_iter()
            .filter(|q| q.config().address == info.name)
            .collect();
        if !configured.contains_key(&*info.name)
            && !info.auto_created
            && settings.config_delete_addresses
        {
            for q in queues {
                tracing::info!("AMQ224077: Undeploying queue {}", q.name());
                if let Err(e) = post_office.delete_queue(q.name(), false, false) {
                    tracing::info!(
                        "AMQ224103: unable to undeploy queue {} : reason {e}",
                        info.name
                    );
                }
            }
            tracing::info!("AMQ224076: Undeploying address {}", info.name);
            if !post_office.remove_address(&info.name) {
                tracing::info!(
                    "AMQ224102: unable to undeploy address {} : reason it still has bindings",
                    info.name
                );
            }
        } else if settings.config_delete_queues {
            for q in queues
                .into_iter()
                .filter(|q| q.config().configuration_managed)
            {
                // A queue stays if some configured address lists it on this
                // address.
                let kept = addresses.iter().any(|a| {
                    a.queues
                        .iter()
                        .any(|s| s.name == *q.name() && s.address == info.name)
                });
                if !kept {
                    tracing::info!("AMQ224077: Undeploying queue {}", q.name());
                    if let Err(e) = post_office.delete_queue(q.name(), false, false) {
                        tracing::info!(
                            "AMQ224103: unable to undeploy queue {} : reason {e}",
                            info.name
                        );
                    }
                }
            }
        }
    }
}

/// The divert part of `deployReloadableConfigFromConfiguration`: new
/// diverts are deployed, changed ones replaced and those no longer
/// configured destroyed.
fn reconcile_diverts(
    post_office: &PostOffice,
    diverts: &[DivertDeployment],
    transformers: &HashMap<String, TransformerFactory>,
) {
    let mut to_remove: Vec<_> = post_office
        .diverts()
        .into_iter()
        .map(|d| d.name.clone())
        .collect();
    for d in diverts {
        to_remove.retain(|n| **n != *d.name);
        let mut d = d.clone();
        if let Some(existing) = post_office.divert(&d.name) {
            // `updateDivert` keeps the divert's routing name.
            if d.routing_name.as_deref().is_none_or(str::is_empty) {
                d.routing_name = Some(existing.routing_name.to_string());
            }
            post_office.destroy_divert(&d.name);
        }
        if let Err(e) = post_office.deploy_divert_persisted(&d, transformers) {
            tracing::warn!("divert {}: {e}", d.name);
        }
    }
    for name in to_remove {
        post_office.destroy_divert(&name);
    }
}
