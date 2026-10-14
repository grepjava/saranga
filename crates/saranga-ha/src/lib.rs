//! High availability: whole-file locks taken as Java takes them, the
//! pluggable lock managers Artemis's replication coordinates through
//! (`artemis-lockmanager`: ZooKeeper as Curator does it, and files), and
//! the activation sequences of `ActivationSequenceStateMachine`.

pub mod activation;
pub mod curator;
pub mod filelock;
pub mod lockmanager;
pub mod zookeeper;
