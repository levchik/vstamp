use crate::protocol::ClientTable;
use bytes::Bytes;

#[derive(Debug)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovery,
    Transitioning,
}

#[derive(Debug)]
struct ReplicaLog(Bytes);

#[derive(Debug)]
pub struct ReplicaState {
    replica_number: u8,
    view_number: u128,
    op_number: u128,
    commit_number: u128,
    log: ReplicaLog,
    pub(crate) client_table: ClientTable,
}

#[derive(Debug)]
pub struct ReplicaConfig {
    pub listen_address: String,
    pub replicas_addresses: Vec<String>,
}

impl ReplicaConfig {
    pub fn get_replica_number(&self) -> u8 {
        // It is the index of our address in the list of addresses
        self.replicas_addresses
            .iter()
            .position(|address| address == &self.listen_address)
            .unwrap() as u8
    }
}

#[derive(Debug)]
pub struct Replica {
    status: ReplicaStatus,
    pub(crate) state: ReplicaState,
    config: ReplicaConfig,
}

impl Replica {
    pub fn new(replica_config: ReplicaConfig) -> Self {
        Replica {
            status: ReplicaStatus::Normal,
            state: ReplicaState {
                replica_number: replica_config.get_replica_number(),
                view_number: 0,
                op_number: 0,
                commit_number: 0,
                log: ReplicaLog(Bytes::new()),
                client_table: ClientTable::new(),
            },
            config: replica_config,
        }
    }
}
