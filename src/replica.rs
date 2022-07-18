use crate::commands::Reply;
use bytes::Bytes;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum ReplicaError {
    ViewNumberBehind,
    ViewNumberAhead,
    OpNumberBehind,
    OpNumberAhead,
}

impl Error for ReplicaError {}

impl fmt::Display for ReplicaError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplicaError::ViewNumberBehind => {
                "View number in request is behind current view of this replica".fmt(fmt)
            }
            ReplicaError::ViewNumberAhead => {
                "View number in request is ahead of current view of this replica".fmt(fmt)
            }
            ReplicaError::OpNumberBehind => {
                "Op number in request is behind current log of this replica".fmt(fmt)
            }
            ReplicaError::OpNumberAhead => {
                "Op number in request is ahead of current log of this replica".fmt(fmt)
            }
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    StaleRequestId,
    NotNormalStatus,
    Other(crate::Error),
}

impl From<String> for ClientError {
    fn from(src: String) -> ClientError {
        ClientError::Other(src.into())
    }
}

impl From<&str> for ClientError {
    fn from(src: &str) -> ClientError {
        src.to_string().into()
    }
}

impl Error for ClientError {}

impl fmt::Display for ClientError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClientError::StaleRequestId => {
                "Stale request_id in client message".fmt(fmt)
            }
            ClientError::NotNormalStatus => {
                "Not normal status for replica".fmt(fmt)
            }
            ClientError::Other(err) => err.fmt(fmt),
        }
    }
}

/// A wrapper around a `Replica` instance. This exists to allow orderly cleanup
/// of the `Replica` by signalling the background purge task to shut down when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct ReplicaDropGuard {
    /// The `Replica` instance that will be shut down when this `ReplicaHolder` struct
    /// is dropped.
    replica: Replica,
}

impl ReplicaDropGuard {
    /// Create a new `ReplicaHolder`, wrapping a `Replica` instance. When this is dropped
    /// the `Replica`'s purge task will be shut down.
    pub(crate) fn new(replica_config: ReplicaConfig) -> ReplicaDropGuard {
        ReplicaDropGuard {
            replica: Replica::new(replica_config),
        }
    }

    /// Get the shared replica state. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn replica(&self) -> Replica {
        self.replica.clone()
    }
}

/// New types for making for easier specifications

#[derive(Debug)]
pub struct ClientTableRecord(u128, bool, Option<Reply>);

pub type ReplyTable = HashMap<u128, ClientTableRecord>;

/*
The client-table records for each client the number of its most recent request,
plus, if the request has been executed, the result sent for that request.

So, the HashMap is of following structure:

    <client_id>: (<latest_request_id>, <is_request_executed>, Reply)
*/
#[derive(Debug)]
pub struct ClientTable {
    pub table: ReplyTable,
}

impl ClientTable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Option<ClientTableRecord> {
        self.table.insert(
            client_id,
            ClientTableRecord {
                0: request_id,
                1: false,
                2: None,
            },
        )
    }

    pub fn update_with_reply(
        &mut self,
        &client_id: &u128,
        &request_id: &u128,
        reply: &Reply,
    ) {
        *self.table.get_mut(&client_id).unwrap() = ClientTableRecord {
            0: request_id,
            1: true,
            2: Some(reply.clone()),
        }
    }

    pub fn get_client_table_record(
        &self,
        &client_id: &u128,
    ) -> Option<&ClientTableRecord> {
        let maybe_reply = self.table.get(&client_id);
        return if maybe_reply.is_some() {
            Some(maybe_reply.unwrap())
        } else {
            // There are no replies for this client just yet
            None
        };
    }

    pub fn check_for_existing_reply(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Result<Option<Reply>, ClientError> {
        let maybe_client_table_record =
            self.get_client_table_record(&client_id);
        if maybe_client_table_record.is_some() {
            let client_table_record = maybe_client_table_record.unwrap();
            let latest_request_id = client_table_record.0;
            if request_id > latest_request_id {
                // This is new request, proceed with execution
                Ok(None)
            } else if request_id == latest_request_id && client_table_record.1
            {
                // This is latest request arrived again:
                match &client_table_record.2 {
                    // If it's already been executed -> resend Frame
                    Some(reply) => Ok(Some(reply.clone())),
                    // TODO: If it isn't been executed -> drop it?
                    None => Err(ClientError::StaleRequestId),
                }
            } else {
                // This is some old request, drop it
                Err(ClientError::StaleRequestId)
            }
        } else {
            // There are no replies for this client just yet, proceed with execution
            Ok(None)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovery,
    Transitioning,
}

#[derive(Debug)]
pub(crate) struct ReplicaLog {
    operations: Vec<Bytes>,
}

impl ReplicaLog {
    pub fn append(&mut self, operation: Bytes) {
        let _ = &self.operations.push(operation);
    }
}

#[derive(Debug)]
pub struct ReplicaState {
    pub(crate) replica_number: u8,
    pub(crate) view_number: u128,
    pub(crate) op_number: u128,
    pub(crate) commit_number: u128,
    pub(crate) log: ReplicaLog,
    pub(crate) client_table: ClientTable,
}

impl ReplicaState {
    pub fn advance_op_number(&mut self) -> u128 {
        self.op_number += 1;
        self.op_number
    }

    pub fn advance_commit_number(&mut self) -> u128 {
        self.commit_number += 1;
        self.commit_number
    }
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

    pub fn get_backup_addresses(&self) -> Vec<String> {
        let mut backup_addresses = Vec::new();
        for address in &self.replicas_addresses {
            if address != &self.listen_address {
                backup_addresses.push(address.clone());
            }
        }
        backup_addresses
    }
}

#[derive(Debug)]
pub struct ReplicaInfo {
    status: ReplicaStatus,
    pub(crate) state: ReplicaState,
    config: ReplicaConfig,
}

#[derive(Debug, Clone)]
pub struct Replica {
    info: Arc<Mutex<ReplicaInfo>>,
}

impl Replica {
    pub fn new(replica_config: ReplicaConfig) -> Self {
        // Consider using parking_lot::Mutex as a faster alternative to std::sync::Mutex.
        let info = Arc::new(Mutex::new(ReplicaInfo {
            status: ReplicaStatus::Normal,
            state: ReplicaState {
                replica_number: replica_config.get_replica_number(),
                view_number: 0,
                op_number: 0,
                commit_number: 0,
                log: ReplicaLog {
                    operations: Vec::new(),
                },
                client_table: ClientTable::new(),
            },
            config: replica_config,
        }));
        Self { info }
    }

    pub fn get_view_number(&self) -> u128 {
        let info = self.info.lock().unwrap();
        info.state.view_number
    }

    pub fn get_op_number(&self) -> u128 {
        let info = self.info.lock().unwrap();
        info.state.op_number
    }

    pub fn get_commit_number(&self) -> u128 {
        let info = self.info.lock().unwrap();
        info.state.commit_number
    }

    pub fn advance_op_number(&self) -> u128 {
        let mut info = self.info.lock().unwrap();
        info.state.advance_op_number()
    }

    pub fn advance_commit_number(&self) -> u128 {
        let mut info = self.info.lock().unwrap();
        info.state.advance_commit_number()
    }

    pub fn get_current_replica_number(&self) -> u8 {
        let info = self.info.lock().unwrap();
        info.state.replica_number
    }

    pub fn append_to_log(&self, operation: Bytes) {
        let mut info = self.info.lock().unwrap();
        info.state.log.append(operation)
    }

    pub fn ensure_normal_status(&self) -> Result<(), ClientError> {
        let info = self.info.lock().unwrap();
        if info.status != ReplicaStatus::Normal {
            Err(ClientError::NotNormalStatus)
        } else {
            Ok(())
        }
    }

    pub fn ensure_same_view(
        &self,
        view_number: u128,
    ) -> Result<(), ReplicaError> {
        let info = self.info.lock().unwrap();
        if info.state.view_number > view_number {
            Err(ReplicaError::ViewNumberBehind)
        } else if info.state.view_number < view_number {
            Err(ReplicaError::ViewNumberAhead)
        } else {
            Ok(())
        }
    }

    pub fn ensure_consecutive_op_number(
        &self,
        op_number: u128,
    ) -> Result<(), ReplicaError> {
        let info = self.info.lock().unwrap();
        let target_last_op_number = op_number - 1;
        if info.state.op_number > target_last_op_number {
            Err(ReplicaError::OpNumberBehind)
        } else if info.state.op_number < target_last_op_number {
            Err(ReplicaError::OpNumberAhead)
        } else {
            Ok(())
        }
    }

    pub fn check_for_existing_reply(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Result<Option<Reply>, ClientError> {
        let info = self.info.lock().unwrap();
        info.state
            .client_table
            .check_for_existing_reply(&client_id, &request_id)
    }

    pub fn insert_to_client_table(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Option<ClientTableRecord> {
        let mut info = self.info.lock().unwrap();
        info.state.client_table.insert(&client_id, &request_id)
    }

    pub fn update_client_table(
        &self,
        &client_id: &u128,
        &request_id: &u128,
        reply: &Reply,
    ) {
        let mut info = self.info.lock().unwrap();
        info.state.client_table.update_with_reply(
            &client_id,
            &request_id,
            &reply,
        )
    }
}
