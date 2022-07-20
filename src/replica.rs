use crate::app::GuardedKVApp;
use crate::commands::Reply;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, info};

/// Errors that change how we process commands,
/// they mean our state is not valid for continuing further processing.
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

/// Errors that related to client requests.
#[derive(Debug)]
pub enum ClientError {
    /// The client request used ID, that is not bigger than in our client table.
    StaleRequestId,
    /// Client sent request, but we are not in NORMAL state.
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

/// This is a record in the client table.
/// First field is the latest request_id that we have seen from the client.
/// Second field is whether that request was executed by up-call to the app.
/// Lastly, the third field is the Reply that the app returned to the client (if any).
#[derive(Debug)]
pub struct ClientTableRecord(u128, bool, Option<Reply>);

/// Client table's internal store.
/// This is a hashmap of client_id to ClientTableRecord.
pub type ReplyTable = HashMap<u128, ClientTableRecord>;

/// The client-table records for each client the number of its most recent request,
/// plus, if the request has been executed, the result sent for that request.
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

    /// Insert a new record into the client table for every new (not seen before) request.
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

    /// For that client, update the record with the Reply, and mark the request as executed.
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

    /// Try to find the latest request for the given client
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

    /// Check if the we've already seen such request from this client.
    ///
    /// If we have, return the reply. If we haven't, return None.
    ///
    /// If we already had any request and new request_id came not bigger than last one,
    /// then we have a stale request and we should return an error.
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
                    // TODO: If it hasn't been executed -> drop it?
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

/// VR uses these sub-protocols that work together to ensure correctness:
///  - Normal case processing of user requests.
///  - View changes to select a new primary.
///  - Recovery of a failed replica so that it can rejoin the group.
///  - Transitioning state to handle reconfigurations of the group.
#[derive(Debug, PartialEq, Eq)]
enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovery,
    Transitioning,
}

/// This is an entry in replica's log.
/// First field is client_id that sent a command.
/// Second field is request_id that was sent by the client.
/// Lastly, the third field is operation in Bytes that clients wish to execute.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaLogEntry(u128, u128, Bytes);

impl ReplicaLogEntry {
    pub(crate) fn new(
        client_id: u128,
        request_id: u128,
        operation: Bytes,
    ) -> Self {
        Self(client_id, request_id, operation)
    }
}

/// This is an array containing op-number entries.
/// The entries contain the requests that have been received so far in their assigned order
#[derive(Debug)]
pub(crate) struct ReplicaLog {
    operations: Vec<ReplicaLogEntry>,
}

impl ReplicaLog {
    /// Adds a new entry with operation to the log.
    pub fn append(
        &mut self,
        client_id: u128,
        request_id: u128,
        operation: Bytes,
    ) {
        let _ = &self
            .operations
            .push(ReplicaLogEntry::new(client_id, request_id, operation));
    }
}

/// The state of the replica:
///  - The replica number. This is the index into the replicas_addresses of ReplicaConfig.
///  - The current view-number, initially 0.
///  - The current status.
///  - The op-number assigned to the most recently received request, initially 0.
///  - The log of operations that client wishes to execute.
///  - The commit-number is the op-number of the most recently committed operation.
///  - The client-table records for each client its most recent request.
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
    /// Gets the index of this replica address in the list of addresses.
    /// This is used to determine the primary replica of the group.
    pub fn get_replica_number(&self) -> u8 {
        self.replicas_addresses
            .iter()
            .position(|address| address == &self.listen_address)
            .unwrap() as u8
    }

    /// Utility function to get the replicas addresses as Vec<String>
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

/// Struct that stores all the info about the replica.
#[derive(Debug)]
pub struct ReplicaInfo {
    status: ReplicaStatus,
    pub(crate) state: ReplicaState,
    config: ReplicaConfig,
}

/// This is the main struct to use throughout the program.
///
/// It wraps the ReplicaInfo struct in Arc<Mutex<>> and provides methods,
/// that take lock and operate on state & status.
///
/// The main reason for this is that we want to make sure that only one thread
/// can access the state at a time and in the meantime we use .await in methods that also want to
/// operate on replicas state.
#[derive(Debug, Clone)]
pub struct Replica {
    info: Arc<Mutex<ReplicaInfo>>,
}

impl Replica {
    /// Initializes the replica with the given config, starting state and various starting numbers.
    pub fn new(replica_config: ReplicaConfig) -> Self {
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

    pub fn get_op_number(&self) -> u128 {
        let info = self.info.lock();
        info.state.op_number
    }

    pub fn get_view_number(&self) -> u128 {
        let info = self.info.lock();
        info.state.view_number
    }

    pub fn get_commit_number(&self) -> u128 {
        let info = self.info.lock();
        info.state.commit_number
    }

    pub fn advance_op_number(&self) -> u128 {
        let mut info = self.info.lock();
        info.state.advance_op_number()
    }

    pub fn advance_commit_number(&self) -> u128 {
        let mut info = self.info.lock();
        info.state.advance_commit_number()
    }

    pub fn get_current_replica_number(&self) -> u8 {
        let info = self.info.lock();
        info.state.replica_number
    }

    pub fn is_primary_and_normal(&self) -> bool {
        let info = self.info.lock();
        info.status == ReplicaStatus::Normal && info.state.replica_number == 0
    }

    pub fn append_to_log(
        &self,
        client_id: u128,
        request_id: u128,
        operation: Bytes,
    ) {
        let mut info = self.info.lock();
        info.state.log.append(client_id, request_id, operation)
    }

    /// Either we're in normal status or return an error.
    pub fn ensure_normal_status(&self) -> Result<(), ClientError> {
        let info = self.info.lock();
        if info.status != ReplicaStatus::Normal {
            Err(ClientError::NotNormalStatus)
        } else {
            Ok(())
        }
    }

    /// Either we're in the same view as incoming request or return an error.
    pub fn ensure_same_view(
        &self,
        view_number: u128,
    ) -> Result<(), ReplicaError> {
        let info = self.info.lock();
        if info.state.view_number > view_number {
            Err(ReplicaError::ViewNumberBehind)
        } else if info.state.view_number < view_number {
            Err(ReplicaError::ViewNumberAhead)
        } else {
            Ok(())
        }
    }

    /// Either our log contains all the previous operations or return an error.
    pub fn ensure_consecutive_op_number(
        &self,
        op_number: u128,
    ) -> Result<(), ReplicaError> {
        let info = self.info.lock();
        let target_last_op_number = op_number - 1;
        if info.state.op_number > target_last_op_number {
            Err(ReplicaError::OpNumberBehind)
        } else if info.state.op_number < target_last_op_number {
            Err(ReplicaError::OpNumberAhead)
        } else {
            Ok(())
        }
    }

    /// When a replica learns of a commit, it ensures it had executed all earlier operations.
    /// Then it executes the operation by performing the up-call to the service code, increments
    /// its commit-number.
    ///
    /// Replicas process log in order! So we iterate until all operations up until the commit-number
    /// are executed, thus ensuring that the commit-number is the last operation executed.
    ///
    /// In each iteration we take operation from log, take lock on app & execute,
    /// then advance commit-number and update client-table.
    pub fn process_up_to_commit(
        &self,
        app: &GuardedKVApp,
        commit_number: u128,
    ) {
        debug!("Processing up to commit number {}", commit_number);
        let mut info = self.info.lock();
        let mut current_commit_number = info.state.commit_number;
        while current_commit_number < commit_number {
            let operation = info.state.log.operations
                [current_commit_number as usize]
                .clone();
            let response = app.lock().apply(operation.2);
            let reply =
                Reply::new(info.state.view_number, operation.1, response);
            current_commit_number = info.state.advance_commit_number();
            // The server also updates the client’s
            // entry in the client-table to contain the result.
            info.state.client_table.update_with_reply(
                &operation.0,
                &operation.1,
                &reply,
            );
        }
    }

    pub fn check_for_existing_reply(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Result<Option<Reply>, ClientError> {
        let info = self.info.lock();
        info.state
            .client_table
            .check_for_existing_reply(&client_id, &request_id)
    }

    pub fn insert_to_client_table(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Option<ClientTableRecord> {
        let mut info = self.info.lock();
        info.state.client_table.insert(&client_id, &request_id)
    }

    pub fn update_client_table(
        &self,
        &client_id: &u128,
        &request_id: &u128,
        reply: &Reply,
    ) {
        let mut info = self.info.lock();
        info.state.client_table.update_with_reply(
            &client_id,
            &request_id,
            &reply,
        )
    }
}
