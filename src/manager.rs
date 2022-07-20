use crate::client;
use crate::commands::{Commit, Prepare};
use futures::{stream::FuturesUnordered, StreamExt};
use std::error::Error;
use std::fmt;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::debug;

#[derive(Debug)]
pub enum CommandError {
    PrepareOkTimeout,
    Other(crate::Error),
}

impl From<String> for CommandError {
    fn from(src: String) -> CommandError {
        CommandError::Other(src.into())
    }
}

impl From<&str> for CommandError {
    fn from(src: &str) -> CommandError {
        src.to_string().into()
    }
}

impl Error for CommandError {}

impl fmt::Display for CommandError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommandError::PrepareOkTimeout => {
                "Didn't receive PrepareOk response in a timely manner".fmt(fmt)
            }
            CommandError::Other(err) => err.fmt(fmt),
        }
    }
}

#[derive(Debug)]
pub enum ManagerCommand {
    BroadcastPrepare {
        command: Prepare,
        resp_tx: Responder<()>,
    },
    BroadcastCommit {
        command: Commit
    },
    Emtpy,
}

type Responder<T> = oneshot::Sender<crate::Result<T>>;

#[derive(Debug)]
pub struct ReplicaManager {
    manager: JoinHandle<()>,
    pub(crate) mpsc_tx: Sender<ManagerCommand>,
}

impl ReplicaManager {
    pub fn new(replicas_addresses: Vec<String>) -> Self {
        const MPSC_CHANNEL_BUFFER: usize = 16;
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(MPSC_CHANNEL_BUFFER);

        let manager = tokio::spawn(async move {
            let mut clients = Vec::new();
            // Just high enough number to specify leader's client_id
            let client_id: u128 = 999_999_999_999_999_999u128.to_be();
            for replica_address in replicas_addresses.iter() {
                clients.push(
                    client::connect(replica_address.clone(), client_id)
                        .await
                        .unwrap(),
                )
            }
            debug!("Connected to other replicas {:?}", replicas_addresses);
            while let Some(cmd) = mpsc_rx.recv().await {
                match cmd {
                    ManagerCommand::BroadcastPrepare { command, resp_tx } => {
                        let mut tasks = FuturesUnordered::new();
                        for client in clients.iter_mut() {
                            tasks.push(client.prepare(command.clone()))
                        }
                        debug!(
                            "Sent {} requests, waiting for responses",
                            tasks.len()
                        );
                        // TODO: this will change when we implement cluster config change
                        let f_size = tasks.len() / 2;
                        let mut responses_replicas = Vec::new();
                        while let Some(result) = tasks.next().await {
                            // TODO: Err(CommandError::PrepareOkTimeout)
                            debug!("result arrived: {:?}", result);

                            // Validate the result
                            match result {
                                Ok(prepare_ok) => responses_replicas
                                    .push(prepare_ok.replica_number),
                                Err(err) => {
                                    // TODO: handle error
                                    debug!("Error: {:?}", err);
                                    continue;
                                }
                            };

                            // Validate quorum
                            if responses_replicas.len() >= f_size {
                                debug!("Quorum responses came back, sending to resp_tx");
                                let _ = resp_tx.send(Ok(()));
                                // TODO: we don't need to wait for others to conclude that
                                // quorum has been reached, but we need to wait for others to
                                // validate them & possibly tell them that they are failing???
                                break;
                            }
                        }
                    }
                    ManagerCommand::BroadcastCommit { command } => {
                        for client in clients.iter_mut() {
                            client.commit(command.clone()).await.expect("Sending commit failed");
                        }
                    }
                    ManagerCommand::Emtpy => {}
                }
            }
        });

        Self { mpsc_tx, manager }
        // TODO: on shutdown manager.await.unwrap(); for every backup_client
    }
}
