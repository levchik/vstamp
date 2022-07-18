use crate::client;
use crate::commands::Prepare;
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
    Emtpy,
}

type Responder<T> = oneshot::Sender<crate::Result<T>>;

#[derive(Debug)]
pub struct BackupsManager {
    manager: JoinHandle<()>,
    pub(crate) mpsc_tx: Sender<ManagerCommand>,
}

impl BackupsManager {
    pub fn new(replicas_addresses: Vec<String>) -> Self {
        const MPSC_CHANNEL_BUFFER: usize = 16;
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(MPSC_CHANNEL_BUFFER);

        let manager = tokio::spawn(async move {
            let mut clients = Vec::new();
            for replica_address in replicas_addresses.iter() {
                clients.push(
                    client::connect(replica_address.clone()).await.unwrap(),
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
                        while let Some(result) = tasks.next().await {
                            debug!("result arrived: {:?}", result);
                            // Err(CommandError::PrepareOkTimeout)
                            // Validate the result, then
                            // Validate quorum
                            // Return Ok()
                        }
                        debug!("Responses came back, sending to resp_tx");
                        // Ok(())
                        // // TODO: don't ignore errors
                        let _ = resp_tx.send(Ok(()));
                    }
                    ManagerCommand::Emtpy => {}
                }
            }
        });

        Self { mpsc_tx, manager }
        // TODO: on shutdown manager.await.unwrap(); for every backup_client
    }
}
