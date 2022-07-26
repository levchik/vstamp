use crate::client;
use crate::commands::{Commit, Prepare};
use futures::{future, stream::FuturesUnordered, StreamExt};
use std::error::Error;
use std::fmt;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tracing::{debug, info};

/// These are mostly broadcast commands, since manager makes requests to all replicas.
#[derive(Debug)]
pub enum ManagerCommand {
    BroadcastPrepare {
        command: Prepare,
        resp_tx: Responder<()>,
    },
    BroadcastCommit {
        command: Commit,
    },
}

/// This is convenience type for sending responses back to the caller
type Responder<T> = oneshot::Sender<crate::Result<T>>;

/// This struct is responsible for talking to all replicas and coordinating the requests.
/// Mostly used for broadcasting commands but maybe later for sending requests to specific replicas.
#[derive(Debug)]
pub struct ReplicaManager {
    pub(crate) mpsc_tx: Sender<ManagerCommand>,
}

impl ReplicaManager {
    /// Create a new ReplicaManager, which will send commands to all replicas in replicas_addresses
    ///
    /// MPSC_CHANNEL_BUFFER is the size of the channel used to send commands to the replicas.
    /// This should be large enough to handle large numbers of simultaneous commands.
    /// TODO: make MPSC_CHANNEL_BUFFER configurable.
    pub fn new(replicas_addresses: Vec<String>) -> Self {
        const MPSC_CHANNEL_BUFFER: usize = 16;
        let (mpsc_tx, mut mpsc_rx) = mpsc::channel(MPSC_CHANNEL_BUFFER);

        // TODO: maybe we need to shutdown it manually
        tokio::spawn(async move {
            // After connecting to all replicas, this will hold all the clients for sending cmds.
            let mut clients = Vec::new();

            // Just high enough number to specify leader's client_id
            let client_id: u128 = 999_999_999_999_999_999u128.to_be();

            // We try to connect to all replicas, but after some time we give up and panic.
            // Retry is using exponential backoff, so that we don't spam the replicas.
            // Later this panic propagates up and shuts down the server, since if we can't talk
            // to other replicas it's pointless to keep running.
            // TODO: check for multiple replica connection in parallel
            for replica_address in replicas_addresses.iter() {
                // TODO: Whole retry thing should be configurable.
                const MAX_RETRIES: usize = 5;
                let retry_strategy = ExponentialBackoff::from_millis(10)
                    .map(jitter)
                    .take(MAX_RETRIES);

                info!("Trying to connect to other replicas...");
                info!(
                    "Waiting at most 60 seconds for replicas to be ready..."
                );
                let result = Retry::spawn(retry_strategy, move || {
                    client::connect(replica_address.clone(), client_id)
                })
                .await
                .expect(&*format!(
                    "Failed to connect to replica {}",
                    replica_address
                ));
                clients.push(result);
            }

            // This is the main loop of the manager.
            // It will listen to all commands on the channel and send them to all replicas.
            // It will also listen to all responses from the replicas and send them back
            // to the caller using responder channel if it is present in command.
            info!("Connected to replicas {:?}", replicas_addresses);
            while let Some(cmd) = mpsc_rx.recv().await {
                match cmd {
                    ManagerCommand::BroadcastPrepare { command, resp_tx } => {
                        debug!("Broadcasting prepare command...");
                        let mut send_tasks = Vec::new();
                        for client in clients.iter_mut() {
                            send_tasks.push(
                                client.prepare_send_only(command.clone()),
                            )
                        }
                        debug!(
                            "Sent {} requests, waiting for responses...",
                            send_tasks.len()
                        );
                        let _results = future::join_all(send_tasks).await;

                        // Now we just have to concurrently wait for responses from all replicas.
                        // If we get more than f_size responses,
                        // we can send the response back to the caller.
                        let mut recv_tasks = Vec::new();
                        for client in clients.iter_mut() {
                            recv_tasks.push(client.prepare_read_response())
                        }
                        // Minimum number of responses required for quorum
                        let f_size = recv_tasks.len() / 2;

                        let mut num_responses = 0;
                        let unpin_futs: Vec<_> =
                            recv_tasks.into_iter().map(Box::pin).collect();
                        let mut futs = unpin_futs;

                        while !futs.is_empty() {
                            match future::select_all(futs).await {
                                (Ok(val), _index, remaining) => {
                                    num_responses += 1;
                                    futs = remaining;
                                }
                                (Err(_e), _index, remaining) => {
                                    // TODO: not ignoring all errors
                                    futs = remaining;
                                }
                            }
                            if num_responses >= f_size {
                                debug!("Quorum PREPAREOK responses came back, sending to resp_tx");
                                resp_tx
                                    .send(Ok(()))
                                    .expect("Could not send response");
                                break;
                            }
                        }
                    }
                    ManagerCommand::BroadcastCommit { command } => {
                        debug!("Broadcasting commit command");
                        for client in clients.iter_mut() {
                            client
                                .commit(command.clone())
                                .await
                                .expect("Sending commit failed");
                        }
                    }
                }
            }
        });

        Self { mpsc_tx }
    }
}
