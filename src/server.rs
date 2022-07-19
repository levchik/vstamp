use std::borrow::BorrowMut;
use crate::{Command, Connection, Replica, Shutdown};
use crate::app::{GuardedKVApp, KVApp};
use crate::commands::Commit;
use crate::manager::{ManagerCommand, ReplicaManager};
use crate::replica::{ReplicaConfig, ReplicaDropGuard};
use std::future::Future;
use std::sync::{Arc, Mutex};
use futures::stream;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, instrument};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// These are under wrapper around an `Arc`. This enables to be cloned and
    /// passed into the per connection state (`Handler`).
    app: GuardedKVApp,
    replica_holder: ReplicaDropGuard,

    /// This is a single point of entry for communication between the replicas.
    manager: ReplicaManager,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    /// Used as part of ensuring that replicas commit their log even if no client msgs being sent
    commit_timer_tx: mpsc::Sender<()>,
}

/// Per-connection handler
#[derive(Debug)]
struct Handler {
    replica: Replica,
    app: GuardedKVApp,
    /// Single mpsc channel will be created on initialization. It is used to
    /// send messages to ReplicaManager that handles all replica-to-replica communication.
    manager_mpsc_tx: mpsc::Sender<ManagerCommand>,

    /// The TCP connection decorated with the protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    /// Used as part of the graceful shutdown process to wait for client
    _shutdown_complete: mpsc::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for timer off
    commit_timer_tx: mpsc::Sender<()>,
}

/// Maximum number of concurrent connections the server will accept.
///
/// When this limit is reached, the server will stop accepting connections until
/// an active connection terminates.
///
/// A real application will want to make this value configurable, but for this
/// example, it is hard coded.
const MAX_CONNECTIONS: usize = 250;

/// Run the server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(
    app: KVApp,
    replica_config: ReplicaConfig,
    listener: TcpListener,
    shutdown: impl Future,
) -> crate::Result<()> {
    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Create timer that sends periodic COMMIT messages to all replicas
    // Timer gets reset if any client sends us any request since we do PREPARE
    let (commit_timer_tx, mut commit_timer_rx) = mpsc::channel(1);

    let backup_addresses = replica_config.get_backup_addresses();

    let replica_holder = ReplicaDropGuard::new(replica_config);
    let replica = replica_holder.replica();
    let manager = ReplicaManager::new(backup_addresses);
    let manager_tx = manager.mpsc_tx.clone();

    // Initialize the listener state
    let mut listener = Listener {
        listener,
        app: Arc::new(Mutex::new(app)),
        replica_holder: replica_holder,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        manager: manager,
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
        commit_timer_tx
    };

    /*
    If the primary does not receive a new client request in a timely way, it
    instead informs the backups of the latest commit by sending them a COMMIT message.
    */
    let timer = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(2));
        interval.tick().await;
        loop {
            let _ = tokio::select! {
                r = interval.tick() => {
                    // If this is PRIMARY replica & in NORMAL status
                    if replica.is_primary_and_normal() {
                        debug!("Sending COMMIT to replicas");
                        let _ = manager_tx.send(ManagerCommand::BroadcastCommit {
                            command: Commit {
                                view_number: replica.get_view_number(),
                                commit_number: replica.get_commit_number(),
                            }
                        });
                    }
                },
                s = commit_timer_rx.recv() => {
                    // The client gave us request, so we can reset the timer
                    interval.reset();
                }
            };
        }
    });
    // Concurrently run the listener and listen for the `shutdown` signal. The
    // listener task runs until an error is encountered, so under normal
    // circumstances, this `select!` statement runs until the `shutdown` signal
    // is received.
    tokio::select! {
        res = listener.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the listener is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        commit_timer_tx,
        ..
    } = listener;
    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);
    // Drop timer
    drop(commit_timer_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;

    // TODO: drop mpsx for every client and oneshots here?

    Ok(())
}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> crate::Result<()> {
        info!("Accepting inbound connections...");
        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the semaphore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            self.limit_connections.acquire().await?.forget();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                // Internally, these are `Arc`, so a clone only increments the ref count.
                app: self.app.clone(),
                replica: self.replica_holder.replica(),

                // Use another mpsc to send a message to the
                // backup manager, it will take care of broadcasting and waiting for quorum
                manager_mpsc_tx: self.manager.mpsc_tx.clone(),

                // Initialize the connection state. This allocates read/write
                // buffers to perform protocol frame parsing.
                connection: Connection::new(socket),

                // The connection state needs a handle to the max connections
                // semaphore. When the handler is done processing the
                // connection, a permit is added back to the semaphore.
                limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),

                // Notifies the receiver half once all clones are
                // dropped.
                commit_timer_tx: self.commit_timer_tx.clone(),
            };

            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => {
                    // If we got a frame from a client, it's ok not to send COMMIT to replicas
                    let _ = self.commit_timer_tx.send(()).await?;
                    res?
                },
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            info!(frame = ?frame, "received request frame");

            // Convert the frame into a command struct. This returns an
            // error if the frame is not a valid command or it is an
            // unsupported command.
            let cmd = Command::from_frame(frame)?;

            info!(?cmd);

            // Perform the work needed to apply the command. This may mutate the
            // database state as a result.
            //
            // The connection is passed into the apply function which allows the
            // command to write response frames directly to the connection.
            cmd.apply(
                &self.replica,
                &mut self.connection,
                &self.manager_mpsc_tx,
                &self.app,
            )
            .await?;
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guarantee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.
        self.limit_connections.add_permits(1);
    }
}
