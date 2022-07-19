use crate::commands::{Commit, Prepare, PrepareOk, Reply, Request};
use crate::{Connection, Frame};

use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, instrument};

/// Established connection with a server.
/// Requests are issued using the various methods of `Client`.
#[derive(Debug)]
pub struct Client {
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,
    id: u128,
    request_id: u128,
    /*
    TODO:
    The client-side proxy has state:
     - It records the configuration
     - What it believes is the current view-number

    Each message sent to the client informs it of the current view-number;
    this allows the client to track the primary.
    */
}

/// Establish a connection with the server located at `addr`.
///
/// `addr` may be any type that can be asynchronously converted to a
/// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
/// trait is the Tokio version and not the `std` version.
///
/// # Examples
///
/// ```no_run
/// use vstamp::client;
///
/// #[tokio::main]
/// async fn main() {
///     let client = match client::connect("127.0.0.1:4627", 1).await {
///         Ok(client) => client,
///         Err(_) => panic!("failed to establish connection"),
///     };
/// # drop(client);
/// }
/// ```
///
pub async fn connect<T: ToSocketAddrs>(
    addr: T,
    client_id: u128,
) -> crate::Result<Client> {
    // The `addr` argument is passed directly to `TcpStream::connect`. This
    // performs any asynchronous DNS lookup and attempts to establish the TCP
    // connection. An error at either step returns an error, which is then
    // bubbled up to the caller of connect.
    let socket = TcpStream::connect(addr).await?;

    // Initialize the connection state. This allocates read/write buffers to
    // perform protocol frame parsing.
    let connection = Connection::new(socket);

    Ok(Client {
        connection,
        id: client_id,
        request_id: 0,
    })
}

impl Client {
    /// Request a command to the server
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use bytes::Bytes;
    /// use vstamp::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("127.0.0.1:4627", 1).await.unwrap();
    ///
    ///     let val = client.request(Bytes::from_static("foo".as_ref())).await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn request(&mut self, op: Bytes) -> crate::Result<Reply> {
        /*
        TODO:
            A client is allowed to have just one outstanding request at a time.
        */
        self.request_id += 1;
        let frame = Request::new(self.id, self.request_id, op).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;

        // TODO:
        // If a client doesn't receive a timely response, it re-sends the request to all replicas.
        // This way if the group has moved to a later view, its message will reach the new primary.
        // Backups ignore client requests; only the primary processes them.
        match self.read_response().await? {
            Frame::Reply(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    pub async fn prepare(
        &mut self,
        command: Prepare,
    ) -> crate::Result<PrepareOk> {
        let frame = command.into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;

        // Wait for the response from the server
        match self.read_response().await? {
            Frame::PrepareOk(prepare_ok) => Ok(prepare_ok),
            frame => Err(frame.to_error()),
        }
    }

    pub async fn commit(&mut self, command: Commit) -> crate::Result<()> {
        let frame = command.into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;
        Ok(())
    }

    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(response = ?response);

        match response {
            // Error frames are converted to `Err`
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // Receiving `None` here indicates the server has closed the
                // connection without sending a frame. This is unexpected and is
                // represented as a "connection reset by peer" error.
                let err = Error::new(
                    ErrorKind::ConnectionReset,
                    "connection reset by server",
                );

                Err(err.into())
            }
        }
    }
}
