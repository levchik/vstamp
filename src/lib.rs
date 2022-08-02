extern crate core;
pub mod client;
pub mod commands;
pub use commands::Command;
mod connection;
pub use connection::Connection;
mod protocol;
pub use protocol::Frame;
pub mod replica;
pub use replica::{Replica, ReplicaConfig};
mod shutdown;
use shutdown::Shutdown;
mod app;
pub use app::KVApp;
pub use app::NIL_VALUE;
mod manager;
pub mod server;

/// Error returned by most functions.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for operations.
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;
