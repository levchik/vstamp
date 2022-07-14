mod reply;
pub use reply::Reply;
mod request;
pub use request::Request;
mod unknown;
use crate::{Connection, Frame, GuardedReplica};
pub use unknown::Unknown;

/// Enumeration of supported commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Request(Request),
    Reply(Reply),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `mini-redis` and
    /// be the array variant.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned, otherwise, `Err` is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // Match the command name, delegating the rest of the parsing to the
        // specific command.
        let cmd = match frame {
            Frame::Request(request) => Ok(Command::Request(request)),
            Frame::Reply(reply) => Ok(Command::Reply(reply)),
            // _ => Err(crate::Error::Protocol("invalid command".into())),
            _ => Err("invalid command"),
        }
        .expect("Unable to parse frame into concrete command");

        // The command has been successfully parsed
        Ok(cmd)
    }

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        self,
        replica: &GuardedReplica,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Request(cmd) => cmd.apply(replica, dst).await,
            Reply(cmd) => cmd.apply(replica, dst).await,
            // Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            // Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }
}
