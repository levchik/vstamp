mod reply;

pub use reply::Reply;
use tokio::sync::mpsc::Sender;
use tracing::debug;

mod request;
pub use request::Request;
mod prepare;
pub use prepare::Prepare;
mod prepareok;
pub use prepareok::PrepareOk;
mod commit;
pub use commit::Commit;

use crate::app::GuardedKVApp;
use crate::manager::ManagerCommand;
use crate::{Connection, Frame, Replica};

/// Enumeration of supported commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Request(Request),
    Reply(Reply),
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a command supported by `vstamp`
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
            Frame::Prepare(prepare) => Ok(Command::Prepare(prepare)),
            Frame::PrepareOk(prepare_ok) => Ok(Command::PrepareOk(prepare_ok)),
            Frame::Commit(commit) => Ok(Command::Commit(commit)),
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
        replica: &Replica,
        dst: &mut Connection,
        manager_sender: &Sender<ManagerCommand>,
        app: &GuardedKVApp,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Request(cmd) => cmd.apply(replica, dst, manager_sender, app).await,
            Reply(cmd) => cmd.apply(replica, dst).await,
            Prepare(cmd) => cmd.apply(replica, dst, app).await,
            PrepareOk(cmd) => cmd.apply(replica, dst).await,
            Commit(cmd) => cmd.apply(replica, dst, app).await,
        }
    }
}
