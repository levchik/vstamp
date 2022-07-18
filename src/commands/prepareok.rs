use crate::{Connection, Frame, Replica};
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct PrepareOk {
    pub(crate) view_number: u128,
    pub(crate) op_number: u128,
    pub(crate) replica_number: u8,
}

impl PrepareOk {
    pub fn new(
        view_number: u128,
        op_number: u128,
        replica_number: u8,
    ) -> Self {
        Self {
            view_number,
            op_number,
            replica_number,
        }
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Reply` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        Frame::PrepareOk(self)
    }

    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Replica,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        Ok(())
    }
}
