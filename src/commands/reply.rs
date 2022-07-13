use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct Reply {
    pub view_number: u128,
    pub request_id: u128,
    pub response: Bytes,
}

impl Reply {
    pub fn new(view_number: u128, request_id: u128, response: Bytes) -> Self {
        Self {
            view_number,
            request_id,
            response,
        }
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Reply` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        Frame::Reply(self)
    }

    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Arc<Mutex<Replica>>,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        // send the request to the other replicas
        // let response = replica.process_cmd(self.clone()).await;
        Ok(())
    }
}
