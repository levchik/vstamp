use std::ops::Deref;
use crate::commands::Reply;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct Request {
    pub(crate) client_id: u128,
    pub(crate) request_id: u128,
    pub(crate) operation: Bytes,
    pub(crate) response: Option<Bytes>,
}

impl Request {
    pub fn new(client_id: u128, request_id: u128, operation: Bytes) -> Self {
        Self {
            client_id,
            request_id,
            operation,
            response: None,
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        Frame::Request(self)
    }

    fn get_stored_reply(&self, replica: &Arc<Mutex<Replica>>) -> Option<Frame> {
        let mut replica = replica.clone();
        let x = replica.lock().unwrap()
            .state
            .client_table
            .get_reply_frame(&self.client_id, &self.request_id);
        x
    }

    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Arc<Mutex<Replica>>,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        /*
        When the primary receives the request, it compares the request-number in the request
        with the information in the client table.

        If the request-number isn’t bigger than the information in the table it drops the request,
        but it will re-send the response if the request is the most recent one from this client and it
        has already been executed.
        */

        let maybe_stored_reply = self.get_stored_reply(replica);
        if maybe_stored_reply.is_some() {
            debug!("Sending stored reply");
            dst.write_frame(&maybe_stored_reply.unwrap()).await?;
            return Ok(());
        } else {
            let reply = Reply::new(
                self.client_id,
                self.request_id,
                self.operation.clone(),
            );
            debug!(?reply);
            dst.write_frame(&reply.into_frame()).await?;
            Ok(())
        }
    }
}
