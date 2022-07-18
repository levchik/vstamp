use crate::commands::PrepareOk;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct Prepare {
    pub(crate) view_number: u128,
    pub(crate) op_number: u128,
    pub(crate) commit_number: u128,
    pub(crate) client_id: u128,
    pub(crate) request_id: u128,
    pub(crate) operation: Bytes,
}

impl Prepare {
    pub fn new(
        view_number: u128,
        op_number: u128,
        commit_number: u128,
        client_id: u128,
        request_id: u128,
        operation: Bytes,
    ) -> Self {
        Self {
            view_number,
            op_number,
            commit_number,
            client_id,
            request_id,
            operation,
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        Frame::Prepare(self)
    }

    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Replica,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        let prepare_ok = PrepareOk::new(
            self.view_number,
            self.op_number,
            replica.get_current_replica_number(),
        );
        debug!(?prepare_ok);
        dst.write_frame(&prepare_ok.into_frame()).await?;
        Ok(())
    }
}
