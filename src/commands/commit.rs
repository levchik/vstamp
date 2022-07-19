use crate::commands::PrepareOk;
use crate::replica::ReplicaError;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tracing::{debug, instrument};
use crate::app::GuardedKVApp;

#[derive(Debug, Clone)]
pub struct Commit {
    pub(crate) view_number: u128,
    pub(crate) commit_number: u128,
}

impl Commit {
    pub fn new(
        view_number: u128,
        commit_number: u128,
    ) -> Self {
        Self {
            view_number,
            commit_number,
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        Frame::Commit(self)
    }

    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Replica,
        dst: &mut Connection,
        app: &GuardedKVApp,
    ) -> crate::Result<()> {
        /*
        When a backup learns of a commit, it waits until it has the request in its log (which may require
        state transfer) and until it has executed all earlier
        operations. Then it executes the operation by performing the up-call to the service code, increments
        its commit-number, updates the client’s entry in the
        client-table, but does not send the reply to the client.
        */
        Ok(())
    }
}
