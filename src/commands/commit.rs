use crate::app::GuardedKVApp;
use crate::commands::PrepareOk;
use crate::replica::ReplicaError;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct Commit {
    pub(crate) view_number: u128,
    pub(crate) commit_number: u128,
}

impl Commit {
    pub fn new(view_number: u128, commit_number: u128) -> Self {
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
        Replicas only process normal protocol messages containing a view-number
        that matches the view-number they know.
        If the sender is behind, the receiver drops the message.
        If the sender is ahead, the replica performs a state transfer.
        */
        match replica.ensure_same_view(self.view_number) {
            Err(e) => {
                match e {
                    ReplicaError::ViewNumberBehind => {
                        debug!("Replica is ahead, dropping Prepare");
                        return Ok(());
                    }
                    ReplicaError::ViewNumberAhead => {
                        debug!("Replica is behind, drop & performing state transfer");
                        return Ok(());
                    }
                    _ => {
                        panic!("Unexpected replica error when checking view number: {:?}", e)
                    }
                }
            }
            Ok(_) => {
                let current_commit_number = replica.get_commit_number();
                if self.commit_number > current_commit_number {
                    replica.process_up_to_commit(app, self.commit_number);
                } else {
                    // Replica received a PREPARE message with a commit-number that is
                    // less than or equal to its current commit-number.
                    // Most often that means that primary has not committed this request.
                    // It might be a duplicate, or it might mean that requester was offline
                    // for quite a while, and it got behind.
                    debug!("Replica has nothing to apply, commit is equal or less than current commit");
                }
                Ok(())
            }
        }
    }
}
