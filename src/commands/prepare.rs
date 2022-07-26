use crate::app::GuardedKVApp;
use crate::commands::PrepareOk;
use crate::replica::ReplicaError;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tracing::field::debug;
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

    /// Replicas only process normal protocol messages containing a view-number
    /// that matches the view-number they know.
    /// If the sender is behind, the receiver drops the message.
    /// If the sender is ahead, the replica performs a state transfer.
    #[instrument(skip(self, replica, dst))]
    pub(crate) async fn apply(
        &self,
        replica: &Replica,
        dst: &mut Connection,
        app: &GuardedKVApp,
    ) -> crate::Result<()> {
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
                /*
                Backups process PREPARE messages in order: a
                backup won’t accept a prepare with op-number n
                until it has entries for all earlier requests in its log.

                When a backup receives a PREPARE message, it
                waits until it has entries in its log for all earlier requests
                (doing state transfer if necessary to get the missing information).
                */

                // TODO: should check full no-gaps, or just checking last op-number is sufficient?
                match replica.ensure_consecutive_op_number(self.op_number) {
                    Err(e) => match e {
                        ReplicaError::OpNumberBehind => {
                            debug!("Replica is ahead, dropping Prepare");
                            return Ok(());
                        }
                        ReplicaError::OpNumberAhead => {
                            debug!("Replica is behind, drop & performing state transfer");
                            return Ok(());
                        }
                        _ => {
                            panic!("Unexpected replica error when checking op number: {:?}", e)
                        }
                    },
                    Ok(_) => {
                        replica.advance_op_number();
                        replica.append_to_log(
                            self.client_id,
                            self.request_id,
                            self.operation.clone(),
                        );
                        replica.insert_to_client_table(
                            &self.client_id,
                            &self.request_id,
                        );

                        let current_commit_number =
                            replica.get_commit_number();
                        if self.commit_number > current_commit_number {
                            replica
                                .process_up_to_commit(app, self.commit_number);
                        } else {
                            // Replica received a PREPARE message with a commit-number that is
                            // less than or equal to its current commit-number.
                            // Most often that means that primary has not committed this request.
                            // It might be a duplicate, or it might mean that requester was offline
                            // for quite a while, and it got behind.
                            debug!("Replica has nothing to apply, commit is equal or less than current commit");
                        }
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
            }
        }
    }
}
