use crate::commands::PrepareOk;
use crate::replica::ReplicaError;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tracing::{debug, instrument};
use crate::app::GuardedKVApp;

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
                        replica.append_to_log(self.operation.clone());
                        replica.insert_to_client_table(
                            &self.client_id,
                            &self.request_id,
                        );

                        //                         if self.commit_number > replica.commit_number {
                        //     replica.set_commit_number(self.commit_number);
                        //     replica.commit_to_log();
                        // } else {
                        //     // Replica received a PREPARE message with a commit-number that is
                        //     // less than or equal to its current commit-number.
                        //     // It might be a duplicate, or it might mean that requester was offline
                        //     // for quite a while, and it got behind.
                        //     // In either case, this is a violation of the protocol, and the
                        //     // replica should drop the message.
                        //     Ok(())
                        // }
                        // for commit in replica.get_commit_number()..self.commit_number {
                        //
                        // }
                        // let response = app
                        //     .lock()
                        //     .unwrap()
                        //     .apply(self.operation.clone());
                        // replica.advance_commit_number();

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
