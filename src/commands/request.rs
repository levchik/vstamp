use crate::app::GuardedKVApp;
use crate::commands::{Prepare, Reply};
use crate::manager::ManagerCommand;
use crate::{Connection, Frame, Replica};
use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct Request {
    pub(crate) client_id: u128,
    pub(crate) request_id: u128,
    pub(crate) operation: Bytes,
}

impl Request {
    pub fn new(client_id: u128, request_id: u128, operation: Bytes) -> Self {
        Self {
            client_id,
            request_id,
            operation,
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        Frame::Request(self)
    }

    #[instrument(skip(self, replica, dst, app))]
    pub(crate) async fn apply(
        &self,
        replica: &Replica,
        dst: &mut Connection,
        manager_sender: &Sender<ManagerCommand>,
        app: &GuardedKVApp,
    ) -> crate::Result<()> {
        /*
        Replicas participate in processing of client requests only when their status is normal.
        This constraint is critical for correctness!
        */
        if replica.ensure_normal_status().is_err() {
            // Drop request to tell client, that it needs to either try later or find new leader
            return Ok(());
        }

        /*
        When the primary receives the request, it compares the request-number in the request
        with the information in the client table.

        If the request-number isn’t bigger than the information in the table it drops the request,
        but it will re-send the response if the request is the most recent one from this client
        and it has already been executed.
        */
        // TODO: check if we are primary now?

        let maybe_stored_reply = replica
            .check_for_existing_reply(&self.client_id, &self.request_id);

        match maybe_stored_reply {
            Ok(maybe_reply) => match maybe_reply {
                None => {
                    // The primary advances op-number, adds the request
                    // to the end of the log, and updates the information
                    // for this client in the client-table to contain the new
                    // request number.

                    // TODO: these all could be under just one lock
                    replica.advance_op_number();
                    replica.append_to_log(self.client_id, self.request_id,self.operation.clone());
                    replica.insert_to_client_table(
                        &self.client_id,
                        &self.request_id,
                    );

                    // Then it sends a PREPARE message to the other replicas.
                    let (resp_tx, resp_rx) = oneshot::channel();
                    manager_sender
                        .send(ManagerCommand::BroadcastPrepare {
                            command: Prepare {
                                view_number: replica.get_view_number(),
                                op_number: replica.get_op_number(),
                                commit_number: replica.get_commit_number(),
                                client_id: self.client_id,
                                request_id: self.request_id,
                                operation: self.operation.clone(),
                            },
                            resp_tx,
                        })
                        .await
                        .expect("Couldn't broadcast backup command");

                    // The primary waits for <quorum> PREPAREOK messages
                    // from different backups; at this point it considers
                    // the operation (and all earlier ones) to be committed.

                    // Await the backup manager response.
                    // It completes future only after quorum of PREPAREOK messages are received.
                    let res = resp_rx.await;
                    debug!("GOT (Prepare) = {:?}", res);
                    match res {
                        Ok(_) => {
                            // Then, after it has executed all earlier operations
                            // (those assigned smaller op-numbers), the primary
                            // executes the operation by making an up-call to the
                            // service code, and increments its commit-number.

                            // NOTE: Advancing the commit-num must be done in strict order,
                            //       and it means that all previous operations have been committed.
                            let response = app
                                .lock()
                                .unwrap()
                                .apply(self.operation.clone());
                            replica.advance_commit_number();

                            // Then it sends a REPLY message to the client.
                            let reply = Reply::new(
                                // TODO: can we use view_number calculated before?
                                replica.get_view_number(),
                                self.request_id,
                                response,
                            );
                            debug!(?reply);
                            dst.write_frame(&reply.clone().into_frame())
                                .await?;

                            // The primary also updates the client’s
                            // entry in the client-table to contain the result.
                            replica.update_client_table(
                                &self.client_id,
                                &self.request_id,
                                &reply,
                            );
                            Ok(())
                        }
                        Err(_) => {
                            // If the primary didn’t receive enough PREPAREOK messages,
                            // it drops the request and re-sends the response.
                            Ok(())
                        }
                    }
                }
                Some(reply) => {
                    debug!("Sending stored reply");
                    dst.write_frame(&Frame::Reply(reply)).await?;
                    Ok(())
                }
            },
            // This is some old request, drop it
            Err(_) => Ok(()),
        }
    }
}
