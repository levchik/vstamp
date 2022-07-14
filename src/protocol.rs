use crate::commands::{Reply, Request};
use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Read};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// New types for making for easier specifications
pub type ReplyTable = HashMap<(u128, u128), Reply>;

#[derive(Debug)]
pub struct ClientTable {
    pub table: ReplyTable,
}

impl ClientTable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn get_reply_frame(
        &self,
        &client_id: &u128,
        &request_id: &u128,
    ) -> Option<Frame> {
        let maybe_reply = self.table.get(&(client_id, request_id));
        return if maybe_reply.is_some() {
            Some(Frame::Reply(maybe_reply.unwrap().clone()))
        } else {
            None
        };
    }
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Frame {
    Request(Request),
    Reply(Reply),
    Prepare {
        view_number: u128,
        op_number: u128,
        commit_number: u128,
        request: Request,
    },
    PrepareOk {
        view_number: u128,
        op_number: u128,
        replica_number: u8,
    },
    Commit {
        view_number: u128,
        commit_number: u128,
    },
    StartViewChange {
        view_number: u128,
        replica_number: u8,
    },
    DoViewChange {
        view_number: u128,
        replica_number: u8,
        log: Vec<Bytes>,
        last_normal_view_number: u128,
        op_number: u128,
        commit_number: u128,
    },
    StartView {
        view_number: u128,
        log: Vec<Bytes>,
        op_number: u128,
        commit_number: u128,
    },
    Recovery {
        replica_number: u8,
        nonce: u128,
    },
    RecoveryResponse {
        replica_number: u8,
        nonce: u128,
        // if this replica is primary, fields below are non-empty
        log: Option<Vec<Bytes>>,
        op_number: Option<u128>,
        commit_number: Option<u128>,
    },
    GetState {
        replica_number: u8,
        view_number: u128,
        op_number: u128,
    },
    NewState {
        view_number: u128,
        op_number: u128,
        log: Vec<Bytes>,
        commit_number: u128,
    },
    Reconfiguration {
        epoch_number: u128,
        client_id: u128,
        request_id: u128,
        replicas_addresses: Vec<String>,
    },
    StartEpoch {
        epoch_number: u128,
        op_number: u128,
        old_replicas_addresses: Vec<String>,
        new_replicas_addresses: Vec<String>,
    },
    EpochStarted {
        epoch_number: u128,
        replica_number: u8,
    },
    CheckEpoch {
        client_id: u128,
        epoch_number: u128,
        request_id: u128,
    },
    Error(String),
}

impl Frame {
    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_decimal(src)?;
                get_decimal(src)?;
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_decimal(src)?;
                get_decimal(src)?;
                get_line(src)?;
                Ok(())
            }
            actual => Err(format!(
                "protocol error; invalid frame type byte `{}`",
                actual
            )
            .into()),
        }
    }

    /// The message has already been validated
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        /*
        Request
        +<client_id>\r\n<request_id>\r\n<operation>\r\n

        Reply
        -<view_number>\r\n<request_id>\r\n<response>\r\n

        Prepare
        ><view_number>\r\n<op_number>\r\n<commit_number>\r\n<request>\r\n

        PrepareOk
        <<view_number>\r\n<op_number>\r\n<replica_number>\r\n

        Commit
        !<view_number>\r\n<commit_number>\r\n

        StartViewChange
        @<view_number>\r\n<replica_number>\r\n

        DoViewChange
        #<view_number>\r\n<replica_number>\r\n<log>\r\n<last_normal_view_number>\r\n<op_number>\r\n<commit_number>\r\n

        StartView
        $<view_number>\r\n<log>\r\n<op_number>\r\n<commit_number>\r\n

        Recovery
        %<replica_number>\r\n<nonce>\r\n

        RecoveryResponse
        &<replica_number>\r\n<nonce>\r\n<log>\r\n<op_number>\r\n<commit_number>\r\n

        GetState
        ^<replica_number>\r\n<view_number>\r\n<op_number>\r\n

        NewState
        *<view_number>\r\n<op_number>\r\n<log>\r\n<commit_number>\r\n

        Reconfiguration
        ~<epoch_number>\r\n<client_id>\r\n<request_id>\r\n<replicas_addresses>\r\n

        StartEpoch
        ;<epoch_number>\r\n<op_number>\r\n<old_replicas_addresses>\r\n<new_replicas_addresses>\r\n

        EpochStarted
        :<epoch_number>\r\n<replica_number>\r\n

        CheckEpoch
        ,<client_id>\r\n<epoch_number>\r\n<request_id>\r\n
         */
        match get_u8(src)? {
            b'+' => {
                let client_id = get_decimal(src)?;
                let request_id = get_decimal(src)?;
                let operation = Bytes::copy_from_slice(get_line(src)?);

                Ok(Frame::Request(Request {
                    client_id,
                    request_id,
                    operation,
                    response: None,
                }))
            }
            b'-' => {
                let view_number = get_decimal(src)?;
                let request_id = get_decimal(src)?;
                let response = Bytes::copy_from_slice(get_line(src)?);

                Ok(Frame::Reply(Reply {
                    view_number,
                    request_id,
                    response,
                }))
            }
            _ => unimplemented!(),
        }
    }

    pub fn unparse(&self) -> Result<Bytes, Error> {
        match self {
            Frame::Request(request) => {
                // +<client_id>\r\n<request_id>\r\n<operation>\r\n
                let mut buf = Vec::new();
                buf.push(b'+');
                buf.extend_from_slice(
                    request.client_id.to_string().as_bytes(),
                );
                buf.push(b'\r');
                buf.push(b'\n');
                buf.extend_from_slice(
                    request.request_id.to_string().as_bytes(),
                );
                buf.push(b'\r');
                buf.push(b'\n');
                buf.extend_from_slice(&request.operation);
                buf.push(b'\r');
                buf.push(b'\n');
                Ok(Bytes::from(buf))
            }
            Frame::Reply(reply) => {
                // -<view_number>\r\n<request_id>\r\n<response>\r\n
                let mut buf = Vec::new();
                buf.push(b'-');
                buf.extend_from_slice(
                    reply.view_number.to_string().as_bytes(),
                );
                buf.push(b'\r');
                buf.push(b'\n');
                buf.extend_from_slice(reply.request_id.to_string().as_bytes());
                buf.push(b'\r');
                buf.push(b'\n');
                buf.extend_from_slice(&reply.response);
                buf.push(b'\r');
                buf.push(b'\n');
                Ok(Bytes::from(buf))
            }
            _ => Ok(Bytes::new()),
        }
    }

    /// Converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Request(request) => write!(
                fmt,
                "Reply: request_id={} view_number={} response={}",
                request.request_id,
                request.client_id,
                str::from_utf8(&*request.operation).unwrap()
            ),
            Frame::Reply(response) => write!(
                fmt,
                "Reply: request_id={} view_number={} response={}",
                response.request_id,
                response.view_number,
                str::from_utf8(&*response.response).unwrap()
            ),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            _ => write!(fmt, "unknown frame"),
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.bytes().next().unwrap()?)
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u128, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u128>(line)
        .ok_or_else(|| "protocol error; invalid frame format".into())
}
