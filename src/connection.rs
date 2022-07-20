use crate::protocol::Error::Incomplete;
use crate::protocol::Frame;
use crate::Result;
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, info};

/// Connection state.
/// This allocates read/write buffers to perform protocol frame parsing.
#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// New Connection with a reference to the underlying TCP stream.
    ///
    /// Reasonable defaults should be used for the buffer size. TODO: make it configurable.
    pub fn new(stream: TcpStream) -> Self {
        const BUFFER_SIZE: usize = 1024;
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
    /// Read a frame from the connection.
    ///
    /// Returns `None` if EOF is reached
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If
            // enough data has been buffered, the frame is
            // returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame.
            // Attempt to read more data from the socket.
            //
            // On success, the number of bytes is returned. `0`
            // indicates "end of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be
                // a clean shutdown, there should be no data in the
                // read buffer. If there is, this means that the
                // peer closed the socket while sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Write a frame to the connection.
    /// This writes the full frame to the socket, waiting if necessary.
    ///
    /// We don't raise an error here because we don't want to fail the whole thread.
    /// TODO: implement timeouts with ability to act from caller, it should decide what to do next
    pub(crate) async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> io::Result<()> {
        let frame_bytes = frame.unparse().unwrap();
        self.stream.write_all(&frame_bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// From our buffer try to parse bytes as a full Frame
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        // Create the `T: Buf` type.
        let mut buf = Cursor::new(&self.buffer[..]);

        // Check whether a full frame is available
        match Frame::check(&mut buf) {
            Ok(_) => {
                // Get the byte length of the frame
                let len = buf.position() as usize;

                // Reset the internal cursor for the
                // call to `parse`.
                buf.set_position(0);

                // Parse the frame
                let frame = Frame::parse(&mut buf)?;

                // Discard the frame from the buffer
                self.buffer.advance(len);

                // Return the frame to the caller.
                Ok(Some(frame))
            }
            // Not enough data has been buffered
            Err(Incomplete) => Ok(None),
            // An error was encountered
            Err(e) => Err(e.into()),
        }
    }
}
