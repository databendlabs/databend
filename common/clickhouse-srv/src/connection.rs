// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Cursor;
use std::sync::Arc;

use bytes::Buf;
use bytes::BytesMut;
use chrono_tz::Tz;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;

use crate::binary::Encoder;
use crate::binary::Parser;
use crate::errors::Error;
use crate::errors::Result;
use crate::protocols::ExceptionResponse;
use crate::protocols::Packet;
use crate::protocols::SERVER_END_OF_STREAM;
use crate::types::Block;
use crate::types::Progress;
use crate::CHContext;
use crate::ClickHouseSession;

/// Send and receive `Packet` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    pub buffer: BytesMut,

    stream: BufWriter<TcpStream>,
    pub session: Arc<dyn ClickHouseSession>,

    // The buffer for reading frames.
    tz: Tz,
    with_stack_trace: bool,
    compress: bool,

    pub client_addr: String,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(
        stream: TcpStream,
        session: Arc<dyn ClickHouseSession>,
        timezone: String,
    ) -> Result<Connection> {
        let tz: Tz = timezone.parse()?;
        let client_addr = stream.peer_addr()?.to_string();
        Ok(Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
            session,
            tz,
            with_stack_trace: false,
            compress: true,
            client_addr,
        })
    }

    /// Read a single `Packet` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_packet`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_packet(&mut self, ctx: &mut CHContext) -> crate::Result<Option<Packet>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_packet(ctx)? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_packet(&mut self, ctx: &mut CHContext) -> crate::Result<Option<Packet>> {
        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);
        let mut parser = Parser::new(&mut buf, self.tz);

        let hello = ctx.hello.clone();
        let packet = parser.parse_packet(&hello, self.compress);

        match packet {
            Ok(packet) => {
                if let Packet::Query(ref query) = &packet {
                    self.compress = query.compression > 0
                }
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Packet::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;
                buf.set_position(0);
                self.buffer.advance(len);
                // Return the parsed frame to the caller.
                Ok(Some(packet))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(err) if err.is_would_block() => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e),
        }
    }

    pub async fn write_block(&mut self, block: &Block) -> Result<()> {
        let mut encoder = Encoder::new();
        block.send_server_data(&mut encoder, self.compress);
        self.stream.write_all(&encoder.get_buffer()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_progress(&mut self, progress: Progress, client_revision: u64) -> Result<()> {
        let mut encoder = Encoder::new();
        progress.write(&mut encoder, client_revision);
        self.stream.write_all(&encoder.get_buffer()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_end_of_stream(&mut self) -> Result<()> {
        let mut encoder = Encoder::new();
        encoder.uvarint(SERVER_END_OF_STREAM);
        self.write_bytes(encoder.get_buffer()).await?;
        Ok(())
    }

    pub async fn write_error(&mut self, err: &Error) -> Result<()> {
        let mut encoder = Encoder::new();
        ExceptionResponse::write(&mut encoder, err, self.with_stack_trace);

        self.stream.write_all(&encoder.get_buffer()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
