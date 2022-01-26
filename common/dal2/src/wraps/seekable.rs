// Copyright 2022 Datafuse Labs.
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
use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::pin_mut;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::FutureExt;

use crate::error::Result;
use crate::Reader;

/// If we already know a file's total size, we can implement Seek for it.
///
/// - Every time we call `read` we will send a new http request to fetch data from cloud storage like s3.
/// - Every time we call `seek` we will update the `pos` field just in memory.
/// # NOTE
///
/// It's better to use SeekableReader as an inner reader inside BufReader.
///
/// # TODO
///
/// We need use update the metrics.
pub struct SeekableReader {
    op: crate::Operator,
    key: String,
    total: u64,

    pos: u64,
    state: SeekableReaderState,
}

enum SeekableReaderState {
    Idle,
    Starting(Pin<Box<dyn Future<Output = Result<Reader>> + Send>>),
    Reading(Reader),
}

impl SeekableReader {
    pub fn new(da: crate::Operator, key: &str, total: u64) -> Self {
        SeekableReader {
            op: da,
            key: key.to_string(),
            total,

            pos: 0,
            state: SeekableReaderState::Idle,
        }
    }
}

impl AsyncRead for SeekableReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.state {
                SeekableReaderState::Idle => {
                    let op = self.op.clone();
                    let key = self.key.clone();
                    let pos = self.pos;
                    let length = buf.len() as u64;

                    let f = async move {
                        let mut builder = op.read(key.as_str());

                        let r = builder.offset(pos).size(length).run().await?;

                        Ok(r)
                    };

                    self.state = SeekableReaderState::Starting(f.boxed());
                }
                SeekableReaderState::Starting(ref mut fut) => {
                    let r = ready!(fut.as_mut().poll(cx)).map_err(io::Error::other)?;

                    self.state = SeekableReaderState::Reading(r);
                }
                SeekableReaderState::Reading(ref mut r) => {
                    pin_mut!(r);

                    let n = ready!(r.poll_read(cx, buf))?;
                    self.pos += n as u64;
                    return Poll::Ready(Ok(n));
                }
            }
        }
    }
}

impl AsyncSeek for SeekableReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        off: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        match off {
            SeekFrom::Start(off) => {
                self.pos = off;
            }
            SeekFrom::End(off) => {
                self.pos = self.total.checked_add_signed(off).expect("overflow");
            }
            SeekFrom::Current(off) => {
                self.pos = self.pos.checked_add_signed(off).expect("overflow");
            }
        }

        self.state = SeekableReaderState::Idle;

        Poll::Ready(Ok(self.pos))
    }
}
