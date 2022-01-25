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

use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes;
use futures;
use futures::io::BufReader;
use futures::pin_mut;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::Future;
use futures::FutureExt;
use pin_project::pin_project;

use crate::error::Result;

pub type Reader = Box<dyn AsyncRead + Unpin + Send>;

const CAPACITY: usize = 4096;

/// ReaderStream is used to convert a `futures::io::AsyncRead` into a `futures::Stream`.
///
/// Most code inspired by `tokio_util::io::ReaderStream`.
#[pin_project]
pub struct ReaderStream {
    #[pin]
    reader: Option<Reader>,
    buf: bytes::BytesMut,
}

impl ReaderStream {
    pub fn new(r: Reader) -> Self {
        ReaderStream {
            reader: Some(r),
            buf: bytes::BytesMut::new(),
        }
    }
}

impl futures::Stream for ReaderStream {
    type Item = io::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();

        let reader = match this.reader.as_pin_mut() {
            Some(r) => r,
            None => return Poll::Ready(None),
        };

        // We will always use the same underlying buffer, the allocation happens only once.
        if this.buf.is_empty() {
            this.buf.resize(CAPACITY, 0);
        }

        match ready!(reader.poll_read(cx, this.buf)) {
            Err(err) => {
                self.project().reader.set(None);
                Poll::Ready(Some(Err(err)))
            }
            Ok(0) => {
                self.project().reader.set(None);
                Poll::Ready(None)
            }
            Ok(n) => {
                let chunk = this.buf.split_to(n);
                Poll::Ready(Some(Ok(chunk.freeze())))
            }
        }
    }
}

#[pin_project]
pub struct CallbackReader<F: FnMut(usize)> {
    #[pin]
    inner: Reader,
    f: F,
}

impl<F> CallbackReader<F>
where F: FnMut(usize)
{
    /// # TODO
    ///
    /// Mark as dead_code for now, we will use it sooner while implement streams support.
    #[allow(dead_code)]
    pub fn new(r: Reader, f: F) -> Self {
        CallbackReader { inner: r, f }
    }
}

impl<F> futures::AsyncRead for CallbackReader<F>
where F: FnMut(usize)
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut().project();

        let r = this.inner.poll_read(cx, buf);

        if let Poll::Ready(Ok(len)) = r {
            (self.f)(len);
        };

        r
    }
}
/// Create a buffered `SeekableReader`
///
/// ## Note
///
/// We will use 4MB buffer for now to avoid too much repeated requests.
#[allow(dead_code)]
pub async fn new_buffered_seekable_reader<'d, S: 'd>(
    da: crate::DataAccessor<S>,
    key: &str,
) -> Result<BufReader<SeekableReader<'d, S>>>
where
    S: super::Read<S> + super::Stat<S>,
{
    let o = da.stat(key).await?;

    // We will create a BufReader with a large buf so it only read big chunk from underlying storage.
    Ok(BufReader::with_capacity(
        4 * 1024 * 1024, // 4 MiB
        SeekableReader::new(da, key, o.size),
    ))
}

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
pub struct SeekableReader<'d, S: super::Read<S>> {
    da: crate::DataAccessor<S>,
    key: String,
    total: u64,

    pos: u64,
    state: SeekableReaderState<'d>,
}

enum SeekableReaderState<'d> {
    Idle,
    Starting(Pin<Box<dyn Future<Output = Result<Reader>> + Send + 'd>>),
    Reading(Reader),
}

impl<'d, S> SeekableReader<'d, S>
where S: super::Read<S> + 'd
{
    pub fn new(da: crate::DataAccessor<S>, key: &str, total: u64) -> Self {
        SeekableReader {
            da,
            key: key.to_string(),
            total,

            pos: 0,
            state: SeekableReaderState::Idle,
        }
    }
}

impl<'d, S> AsyncRead for SeekableReader<'d, S>
where S: super::Read<S> + 'd
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.state {
                SeekableReaderState::Idle => {
                    let da = self.da.clone();
                    let key = self.key.clone();
                    let pos = self.pos;
                    let length = buf.len() as u64;

                    let f = async move {
                        let mut builder = da.read(key.as_str());

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

impl<'d, S> AsyncSeek for SeekableReader<'d, S>
where S: super::Read<S> + 'd
{
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
