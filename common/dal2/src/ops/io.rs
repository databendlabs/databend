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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes;
use futures;
use futures::ready;
use futures::AsyncRead;
use pin_project::pin_project;

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
    type Item = Result<bytes::Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();

        let mut reader = match this.reader.as_pin_mut() {
            Some(r) => r,
            None => return Poll::Ready(None),
        };

        // We will always use the same underlying buffer, the allocation happens only once.
        if this.buf.len() == 0 {
            this.buf.resize(CAPACITY, 0);
        }

        match ready!(reader.as_mut().poll_read(cx, this.buf)) {
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

pub struct CallbackReader {
    inner: Reader,
    f: Box<dyn Fn(usize)>,
}

impl futures::AsyncRead for CallbackReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(len)) = r {
            (self.f)(len);
        };

        r
    }
}

#[cfg(test)]
mod test {
    use futures::io::Cursor;
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn reader_stream() {
        let reader = Box::new(Cursor::new("Hello, world!"));
        let mut s = ReaderStream::new(reader);

        let mut bs = Vec::new();
        while let Some(chunk) = s.next().await {
            bs.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(&bs[..], "Hello, world!".as_bytes());
    }
}
