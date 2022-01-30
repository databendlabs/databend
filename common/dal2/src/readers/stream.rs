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
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::ready;
use futures::AsyncRead;
use pin_project::pin_project;

use crate::Reader;

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
