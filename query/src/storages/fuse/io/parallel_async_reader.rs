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

use std::io::IoSliceMut;
use std::io::SeekFrom;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use common_base::tokio::sync::OwnedSemaphorePermit;
use futures::AsyncRead;
use futures::AsyncSeek;

pub struct ParallelAsyncReader<T: ?Sized> {
    _permit: OwnedSemaphorePermit,
    value: T,
}

impl<T: Sized> ParallelAsyncReader<T> {
    pub fn new(permit: OwnedSemaphorePermit, value: T) -> Self {
        ParallelAsyncReader {
            _permit: permit,
            value,
        }
    }
}

impl<T: ?Sized> Deref for ParallelAsyncReader<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ?Sized> DerefMut for ParallelAsyncReader<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: ?Sized + AsyncRead + Unpin> AsyncRead for ParallelAsyncReader<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut **self).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut **self).poll_read_vectored(cx, bufs)
    }
}

impl<T: ?Sized + AsyncSeek + Unpin> AsyncSeek for ParallelAsyncReader<T> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut **self).poll_seek(cx, pos)
    }
}
