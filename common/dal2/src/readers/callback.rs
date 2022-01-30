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
use std::pin::Pin;
use std::task::Poll;

use pin_project::pin_project;

use crate::Reader;

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
