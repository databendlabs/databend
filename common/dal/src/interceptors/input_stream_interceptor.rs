//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use common_base::tokio::io::SeekFrom;

use crate::DalContext;
use crate::InputStream;

/// A interceptor for input stream.
pub struct InputStreamInterceptor {
    ctx: Arc<DalContext>,
    inner: InputStream,
}

impl InputStreamInterceptor {
    pub fn new(ctx: Arc<DalContext>, inner: InputStream) -> Self {
        Self { ctx, inner }
    }
}

impl futures::AsyncRead for InputStreamInterceptor {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let r = Pin::new(&mut self.inner).poll_read(ctx, buf);
        if let Poll::Ready(Ok(len)) = r {
            self.ctx.inc_read_bytes(len as usize);
        };
        r
    }
}

impl futures::AsyncSeek for InputStreamInterceptor {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.inner).poll_seek(cx, pos)
    }
}
