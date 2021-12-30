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
use std::time::Instant;

use common_base::tokio::io::SeekFrom;

use crate::DalContext;
use crate::InputStream;

/// A interceptor for input stream.
pub struct InputStreamInterceptor {
    ctx: Arc<DalContext>,
    inner: InputStream,
    last_pending: Option<Instant>,
}

impl InputStreamInterceptor {
    pub fn new(ctx: Arc<DalContext>, inner: InputStream) -> Self {
        Self {
            ctx,
            inner,
            last_pending: None,
        }
    }
}

impl futures::AsyncRead for InputStreamInterceptor {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let start = match self.last_pending {
            None => Instant::now(),
            Some(t) => t,
        };
        let r = Pin::new(&mut self.inner).poll_read(ctx, buf);
        match r {
            Poll::Ready(Ok(len)) => {
                self.ctx.inc_read_bytes(len as usize);
                self.last_pending = None;
            }
            Poll::Ready(Err(_)) => self.last_pending = None,
            Poll::Pending => self.last_pending = Some(start),
        }
        self.ctx.inc_cost_ms(start.elapsed().as_millis() as usize);
        r
    }
}

impl futures::AsyncSeek for InputStreamInterceptor {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let start = match self.last_pending {
            None => Instant::now(),
            Some(t) => t,
        };
        let r = Pin::new(&mut self.inner).poll_seek(cx, pos);

        match r {
            Poll::Ready(Ok(_)) => {
                self.last_pending = None;
            }
            Poll::Ready(Err(_)) => self.last_pending = None,
            Poll::Pending => {
                let _duration = start.elapsed();
                self.last_pending = Some(start)
            }
        }
        self.ctx.inc_cost_ms(start.elapsed().as_millis() as usize);
        r
    }
}
