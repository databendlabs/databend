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

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;

use common_base::tokio::macros::support::Pin;
use common_base::tokio::macros::support::Poll;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_meta_types::NodeInfo;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::Stream;
use futures::StreamExt;

use crate::interpreters::plan_schedulers;
use crate::sessions::QueryContext;

pub type Scheduled = HashMap<String, Arc<NodeInfo>>;

pub struct ScheduledStream {
    scheduled: Scheduled,
    is_success: AtomicBool,
    ctx: Arc<QueryContext>,
    inner: SendableDataBlockStream,
}

impl ScheduledStream {
    pub fn create(
        ctx: Arc<QueryContext>,
        scheduled: Scheduled,
        inner: SendableDataBlockStream,
    ) -> SendableDataBlockStream {
        Box::pin(ScheduledStream {
            ctx,
            inner,
            scheduled,
            is_success: AtomicBool::new(false),
        })
    }

    fn cancel_scheduled_action(&self) -> Result<()> {
        let scheduled = self.scheduled.clone();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        let handler = plan_schedulers::handle_error(&self.ctx, scheduled, timeout);
        futures::executor::block_on(handler);
        Ok(())
    }
}

impl Drop for ScheduledStream {
    fn drop(&mut self) {
        if !self.is_success.load(Ordering::Relaxed) {
            if let Err(cause) = self.cancel_scheduled_action() {
                tracing::error!("Cannot cancel action, cause: {:?}", cause);
            }
        }
    }
}

impl Stream for ScheduledStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map(|x| match x {
            None => {
                self.is_success.store(true, Ordering::Relaxed);
                None
            }
            other => other,
        })
    }
}
