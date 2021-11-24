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
use common_planners::PlanNode;
use common_streams::SendableDataBlockStream;
use futures::Stream;
use futures::StreamExt;

use crate::api::CancelAction;
use crate::api::FlightAction;
use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::DatabendQueryContextRef;

pub type Scheduled = HashMap<String, Arc<NodeInfo>>;

pub async fn schedule_query(
    ctx: &DatabendQueryContextRef,
    plan: &PlanNode,
) -> Result<SendableDataBlockStream> {
    let scheduler = PlanScheduler::try_create(ctx.clone())?;
    let scheduled_tasks = scheduler.reschedule(plan)?;
    let remote_stage_actions = scheduled_tasks.get_tasks()?;

    let config = ctx.get_config();
    let cluster = ctx.get_cluster();
    let timeout = ctx.get_settings().get_flight_client_timeout()?;
    let mut scheduled = Scheduled::new();
    for (node, action) in remote_stage_actions {
        let mut flight_client = cluster.create_node_conn(&node.id, &config).await?;
        let executing_action = flight_client.execute_action(action.clone(), timeout);

        executing_action.await?;
        scheduled.insert(node.id.clone(), node.clone());
    }

    let pipeline_builder = PipelineBuilder::create(ctx.clone());
    let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
    match in_local_pipeline.execute().await {
        Ok(stream) => Ok(ScheduledStream::create(scheduled, stream, ctx.clone())),
        Err(error) => {
            error_handler(scheduled, ctx, timeout).await;
            Err(error)
        }
    }
}

pub struct ScheduledStream {
    scheduled: Scheduled,
    is_success: AtomicBool,
    context: DatabendQueryContextRef,
    inner: SendableDataBlockStream,
}

impl ScheduledStream {
    pub fn create(
        scheduled: Scheduled,
        inner: SendableDataBlockStream,
        context: DatabendQueryContextRef,
    ) -> SendableDataBlockStream {
        Box::pin(ScheduledStream {
            inner,
            scheduled,
            context,
            is_success: AtomicBool::new(false),
        })
    }

    fn cancel_scheduled_action(&self) -> Result<()> {
        let scheduled = self.scheduled.clone();
        let timeout = self.context.get_settings().get_flight_client_timeout()?;
        let handler = error_handler(scheduled, &self.context, timeout);
        futures::executor::block_on(handler);
        Ok(())
    }
}

impl Drop for ScheduledStream {
    fn drop(&mut self) {
        if !self.is_success.load(Ordering::Relaxed) {
            if let Err(cause) = self.cancel_scheduled_action() {
                log::error!("Cannot cancel action, cause: {:?}", cause);
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

async fn error_handler(scheduled: Scheduled, context: &DatabendQueryContextRef, timeout: u64) {
    let query_id = context.get_id();
    let config = context.get_config();
    let cluster = context.get_cluster();

    for (_stream_name, scheduled_node) in scheduled {
        match cluster.create_node_conn(&scheduled_node.id, &config).await {
            Err(cause) => {
                log::error!(
                    "Cannot cancel action for {}, cause: {}",
                    scheduled_node.id,
                    cause
                );
            }
            Ok(mut flight_client) => {
                let cancel_action = FlightAction::CancelAction(CancelAction {
                    query_id: query_id.clone(),
                });
                let executing_action = flight_client.execute_action(cancel_action, timeout);
                if let Err(cause) = executing_action.await {
                    log::error!(
                        "Cannot cancel action for {}, cause:{}",
                        scheduled_node.id,
                        cause
                    );
                }
            }
        };
    }
}
