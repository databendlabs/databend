// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::SelectPlan;
use common_runtime::tokio::macros::support::Pin;
use common_runtime::tokio::macros::support::Poll;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::Stream;
use futures::StreamExt;

use crate::api::CancelAction;
use crate::api::FlightAction;
use crate::clusters::Node;
use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::DatafuseQueryContextRef;

pub struct SelectInterpreter {
    ctx: DatafuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(ctx: DatafuseQueryContextRef, select: SelectPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        // TODO: maybe panic?
        let mut scheduled = Scheduled::new();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        match self.schedule_query(&mut scheduled).await {
            Ok(stream) => Ok(ScheduledStream::create(scheduled, stream, self.ctx.clone())),
            Err(error) => {
                Self::error_handler(scheduled, &self.ctx, timeout).await;
                Err(error)
            }
        }
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }
}

type Scheduled = HashMap<String, Arc<Node>>;

impl SelectInterpreter {
    async fn schedule_query(&self, scheduled: &mut Scheduled) -> Result<SendableDataBlockStream> {
        let optimized_plan = Optimizers::create(self.ctx.clone()).optimize(&self.select.input)?;

        let scheduler = PlanScheduler::try_create(self.ctx.clone())?;
        let scheduled_tasks = scheduler.reschedule(&optimized_plan)?;
        let remote_stage_actions = scheduled_tasks.get_tasks()?;

        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        for (node, action) in remote_stage_actions {
            let mut flight_client = node.get_flight_client(&self.ctx.get_config()).await?;
            let executing_action = flight_client.execute_action(action.clone(), timeout);

            executing_action.await?;
            scheduled.insert(node.name.clone(), node.clone());
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
        in_local_pipeline.execute().await
    }

    async fn error_handler(scheduled: Scheduled, context: &DatafuseQueryContextRef, timeout: u64) {
        let query_id = context.get_id();
        for (_stream_name, scheduled_node) in scheduled {
            match scheduled_node
                .get_flight_client(&context.get_config())
                .await
            {
                Err(cause) => {
                    log::error!(
                        "Cannot cancel action for {}, cause: {}",
                        scheduled_node.name,
                        cause
                    );
                }
                Ok(mut flight_client) => {
                    let cancel_action = Self::cancel_flight_action(query_id.clone());
                    let executing_action = flight_client.execute_action(cancel_action, timeout);
                    if let Err(cause) = executing_action.await {
                        log::error!(
                            "Cannot cancel action for {}, cause:{}",
                            scheduled_node.name,
                            cause
                        );
                    }
                }
            };
        }
    }

    fn cancel_flight_action(query_id: String) -> FlightAction {
        FlightAction::CancelAction(CancelAction { query_id })
    }
}

struct ScheduledStream {
    scheduled: Scheduled,
    is_success: AtomicBool,
    context: DatafuseQueryContextRef,
    inner: SendableDataBlockStream,
}

impl ScheduledStream {
    pub fn create(
        scheduled: Scheduled,
        inner: SendableDataBlockStream,
        context: DatafuseQueryContextRef,
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
        let error_handler = SelectInterpreter::error_handler(scheduled, &self.context, timeout);
        futures::executor::block_on(error_handler);
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
