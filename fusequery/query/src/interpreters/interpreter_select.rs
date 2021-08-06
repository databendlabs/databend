// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::{HashSet, HashMap};
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::plan_scheduler::{PlanScheduler, Tasks};
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;
use crate::clusters::Node;
use crate::api::{FlightAction, CancelAction};
use common_runtime::tokio::macros::support::{Future, Pin, Poll};
use futures::{Stream, StreamExt};
use common_datablocks::DataBlock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Context;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, select: SelectPlan) -> Result<InterpreterPtr> {
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
            Ok(stream) => {
                Ok(ScheduledStream::create(scheduled, stream, self.ctx.clone()))
            },
            Err(error) => {
                Self::error_handler(scheduled, timeout, self.ctx.get_id()).await;
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
            let mut flight_client = node.get_flight_client().await?;
            let executing_action = flight_client.execute_action(action.clone(), timeout);

            executing_action.await?;
            scheduled.insert(node.name.clone(), node.clone());
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
        in_local_pipeline.execute().await
    }

    async fn error_handler(scheduled: Scheduled, timeout: u64, query_id: String) {
        for (_stream_name, scheduled_node) in scheduled {
            let mut flight_client = scheduled_node.get_flight_client().await;

            match flight_client {
                Err(cause) => {
                    log::error!(
                        "Cannot cancel action for {}, cause: {}",
                        scheduled_node.name, cause
                    );
                }
                Ok(mut flight_client) => {
                    let cancel_action = Self::cancel_flight_action(query_id.clone());
                    let executing_action = flight_client.execute_action(cancel_action, timeout);
                    if let Err(cause) = executing_action.await {
                        log::error!(
                            "Cannot cancel action for {}, cause:{}",
                            scheduled_node.name, cause
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
    complete: AtomicBool,
    scheduled: Scheduled,
    context: FuseQueryContextRef,
    inner: SendableDataBlockStream,
}

impl ScheduledStream {
    pub fn create(
        scheduled: Scheduled,
        inner: SendableDataBlockStream,
        context: FuseQueryContextRef,
    ) -> SendableDataBlockStream {
        Box::pin(ScheduledStream {
            inner,
            scheduled,
            context,
            complete: AtomicBool::new(false),
        })
    }

    fn cancel_scheduled_action(&self) -> Result<()> {
        let query_id = self.context.get_id();
        let scheduled = self.scheduled.clone();
        let timeout = self.context.get_settings().get_flight_client_timeout()?;
        let error_handler = SelectInterpreter::error_handler(scheduled, timeout, query_id);
        futures::executor::block_on(error_handler);
        Ok(())
    }
}

impl Drop for ScheduledStream {
    fn drop(&mut self) {
        if !self.complete.load(Ordering::Relaxed) {
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
                self.complete.store(true, Ordering::Relaxed);
                None
            },
            other => other
        })
    }
}
