// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_planners::PlanNode;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::processors::{EmptyProcessor, IProcessor};
use crate::rpcs::rpc::{ExecuteAction, ExecutePlanAction, FlightClient};
use crate::sessions::FuseQueryContextRef;

pub struct RemoteTransform {
    job_id: String,
    remote_addr: String,
    pub ctx: FuseQueryContextRef,
    pub plan: PlanNode,
    input: Arc<dyn IProcessor>,
}

impl RemoteTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        job_id: String,
        remote_addr: String,
        plan: PlanNode,
    ) -> FuseQueryResult<Self> {
        Ok(Self {
            job_id,
            remote_addr,
            ctx,
            plan,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for RemoteTransform {
    fn name(&self) -> &str {
        "RemoteTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let mut client = FlightClient::try_create(self.remote_addr.clone()).await?;
        let action = ExecuteAction::ExecutePlan(ExecutePlanAction::create(
            self.job_id.clone(),
            self.plan.clone(),
        ));
        Ok(Box::pin(client.execute(&action).await?))
    }
}
