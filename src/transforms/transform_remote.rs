// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::planners::PlanNode;
use crate::processors::{EmptyProcessor, IProcessor};
use crate::rpcs::rpc::{ExecuteAction, ExecutePlanAction, FlightClient};

pub struct RemoteTransform {
    job_id: String,
    remote_addr: String,
    plan: PlanNode,
    input: Arc<dyn IProcessor>,
}

impl RemoteTransform {
    pub fn try_create(
        job_id: String,
        remote_addr: String,
        plan: PlanNode,
    ) -> FuseQueryResult<Self> {
        Ok(Self {
            job_id,
            remote_addr,
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
