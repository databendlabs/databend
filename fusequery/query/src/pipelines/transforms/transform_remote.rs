// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::{Result, ErrorCodes};
use common_planners::PlanNode;
use common_streams::SendableDataBlockStream;

use crate::api::rpc::FlightClient;
use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;

pub struct RemoteTransform {
    job_id: String,
    remote_addr: String,
    pub ctx: FuseQueryContextRef,
    pub plan: PlanNode,
    input: Arc<dyn IProcessor>
}

impl RemoteTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        job_id: String,
        remote_addr: String,
        plan: PlanNode
    ) -> Result<Self> {
        Ok(Self {
            job_id,
            remote_addr,
            ctx,
            plan,
            input: Arc::new(EmptyProcessor::create())
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for RemoteTransform {
    fn name(&self) -> &str {
        "RemoteTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        async fn execute_impl(remote_addr: &String, job_id: &String, plan: &PlanNode)
                              -> anyhow::Result<SendableDataBlockStream> {
            let mut client = FlightClient::try_create(remote_addr.clone()).await?;
            client.execute_remote_plan_action(job_id.clone(), plan).await
        }

        Ok(Box::pin(
            execute_impl(&self.remote_addr, &self.job_id, &self.plan)
                .await.map_err(ErrorCodes::from_anyhow)?
        ))
    }
}
