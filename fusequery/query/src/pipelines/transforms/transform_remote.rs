// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_flights::QueryClient;
use common_planners::PlanNode;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;
use common_datavalues::DataSchemaRef;

pub struct RemoteTransform {
    fetch_name: String,
    fetch_node_name: String,
    schema: DataSchemaRef,
    pub ctx: FuseQueryContextRef,
}

impl RemoteTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        fetch_name: String,
        fetch_node_name: String,
        schema: DataSchemaRef,
    ) -> Result<Self> {
        Ok(Self { fetch_name, fetch_node_name, schema, ctx })
    }
}

#[async_trait::async_trait]
impl IProcessor for RemoteTransform {
    fn name(&self) -> &str {
        "RemoteTransform"
    }

    fn connect_to(&mut self, _input: Arc<dyn IProcessor>) -> Result<()> {
        Result::Err(ErrorCodes::LogicalError("Cannot call RemoteTransform connect_to"))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let mut context = self.ctx.clone();
        let mut cluster = context.try_get_cluster()?;
        let mut fetch_node = cluster.get_node_by_name(self.fetch_node_name.clone())?;

        let timeout = self.ctx.get_flight_client_timeout()?;
        let mut flight_client = fetch_node.try_get_client()?;
        flight_client.fetch_stream(self.fetch_name.clone(), self.schema.clone(), timeout).await
    }
}
