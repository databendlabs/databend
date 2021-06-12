// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;

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
        Ok(Self {
            fetch_name,
            fetch_node_name,
            schema,
            ctx,
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for RemoteTransform {
    fn name(&self) -> &str {
        "RemoteTransform"
    }

    fn connect_to(&mut self, _input: Arc<dyn IProcessor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call RemoteTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!(
            "execute, fetch name:{:#}, node name:{:#}...",
            self.fetch_name,
            self.fetch_node_name
        );

        let context = self.ctx.clone();
        let cluster = context.try_get_cluster()?;
        let fetch_node = cluster.get_node_by_name(self.fetch_node_name.clone())?;

        let timeout = self.ctx.get_flight_client_timeout()?;
        let mut flight_client = fetch_node.get_flight_client().await?;
        flight_client
            .fetch_stream(self.fetch_name.clone(), self.schema.clone(), timeout)
            .await
    }
}
