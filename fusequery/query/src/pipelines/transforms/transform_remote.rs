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

use crate::api::FlightTicket;
use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::FuseQueryContextRef;

pub struct RemoteTransform {
    query_id: String,
    stage_id: String,
    stream_id: String,
    fetch_node_name: String,
    schema: DataSchemaRef,
    pub ctx: FuseQueryContextRef,
}

impl RemoteTransform {
    pub fn try_create(
        query_id: String,
        stage_id: String,
        stream_id: String,
        fetch_node_name: String,
        schema: DataSchemaRef,
        ctx: FuseQueryContextRef,
    ) -> Result<Self> {
        Ok(Self {
            query_id,
            stage_id,
            stream_id,
            fetch_node_name,
            schema,
            ctx,
        })
    }
}

#[async_trait::async_trait]
impl Processor for RemoteTransform {
    fn name(&self) -> &str {
        "RemoteTransform"
    }

    fn connect_to(&mut self, _input: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call RemoteTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!(
            "execute, query id:{:#}, stage id:{:#}, stream:{:#}, node name:{:#}...",
            self.query_id,
            self.stage_id,
            self.stream_id,
            self.fetch_node_name
        );

        let context = self.ctx.clone();
        let cluster = context.try_get_cluster()?;
        let fetch_node = cluster.get_node_by_name(self.fetch_node_name.clone())?;

        let data_schema = self.schema.clone();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        let mut flight_client = fetch_node.get_flight_client().await?;

        let ticket = FlightTicket::stream(&self.query_id, &self.stage_id, &self.stream_id);
        // TODO: cancel action if stream is not complete
        flight_client.fetch_stream(ticket, data_schema, timeout).await
    }
}
