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

use crate::api::FlightClient;
use crate::api::FlightTicket;
use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::FuseQueryContextRef;

pub struct RemoteTransform {
    ticket: FlightTicket,
    fetch_node_name: String,
    schema: DataSchemaRef,
    pub ctx: FuseQueryContextRef,
}

impl RemoteTransform {
    pub fn try_create(
        ticket: FlightTicket,
        context: FuseQueryContextRef,
        fetch_node_name: String,
        schema: DataSchemaRef,
    ) -> Result<RemoteTransform> {
        Ok(RemoteTransform {
            ticket,
            fetch_node_name,
            schema,
            ctx: context,
        })
    }

    async fn flight_client(&self) -> Result<FlightClient> {
        let context = self.ctx.clone();
        let cluster = context.try_get_cluster()?;
        let fetch_node = cluster.get_node_by_name(self.fetch_node_name.clone())?;
        fetch_node.get_flight_client(&self.ctx.get_config()).await
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
            "execute, flight_ticket {:?}, node name:{:#}...",
            self.ticket,
            self.fetch_node_name
        );

        let data_schema = self.schema.clone();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;

        let fetch_ticket = self.ticket.clone();
        let mut flight_client = self.flight_client().await?;
        let fetch_stream = flight_client.fetch_stream(fetch_ticket, data_schema, timeout);
        Ok(Box::pin(self.ctx.try_create_abortable(fetch_stream.await?)?))
    }
}
