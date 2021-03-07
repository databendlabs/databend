// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;

use arrow_flight::{
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use log::debug;
use tonic::{Request, Response, Status, Streaming};

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::error::FuseQueryResult;
use crate::planners::PlanNode;
use crate::processors::PipelineBuilder;
use crate::sessions::{FuseQueryContextRef, SessionRef};

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct FlightService {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef,
}

impl FlightService {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager,
        }
    }

    pub fn make_server(self) -> FlightServer<impl Flight> {
        FlightServer::new(self)
    }

    pub fn try_create_ctx(&self) -> FuseQueryResult<FuseQueryContextRef> {
        let ctx = self
            .session_manager
            .try_create_context()?
            .with_cluster(self.cluster.clone())?;
        ctx.set_max_threads(self.conf.num_cpus)?;
        Ok(ctx)
    }
}

fn from_fuse_err(e: &crate::error::FuseQueryError) -> Status {
    Status::internal(format!("{:?}", e))
}

#[tonic::async_trait]
impl Flight for FlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    type ListFlightsStream = FlightStream<FlightInfo>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!()
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    type DoGetStream = FlightStream<FlightData>;
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let plan: PlanNode =
            serde_json::from_str(String::from_utf8(ticket.ticket.to_vec()).unwrap().as_str())
                .unwrap();
        debug!("do_get: {:?}", plan);

        let ctx = self.try_create_ctx().map_err(|e| from_fuse_err(&e))?;
        let mut stream = PipelineBuilder::create(ctx.clone(), plan.clone())
            .build()
            .map_err(|e| from_fuse_err(&e))?
            .execute()
            .await
            .map_err(|e| from_fuse_err(&e))?;
        let mut batches = vec![];
        // TODO(BohuTANG): move the stream instead the batch
        while let Some(block) = stream.next().await {
            let block = block.map_err(|e| from_fuse_err(&e))?;
            batches.push(block.to_arrow_batch().map_err(|e| from_fuse_err(&e))?);
        }

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data =
            arrow_flight::utils::flight_data_from_arrow_schema(plan.schema().as_ref(), &options);
        let mut flights: Vec<Result<FlightData, tonic::Status>> = vec![Ok(schema_flight_data)];

        let mut results: Vec<Result<FlightData, tonic::Status>> = batches
            .iter()
            .flat_map(|batch| {
                let (flight_dictionaries, flight_batch) =
                    arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);
                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();

        flights.append(&mut results);

        let output = futures::stream::iter(flights);
        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    type DoPutStream = FlightStream<PutResult>;
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!()
    }

    type DoExchangeStream = FlightStream<FlightData>;
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }

    type DoActionStream = FlightStream<arrow_flight::Result>;
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!()
    }

    type ListActionsStream = FlightStream<ActionType>;
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}
