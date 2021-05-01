// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::pin::Pin;
use std::time::Instant;

use common_arrow::arrow;
use common_arrow::arrow_flight::flight_service_server::FlightService as Flight;
use common_arrow::arrow_flight::flight_service_server::FlightServiceServer as FlightServer;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::ActionType;
use common_arrow::arrow_flight::Criteria;
use common_arrow::arrow_flight::Empty;
use common_arrow::arrow_flight::FlightData;
use common_arrow::arrow_flight::FlightDescriptor;
use common_arrow::arrow_flight::FlightInfo;
use common_arrow::arrow_flight::HandshakeRequest;
use common_arrow::arrow_flight::HandshakeResponse;
use common_arrow::arrow_flight::PutResult;
use common_arrow::arrow_flight::SchemaResult;
use common_arrow::arrow_flight::Ticket;
use common_arrow::arrow_flight::{self};
use common_flights::query_do_action::QueryDoAction;
use common_flights::query_do_get::QueryDoGet;
use futures::Stream;
use futures::StreamExt;
use log::info;
use metrics::histogram;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::SessionRef;

type FlightDataSender = tokio::sync::mpsc::Sender<Result<FlightData, Status>>;
type FlightDataReceiver = tokio::sync::mpsc::Receiver<Result<FlightData, Status>>;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct FlightService {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef
}

impl FlightService {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager
        }
    }

    pub fn make_server(self) -> FlightServer<impl Flight> {
        FlightServer::new(self)
    }
}

#[async_trait::async_trait]
impl Flight for FlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    type ListFlightsStream = FlightStream<FlightInfo>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!()
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>
    ) -> Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    type DoGetStream = FlightStream<FlightData>;
    async fn do_get(
        &self,
        request: Request<Ticket>
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let action: QueryDoGet = request.try_into()?;
        match action {
            QueryDoGet::ExecutePlan(action) => {
                let plan = action.plan;
                let cpus = self.conf.num_cpus;
                let cluster = self.cluster.clone();
                let session_manager = self.session_manager.clone();
                let (sender, receiver): (FlightDataSender, FlightDataReceiver) =
                    tokio::sync::mpsc::channel(2);

                info!(
                    "Executor[{:?}] received action, job_id: {:?}",
                    self.conf.flight_api_address, action.job_id
                );

                // Create the context.
                let ctx = session_manager
                    .try_create_context()
                    .map_err(|e| Status::internal(e.to_string()))?
                    .with_cluster(cluster.clone())
                    .map_err(|e| Status::internal(e.to_string()))?;
                ctx.set_max_threads(cpus)
                    .map_err(|e| Status::internal(e.to_string()))?;

                // Pipeline.
                let mut pipeline = PipelineBuilder::create(ctx.clone(), plan.clone())
                    .build()
                    .map_err(|e| Status::internal(e.to_string()))?;

                let mut stream = pipeline
                    .execute()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                tokio::spawn(async move {
                    let options = arrow::ipc::writer::IpcWriteOptions::default();
                    let mut has_send = false;
                    let start = Instant::now();

                    // Get the batch from the stream and send to one channel.
                    while let Some(item) = stream.next().await {
                        let block = match_async_result!(item, sender);
                        if !has_send {
                            let schema_flight_data =
                                arrow_flight::utils::flight_data_from_arrow_schema(
                                    block.schema(),
                                    &options
                                );
                            sender.send(Ok(schema_flight_data)).await.ok();
                            has_send = true;
                        }

                        // Check block is empty.
                        if !block.is_empty() {
                            // Convert batch to flight data.
                            let batch = match_async_result!(block.try_into(), sender);
                            let (flight_dicts, flight_batch) =
                                arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);
                            let batch_flight_data = flight_dicts
                                .into_iter()
                                .chain(std::iter::once(flight_batch))
                                .map(Ok);

                            for batch in batch_flight_data {
                                send_response(&sender, batch.clone()).await.ok();
                            }
                        }
                    }

                    // Cost.
                    let delta = start.elapsed();
                    histogram!(super::metrics::METRIC_FLIGHT_EXECUTE_COST, delta);

                    info!("Executor executed cost: {:?}", delta);

                    // Remove the context from the manager.
                    session_manager.try_remove_context(ctx.clone()).ok();
                });

                Ok(Response::new(
                    Box::pin(ReceiverStream::new(receiver)) as Self::DoGetStream
                ))
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!()
    }

    type DoExchangeStream = FlightStream<FlightData>;
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }

    type DoActionStream = FlightStream<arrow_flight::Result>;
    async fn do_action(
        &self,
        request: Request<Action>
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action: QueryDoAction = request.try_into()?;
        match action {
            QueryDoAction::FetchPartition(v) => {
                let (uuid, nums) = (v.uuid, v.nums);

                // Get partitions.
                let parts = self
                    .session_manager
                    .try_fetch_partitions(uuid, nums as usize)
                    .map_err(|e| Status::internal(e.to_string()))?;

                // Response stream.
                let stream = {
                    let result = arrow_flight::Result {
                        body: serde_json::to_vec(&parts)
                            .map_err(|e| Status::internal(e.to_string()))?
                    };

                    let flights: Vec<Result<arrow_flight::Result, Status>> = vec![Ok(result)];
                    futures::stream::iter(flights)
                };
                Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
            }
        }
    }

    type ListActionsStream = FlightStream<ActionType>;
    async fn list_actions(
        &self,
        _request: Request<Empty>
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

async fn send_response(
    tx: &FlightDataSender,
    data: Result<FlightData, Status>
) -> Result<(), Status> {
    tx.send(data)
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
}
