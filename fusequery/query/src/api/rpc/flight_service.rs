// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Cursor;
use std::pin::Pin;
use std::time::Instant;

use common_arrow::arrow;
use common_arrow::arrow_flight::{
    self,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use log::info;
use metrics::histogram;
use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::api::rpc::{ExecuteAction, FetchPartitionRequest};
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::pipelines::processors::PipelineBuilder;
use crate::protobuf::{FlightActionRequest, FlightRequest};
use crate::sessions::SessionRef;

type FlightDataSender = tokio::sync::mpsc::Sender<Result<FlightData, Status>>;
type FlightDataReceiver = tokio::sync::mpsc::Receiver<Result<FlightData, Status>>;

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
        let mut buf = Cursor::new(&ticket.ticket);

        // Decode FlightRequest from buffer.
        let request: FlightRequest =
            FlightRequest::decode(&mut buf).map_err(|e| Status::internal(e.to_string()))?;

        // Decode ExecuteAction from request.
        let json_str = request.action.as_str();
        let action = serde_json::from_str::<ExecuteAction>(json_str)
            .map_err(|e| Status::internal(e.to_string()))?;

        match action {
            ExecuteAction::ExecutePlan(action) => {
                let plan = action.plan;
                let cpus = self.conf.num_cpus;
                let cluster = self.cluster.clone();
                let session_manager = self.session_manager.clone();
                let (sender, receiver): (FlightDataSender, FlightDataReceiver) =
                    tokio::sync::mpsc::channel(2);

                info!(
                    "Executor[{:?}] received action, job_id: {:?}",
                    self.conf.rpc_api_address, action.job_id
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
                                    &options,
                                );
                            sender.send(Ok(schema_flight_data)).await.ok();
                            has_send = true;
                        }

                        // Check block is empty.
                        if !block.is_empty() {
                            // Convert batch to flight data.
                            let batch = match_async_result!(block.to_arrow_batch(), sender);
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
            ExecuteAction::FetchPartition(_) => {
                unimplemented!()
            }
        }
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
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Get the FlightActionRequest.
        let action = request.into_inner();
        let mut buf = Cursor::new(&action.body);
        let request: FlightActionRequest =
            FlightActionRequest::decode(&mut buf).map_err(|e| Status::internal(e.to_string()))?;

        // Check action.
        match action.r#type.as_str() {
            "fetch_partition_action" => {
                // Fetch partition request.
                let (uuid, nums) = {
                    let req: FetchPartitionRequest = serde_json::from_str(&request.body)
                        .map_err(|e| Status::internal(e.to_string()))?;
                    (req.uuid, req.nums)
                };

                // Get partitions.
                let parts = self
                    .session_manager
                    .try_fetch_partitions(uuid, nums as usize)
                    .map_err(|e| Status::internal(e.to_string()))?;

                // Response stream.
                let stream = {
                    let result = arrow_flight::Result {
                        body: serde_json::to_vec(&parts)
                            .map_err(|e| Status::internal(e.to_string()))?,
                    };

                    let flights: Vec<Result<arrow_flight::Result, Status>> = vec![Ok(result)];
                    futures::stream::iter(flights)
                };
                Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
            }
            _ => Err(Status::internal(format!(
                "do_action unsupported {:?} method",
                action.r#type
            ))),
        }
    }

    type ListActionsStream = FlightStream<ActionType>;
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

async fn send_response(
    tx: &FlightDataSender,
    data: Result<FlightData, Status>,
) -> Result<(), Status> {
    tx.send(data)
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
}
