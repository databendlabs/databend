// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;

use common_arrow::arrow_flight::flight_descriptor::DescriptorType;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_arrow::arrow_flight::utils::flight_schema_from_arrow_schema;
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
use common_arrow::arrow_flight::Result as FlightResult;
use common_arrow::arrow_flight::SchemaResult;
use common_arrow::arrow_flight::Ticket;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use crate::api::rpc::actions::ExecutePlanWithShuffleAction;
use crate::api::rpc::flight_dispatcher::PrepareStageInfo;
use crate::api::rpc::flight_dispatcher::Request as DispatcherRequest;
use crate::api::rpc::to_status;
use crate::api::rpc::StreamInfo;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct FuseQueryService {
    dispatcher_sender: Sender<DispatcherRequest>
}

impl FuseQueryService {
    pub fn create(dispatcher_sender: Sender<DispatcherRequest>) -> FuseQueryService {
        FuseQueryService { dispatcher_sender }
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamRequest<T> = Request<Streaming<T>>;

#[async_trait::async_trait]
impl FlightService for FuseQueryService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _: StreamRequest<HandshakeRequest>
    ) -> Response<Self::HandshakeStream> {
        Result::Err(Status::unimplemented(
            "FuseQuery does not implement handshake."
        ))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(&self, _: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        Result::Err(Status::unimplemented(
            "FuseQuery does not implement list_flights."
        ))
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Response<FlightInfo> {
        let descriptor = request.into_inner();

        fn create_descriptor(stream_full_name: &str) -> FlightDescriptor {
            FlightDescriptor {
                r#type: DescriptorType::Path as i32,
                cmd: vec![],
                path: stream_full_name
                    .split('/')
                    .map(|str| str.to_string())
                    .collect::<Vec<_>>()
            }
        }

        // fn create_endpoints(stream_full_name: &String, endpoints: Vec<String>) -> Vec<FlightEndpoint> {
        //     endpoints.iter().map(|endpoint| {
        //         FlightEndpoint {
        //             ticket: Some(Ticket { ticket: endpoint.as_bytes().to_vec() }),
        //             location: "".to_string(),
        //         }
        //     })
        // }

        fn create_flight_info(stream_info: StreamInfo) -> FlightInfo {
            let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();

            FlightInfo {
                schema: flight_schema_from_arrow_schema(&*stream_info.0, &options).schema,
                flight_descriptor: Some(create_descriptor(&stream_info.1)),
                endpoint: vec![],
                total_records: -1,
                total_bytes: -1
            }
        }

        type ResponseSchema = common_exception::Result<RawResponse<FlightInfo>>;
        fn create_flight_info_response(receive_schema: Option<StreamInfo>) -> ResponseSchema {
            Ok(RawResponse::new(create_flight_info(
                receive_schema.unwrap()
            )))
        }

        match descriptor.r#type {
            1 => {
                // DescriptorType::Path
                let (response_sender, mut receiver) = channel(1);
                self.dispatcher_sender
                    .send(DispatcherRequest::GetStreamInfo(
                        descriptor.path.join("/"),
                        response_sender
                    ))
                    .await
                    .map_err(|error| Status::unknown(error.to_string()))?;

                let schema = receiver.recv().await;

                schema
                    .transpose()
                    .and_then(create_flight_info_response)
                    .map_err(to_status)
            }
            _unimplemented_type => Err(Status::unimplemented(format!(
                "FuseQuery does not implement Flight type: {}",
                descriptor.r#type
            )))
        }
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Response<SchemaResult> {
        let descriptor = request.into_inner();

        fn create_schema(schema_ref: DataSchemaRef) -> SchemaResult {
            let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();
            flight_schema_from_arrow_schema(&*schema_ref, &options)
        }

        type ResponseSchema = common_exception::Result<RawResponse<SchemaResult>>;
        fn create_schema_response(receive_schema: Option<DataSchemaRef>) -> ResponseSchema {
            Ok(RawResponse::new(create_schema(receive_schema.unwrap())))
        }

        match descriptor.r#type {
            1 => {
                // DescriptorType::Path
                let (response_sender, mut receiver) = channel(1);
                self.dispatcher_sender
                    .send(DispatcherRequest::GetSchema(
                        descriptor.path.join("/"),
                        response_sender
                    ))
                    .await
                    .map_err(|error| Status::unknown(error.to_string()))?;

                let schema = receiver.recv().await;

                schema
                    .transpose()
                    .and_then(create_schema_response)
                    .map_err(to_status)
            }
            _unimplemented_type => Err(Status::unimplemented(format!(
                "FuseQuery does not implement Flight type: {}",
                descriptor.r#type
            )))
        }
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> Response<Self::DoGetStream> {
        type DataReceiver = Receiver<common_exception::Result<FlightData>>;
        fn create_stream(receiver: DataReceiver) -> FlightStream<FlightData> {
            // TODO: Tracking progress is shown in the system.shuffles table
            Box::pin(
                ReceiverStream::new(receiver).map(|flight_data| flight_data.map_err(to_status))
            ) as FlightStream<FlightData>
        }

        type ResultResponse = common_exception::Result<RawResponse<FlightStream<FlightData>>>;
        fn create_stream_response(receiver: Option<DataReceiver>) -> ResultResponse {
            Ok(RawResponse::new(create_stream(receiver.unwrap())))
        }

        match std::str::from_utf8(&request.into_inner().ticket) {
            Err(utf_8_error) => Err(Status::invalid_argument(utf_8_error.to_string())),
            Ok(ticket) => {
                // Flight ticket = query_id/stage_id/stream_id
                let (response_sender, mut receiver) = channel(1);
                let get_stream_request =
                    DispatcherRequest::GetStream(ticket.to_string(), response_sender);

                match self.dispatcher_sender.send(get_stream_request).await {
                    Err(error) => Err(Status::unavailable(format!(
                        "Flight do_get unavailable: {}",
                        error
                    ))),
                    Ok(_) => receiver
                        .recv()
                        .await
                        .transpose()
                        .and_then(create_stream_response)
                        .map_err(to_status)
                }
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, _: StreamRequest<FlightData>) -> Response<Self::DoPutStream> {
        Result::Err(Status::unimplemented(
            "FuseQuery does not implement do_put."
        ))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(&self, _: StreamRequest<FlightData>) -> Response<Self::DoExchangeStream> {
        Result::Err(Status::unimplemented(
            "FuseQuery does not implement do_exchange."
        ))
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let action = request.into_inner();

        fn once(result: common_exception::Result<FlightResult>) -> FlightStream<FlightResult> {
            Box::pin(tokio_stream::once::<Result<FlightResult, Status>>(
                result.map_err(to_status)
            )) as FlightStream<FlightResult>
        }

        match action.r#type.as_str() {
            "PrepareQueryStage" => match std::str::from_utf8(&action.body) {
                Err(utf_8_error) => Err(Status::invalid_argument(utf_8_error.to_string())),
                Ok(prepare_stage_info_str) => {
                    let action = serde_json::from_str::<ExecutePlanWithShuffleAction>(
                        prepare_stage_info_str
                    )
                    .map_err(ErrorCodes::from)
                    .map_err(to_status)?;

                    let (response_sender, mut receiver) = channel(1);
                    self.dispatcher_sender
                        .send(DispatcherRequest::PrepareQueryStage(
                            PrepareStageInfo::create(
                                action.query_id,
                                action.stage_id,
                                action.plan,
                                action.scatters,
                                action.scatters_action
                            ),
                            response_sender
                        ))
                        .await
                        .map_err(|error| Status::unknown(error.to_string()))?;

                    Ok(RawResponse::new(once(
                        receiver
                            .recv()
                            .await
                            .transpose()
                            .map(|_| FlightResult { body: vec![] })
                    )))
                }
            },
            _ => Result::Err(Status::unimplemented(format!(
                "FuseQuery does not implement action: {}.",
                action.r#type
            )))
        }
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(&self, _: Request<Empty>) -> Response<Self::ListActionsStream> {
        Result::Ok(RawResponse::new(
            Box::pin(tokio_stream::iter(vec![
                Ok(ActionType {
                    r#type: "PrepareQueryStage".to_string(),
                    description: "Prepare a query stage that can be sent to the remote after receiving data from remote".to_string(),
                })
            ])) as FlightStream<ActionType>
        ))
    }
}
