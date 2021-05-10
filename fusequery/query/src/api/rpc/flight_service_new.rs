use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::channel;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use common_arrow::arrow_flight::{Action, FlightEndpoint};
use common_arrow::arrow_flight::ActionType;
use common_arrow::arrow_flight::Criteria;
use common_arrow::arrow_flight::Empty;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_arrow::arrow_flight::FlightData;
use common_arrow::arrow_flight::FlightDescriptor;
use common_arrow::arrow_flight::FlightInfo;
use common_arrow::arrow_flight::HandshakeRequest;
use common_arrow::arrow_flight::HandshakeResponse;
use common_arrow::arrow_flight::PutResult;
use common_arrow::arrow_flight::SchemaResult;
use common_arrow::arrow_flight::Ticket;
use common_arrow::arrow_flight::Result as FlightResult;

use crate::api::rpc::flight_dispatcher::{Request as DispatcherRequest, PrepareStageInfo};
use crate::api::rpc::{FlightStream, StreamInfo, FlightDispatcher};
use tokio_stream::wrappers::ReceiverStream;
use common_exception::ErrorCodes;
use tokio_stream::StreamExt;
use common_arrow::arrow_flight::flight_descriptor::DescriptorType;
use common_arrow::arrow_flight::utils::flight_schema_from_arrow_schema;
use common_datavalues::DataSchemaRef;
use common_arrow::arrow::datatypes::SchemaRef;
use common_flights::ExecutePlanWithShuffleAction;

pub struct FuseQueryService {
    dispatcher_sender: Sender<DispatcherRequest>,
}

impl FuseQueryService {
    pub fn create(dispatcher_sender: Sender<DispatcherRequest>) -> FuseQueryService {
        FuseQueryService {
            dispatcher_sender
        }
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamRequest<T> = Request<Streaming<T>>;

#[async_trait::async_trait]
impl FlightService for FuseQueryService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(&self, request: StreamRequest<HandshakeRequest>) -> Response<Self::HandshakeStream> {
        unimplemented!()
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(&self, request: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        unimplemented!()
        // let criteria = request.into_inner();
        // let expression = criteria.expression.into_string()?;
        //
        // fn get_flight(query_id: &String, stage_id: &String, flight_id: &String) -> FlightInfo {
        //     FlightInfo {
        //         schema:,
        //         endpoint: vec![],
        //         flight_descriptor: None,
        //         total_records: -1,
        //         total_bytes: -1,
        //     }
        // }
        //
        // fn get_flights(query_id: &String, v: (&String, &StagePtr), expressions: &Vec<&str>) -> Vec<FlightInfo> {
        //     let (stage_id, stage) = v;
        //     if expressions.len() < 3 {
        //         return stage.flights
        //             .iter()
        //             .map(|(id, _)| get_flight(query_id, stage_id, id))
        //             .collect_vec();
        //     }
        //
        //     stage.flights
        //         .iter()
        //         .filter(|(id, _)| id.starts_with(expressions[2]))
        //         .map(|(id, _)| get_flight(query_id, stage_id, id))
        //         .collect_vec()
        // }
        //
        // fn get_stage_flights(v: (&String, &QueryInfoPtr), expressions: &Vec<&str>) -> Vec<FlightInfo> {
        //     let (query_id, query) = v;
        //     if expressions.len() < 2 {
        //         return query.stages
        //             .iter()
        //             .flat_map(|v| get_flights(query_id, v, expressions))
        //             .collect_vec();
        //     }
        //
        //     query.stages
        //         .iter()
        //         .filter(|(id, _)| id.starts_with(expressions[1]))
        //         .flat_map(|v| get_flights(query_id, v, expressions))
        //         .collect_vec()
        // }
        //
        // fn get_queries_flights(queries: &Queries, expressions: &Vec<&str>) -> Vec<FlightInfo> {
        //     match expressions.len() {
        //         0 => {
        //             queries
        //                 .read()
        //                 .iter()
        //                 .flat_map(|v| get_stage_flights(v, expressions))
        //         },
        //         _ => {
        //             queries
        //                 .read()
        //                 .iter()
        //                 .filter(|(id, _)| id.starts_with(expressions[0]))
        //                 .flat_map(|v| get_stage_flights(v, expressions))
        //         }
        //     }.collect_vec()
        // }
        //
        // let expressions = expression.trim_start_matches("/").split("/").collect_vec();
        // let stream = futures::stream::iter(flightsget_queries_flights(&self.queries, &expressions).iter().map(Result::Ok));
        // Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Response<FlightInfo> {
        let descriptor = request.into_inner();

        fn create_descriptor(stream_full_name: &String) -> FlightDescriptor {
            FlightDescriptor {
                r#type: DescriptorType::Path as i32,
                cmd: vec![],
                path: stream_full_name.split("/").map(|str| str.to_string()).collect::<Vec<_>>(),
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
                total_bytes: -1,
            }
        }

        type ResponseSchema = common_exception::Result<RawResponse<FlightInfo>>;
        fn create_flight_info_response(receive_schema: Option<StreamInfo>) -> ResponseSchema {
            receive_schema.ok_or_else(|| ErrorCodes::NotFoundStream("".to_string()))
                .map(create_flight_info).map(RawResponse::new)
        }

        match descriptor.r#type {
            1 => {
                // DescriptorType::Path
                let (response_sender, mut receiver) = channel(1);
                self.dispatcher_sender.send(DispatcherRequest::GetStreamInfo(descriptor.path.join("/"), response_sender)).await;
                let schema = receiver.recv().await;

                schema.transpose().and_then(create_flight_info_response)
                    .map_err(|e| Status::internal(e.to_string()))
            },
            _unimplemented_type => Err(Status::unimplemented(format!("FuseQuery does not implement Flight type: {}", descriptor.r#type)))
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
            receive_schema.ok_or_else(|| ErrorCodes::NotFoundStream("".to_string()))
                .map(create_schema).map(RawResponse::new)
        }

        match descriptor.r#type {
            1 => {
                // DescriptorType::Path
                let (response_sender, mut receiver) = channel(1);
                self.dispatcher_sender.send(DispatcherRequest::GetSchema(descriptor.path.join("/"), response_sender)).await;
                let schema = receiver.recv().await;

                schema.transpose().and_then(create_schema_response)
                    .map_err(|e| Status::internal(e.to_string()))
            },
            _unimplemented_type => Err(Status::unimplemented(format!("FuseQuery does not implement Flight type: {}", descriptor.r#type)))
        }
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> Response<Self::DoGetStream> {
        type DataReceiver = Receiver<common_exception::Result<FlightData>>;
        fn create_stream(receiver: DataReceiver) -> FlightStream<FlightData> {
            // TODO: Tracking progress is shown in the system.shuffles table
            Box::pin(ReceiverStream::new(receiver).map(|flight_data| {
                flight_data.map_err(|e| Status::internal(e.to_string()))
            })) as FlightStream<FlightData>
        }

        type ResultResponse = common_exception::Result<RawResponse<FlightStream<FlightData>>>;
        fn create_stream_response(receiver: Option<DataReceiver>) -> ResultResponse {
            receiver.ok_or_else(|| ErrorCodes::NotFoundStream("".to_string()))
                .map(create_stream).map(RawResponse::new)
        }

        match std::str::from_utf8(&request.into_inner().ticket) {
            Err(utf_8_error) => Err(Status::invalid_argument(utf_8_error.to_string())),
            Ok(ticket) => {
                // Flight ticket = query_id/stage_id/stream_id
                println!("Get Flight Stream {}", ticket);
                let (response_sender, mut receiver) = channel(1);
                self.dispatcher_sender.send(DispatcherRequest::GetStream(ticket.to_string(), response_sender)).await;
                receiver.recv().await
                    .transpose()
                    .and_then(create_stream_response)
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, _: StreamRequest<FlightData>) -> Response<Self::DoPutStream> {
        Result::Err(Status::unimplemented("FuseQuery does not implement do_put."))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(&self, _: StreamRequest<FlightData>) -> Response<Self::DoExchangeStream> {
        Result::Err(Status::unimplemented("FuseQuery does not implement do_exchange."))
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let action = request.into_inner();

        fn once(result: common_exception::Result<FlightResult>) -> FlightStream<FlightResult> {
            Box::pin(tokio_stream::once::<Result<FlightResult, Status>>(
                result.map_err(|e| Status::internal(e.to_string())))
            ) as FlightStream<FlightResult>
        }

        match action.r#type.as_str() {
            "PrepareQueryStage" => {
                match std::str::from_utf8(&action.body) {
                    Err(utf_8_error) => Err(Status::invalid_argument(utf_8_error.to_string())),
                    Ok(prepare_stage_info_str) => {
                        let action = serde_json::from_str::<ExecutePlanWithShuffleAction>(prepare_stage_info_str)
                            .map_err(|e| tonic::Status::internal(e.to_string()))?;

                        let (response_sender, mut receiver) = channel(1);
                        self.dispatcher_sender.send(DispatcherRequest::PrepareQueryStage(PrepareStageInfo::create(action.query_id, action.stage_id, action.plan, action.shuffle_to), response_sender)).await;
                        Ok(RawResponse::new(once(receiver.recv().await.transpose().map(|_| FlightResult { body: vec![] }))))
                    }
                }
            }
            _ => Result::Err(Status::unimplemented(format!("FuseQuery does not implement action: {}.", action.r#type))),
        }
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(&self, request: Request<Empty>) -> Response<Self::ListActionsStream> {
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

