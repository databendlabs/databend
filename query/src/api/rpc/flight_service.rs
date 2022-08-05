// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::ActionType;
use common_arrow::arrow_format::flight::data::Criteria;
use common_arrow::arrow_format::flight::data::Empty;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::data::FlightDescriptor;
use common_arrow::arrow_format::flight::data::FlightInfo;
use common_arrow::arrow_format::flight::data::HandshakeRequest;
use common_arrow::arrow_format::flight::data::HandshakeResponse;
use common_arrow::arrow_format::flight::data::PutResult;
use common_arrow::arrow_format::flight::data::Result as FlightResult;
use common_arrow::arrow_format::flight::data::SchemaResult;
use common_arrow::arrow_format::flight::data::Ticket;
use common_arrow::arrow_format::flight::service::flight_service_server::FlightService;
use tokio_stream::Stream;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::rpc::flight_client::{FlightExchange, ServerFlightExchange};
use crate::api::rpc::request_builder::RequestGetter;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub type FlightStream<T> =
Pin<Box<dyn Stream<Item=Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct DatabendQueryFlightService {
    sessions: Arc<SessionManager>,
}

impl DatabendQueryFlightService {
    pub fn create(sessions: Arc<SessionManager>) -> Self {
        DatabendQueryFlightService { sessions }
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamReq<T> = Request<Streaming<T>>;

#[async_trait::async_trait]
impl FlightService for DatabendQueryFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(&self, _: StreamReq<HandshakeRequest>) -> Response<Self::HandshakeStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement handshake.",
        ))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(&self, _: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement list_flights.",
        ))
    }

    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Response<FlightInfo> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_flight_info.",
        ))
    }

    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Response<SchemaResult> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_schema.",
        ))
    }

    type DoGetStream = FlightStream<FlightData>;

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, req: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        // TODO: panic hook?
        // let query_id = match req.metadata().get("x-query-id") {
        //     None => Err(Status::invalid_argument(
        //         "Must be send X-Query-ID metadata.",
        //     )),
        //     Some(metadata_value) => match metadata_value.to_str() {
        //         Ok(query_id) if !query_id.is_empty() => Ok(query_id.to_string()),
        //         Ok(_query_id) => Err(Status::invalid_argument("x-query-id metadata is empty.")),
        //         Err(cause) => Err(Status::invalid_argument(format!(
        //             "Cannot parse X-Query-ID metadata value, cause: {:?}",
        //             cause
        //         ))),
        //     },
        // }?;
        //
        // let source = match req.metadata().get("x-source") {
        //     None => Err(Status::invalid_argument("Must be send x-source metadata.")),
        //     Some(metadata_value) => match metadata_value.to_str() {
        //         Ok(source) if !source.is_empty() => Ok(source.to_string()),
        //         Ok(_source) => Err(Status::invalid_argument("x-source metadata is empty.")),
        //         Err(cause) => Err(Status::invalid_argument(format!(
        //             "Cannot parse x-source metadata value, cause: {:?}",
        //             cause
        //         ))),
        //     },
        // }?;
        //
        // let stream = req.into_inner();
        // let exchange_manager = self.sessions.get_data_exchange_manager();
        // let join_handler = exchange_manager
        //     .handle_do_put(&query_id, &source, stream)
        //     .await?;
        //
        // if let Err(cause) = join_handler.await {
        //     if !cause.is_panic() {
        //         return Err(Status::internal(format!(
        //             "Put stream is canceled. {:?}",
        //             cause
        //         )));
        //     }
        //
        //     if cause.is_panic() {
        //         let panic_error = cause.into_panic();
        //
        //         if let Some(message) = panic_error.downcast_ref::<&'static str>() {
        //             return Err(Status::internal(format!("Put stream panic, {}", message)));
        //         }
        //
        //         if let Some(message) = panic_error.downcast_ref::<String>() {
        //             return Err(Status::internal(format!("Put stream panic, {}", message)));
        //         }
        //     }
        // }
        //
        // Ok(RawResponse::new(Box::pin(tokio_stream::once(Ok(PutResult {
        //     app_metadata: vec![],
        // }))) as FlightStream<PutResult>))
        Err(Status::unimplemented("unimplement do_put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn do_get(&self, _request: Request<Ticket>) -> Response<Self::DoGetStream> {
        Err(Status::unimplemented("unimplement do_get"))
    }

    async fn do_exchange(&self, req: StreamReq<FlightData>) -> Response<Self::DoExchangeStream> {
        let source = req.get_metadata("x-source")?;
        let query_id = req.get_metadata("x-query-id")?;
        let fragment = req.get_metadata("x-fragment-id")?.parse::<usize>().unwrap();

        let (tx, rx) = async_channel::bounded(1);
        let exchange = FlightExchange::from_server(req, tx);

        let exchange_manager = self.sessions.get_data_exchange_manager();
        exchange_manager.handle_exchange(query_id, source, fragment, exchange);
        Ok(RawResponse::new(Box::pin(rx)))
    }

    type DoActionStream = FlightStream<FlightResult>;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        common_tracing::extract_remote_span_as_parent(&request);

        let action = request.into_inner();
        let flight_action: FlightAction = action.try_into()?;

        let action_result = match &flight_action {
            FlightAction::InitQueryFragmentsPlan(init_query_fragments_plan) => {
                let session = self.sessions.create_session(SessionType::FlightRPC).await?;
                let ctx = session.create_query_context().await?;
                let exchange_manager = self.sessions.get_data_exchange_manager();
                exchange_manager.init_query_fragments_plan(&ctx, &init_query_fragments_plan.executor_packet)?;

                FlightResult { body: vec![] }
            }
            FlightAction::InitNodesChannel(init_nodes_channel) => {
                let publisher_packet = &init_nodes_channel.init_nodes_channel_packet;
                let exchange_manager = self.sessions.get_data_exchange_manager();
                exchange_manager
                    .init_nodes_channel(publisher_packet)
                    .await?;

                FlightResult { body: vec![] }
            }
            FlightAction::ExecutePartialQuery(query_id) => {
                let exchange_manager = self.sessions.get_data_exchange_manager();
                exchange_manager.execute_partial_query(query_id)?;

                FlightResult { body: vec![] }
            }
        };

        // let action_result = do_flight_action.await?;
        Ok(RawResponse::new(
            Box::pin(tokio_stream::once(Ok(action_result))) as FlightStream<FlightResult>,
        ))
    }

    type ListActionsStream = FlightStream<ActionType>;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_actions(&self, request: Request<Empty>) -> Response<Self::ListActionsStream> {
        common_tracing::extract_remote_span_as_parent(&request);
        Result::Ok(RawResponse::new(
            Box::pin(tokio_stream::iter(vec![
                Ok(ActionType {
                    r#type: "PrepareShuffleAction".to_string(),
                    description: "Prepare a query stage that can be sent to the remote after receiving data from remote".to_string(),
                })
            ])) as FlightStream<ActionType>
        ))
    }
}
