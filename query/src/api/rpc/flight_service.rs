// Copyright 2020 Datafuse Labs.
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
use crate::api::rpc::flight_dispatcher::DatabendQueryFlightDispatcher;
use crate::api::rpc::flight_dispatcher::DatabendQueryFlightDispatcherRef;
use crate::api::rpc::flight_service_stream::FlightDataStream;
use crate::api::rpc::flight_tickets::FlightTicket;
use crate::sessions::SessionManagerRef;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct DatabendQueryFlightService {
    sessions: SessionManagerRef,
    dispatcher: Arc<DatabendQueryFlightDispatcher>,
}

impl DatabendQueryFlightService {
    pub fn create(
        dispatcher: DatabendQueryFlightDispatcherRef,
        sessions: SessionManagerRef,
    ) -> Self {
        DatabendQueryFlightService {
            sessions,
            dispatcher,
        }
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

    async fn do_get(&self, request: Request<Ticket>) -> Response<Self::DoGetStream> {
        let ticket: FlightTicket = request.into_inner().try_into()?;

        match ticket {
            FlightTicket::StreamTicket(steam_ticket) => {
                let receiver = self.dispatcher.get_stream(&steam_ticket)?;

                Ok(RawResponse::new(
                    Box::pin(FlightDataStream::create(receiver)) as FlightStream<FlightData>,
                ))
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, _: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement do_put.",
        ))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(&self, _: StreamReq<FlightData>) -> Response<Self::DoExchangeStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement do_exchange.",
        ))
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let action = request.into_inner();
        let flight_action: FlightAction = action.try_into()?;

        let action_result = match &flight_action {
            FlightAction::CancelAction(action) => {
                // We only destroy when session is exist
                let session_id = action.query_id.clone();
                if let Some(session) = self.sessions.get_session(&session_id) {
                    // TODO: remove streams
                    session.force_kill_session();
                }

                FlightResult { body: vec![] }
            }
            FlightAction::BroadcastAction(action) => {
                let session_id = action.query_id.clone();
                let is_aborted = self.dispatcher.is_aborted();
                let session = self.sessions.create_rpc_session(session_id, is_aborted)?;

                self.dispatcher
                    .broadcast_action(session, flight_action)
                    .await?;
                FlightResult { body: vec![] }
            }
            FlightAction::PrepareShuffleAction(action) => {
                let session_id = action.query_id.clone();
                let is_aborted = self.dispatcher.is_aborted();
                let session = self.sessions.create_rpc_session(session_id, is_aborted)?;

                self.dispatcher
                    .shuffle_action(session, flight_action)
                    .await?;
                FlightResult { body: vec![] }
            }
        };

        // let action_result = do_flight_action.await?;
        Ok(RawResponse::new(
            Box::pin(tokio_stream::once(Ok(action_result))) as FlightStream<FlightResult>,
        ))
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(&self, _: Request<Empty>) -> Response<Self::ListActionsStream> {
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
