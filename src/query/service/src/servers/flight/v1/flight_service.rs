// Copyright 2021 Datafuse Labs
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

use std::pin::Pin;

use arrow_flight::Action;
use arrow_flight::ActionType;
use arrow_flight::Criteria;
use arrow_flight::Empty;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightInfo;
use arrow_flight::HandshakeRequest;
use arrow_flight::HandshakeResponse;
use arrow_flight::PollInfo;
use arrow_flight::PutResult;
use arrow_flight::Result as FlightResult;
use arrow_flight::SchemaResult;
use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use fastrace::func_path;
use fastrace::prelude::*;
use futures_util::stream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use crate::servers::flight::request_builder::RequestGetter;
use crate::servers::flight::v1::actions::FlightActions;
use crate::servers::flight::v1::actions::flight_actions;
use crate::servers::flight::v1::exchange::DataExchangeManager;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct DatabendQueryFlightService {
    actions: FlightActions,
}

impl DatabendQueryFlightService {
    pub fn create() -> Self {
        DatabendQueryFlightService {
            actions: flight_actions(),
        }
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamReq<T> = Request<Streaming<T>>;

#[async_trait::async_trait]
impl FlightService for DatabendQueryFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    #[async_backtrace::framed]
    async fn handshake(&self, _: StreamReq<HandshakeRequest>) -> Response<Self::HandshakeStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement handshake.",
        ))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    #[async_backtrace::framed]
    async fn list_flights(&self, _: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement list_flights.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Response<FlightInfo> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_flight_info.",
        ))
    }

    #[async_backtrace::framed]
    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Response<PollInfo> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement poll_flight_info.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Response<SchemaResult> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_schema.",
        ))
    }

    type DoGetStream = FlightStream<FlightData>;

    #[async_backtrace::framed]
    async fn do_get(&self, request: Request<Ticket>) -> Response<Self::DoGetStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(func_path!(), &request);
        let _guard = root.set_local_parent();

        match request.get_metadata("x-type")?.as_str() {
            "request_server_exchange" => {
                let target = request.get_metadata("x-target")?;
                let query_id = request.get_metadata("x-query-id")?;
                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance().handle_statistics_exchange(query_id, target)?,
                )))
            }
            "exchange_fragment" => {
                let query_id = request.get_metadata("x-query-id")?;
                let channel_id = request.get_metadata("x-channel-id")?;

                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance()
                        .handle_exchange_fragment(query_id, channel_id)?,
                )))
            }
            exchange_type => Err(Status::unimplemented(format!(
                "Unimplemented exchange type: {:?}",
                exchange_type
            ))),
        }
    }

    type DoPutStream = FlightStream<PutResult>;

    #[async_backtrace::framed]
    async fn do_put(&self, _req: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        Err(Status::unimplemented("unimplemented do_put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    #[async_backtrace::framed]
    async fn do_exchange(&self, req: StreamReq<FlightData>) -> Response<Self::DoExchangeStream> {
        let query_id = req.get_metadata("x-query-id")?;
        let channel_id = req.get_metadata("x-channel-id")?;

        let sender = DataExchangeManager::instance().handle_do_exchange(&query_id, &channel_id)?;

        let mut stream = req.into_inner();
        let (tx, rx) = async_channel::bounded(1);

        GlobalIORuntime::instance().spawn(async move {
            while let Some(result) = stream.next().await {
                let Ok(flight_data) = result else {
                    break;
                };

                if sender.add_data(flight_data).await.is_err() {
                    break; // Receiver closed
                }

                // Send pong (empty response signals readiness for next ping)
                if let Err(_cause) = tx.try_send(Ok(FlightData::default())) {
                    break;
                }
            }
            // sender is dropped here → closes sub-queues, notifies processors
        });

        Ok(RawResponse::new(Box::pin(rx)))
    }

    type DoActionStream = FlightStream<FlightResult>;

    #[async_backtrace::framed]
    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(func_path!(), &request);

        let secret = request.get_metadata("secret")?;

        let config = GlobalConfig::instance();
        if secret != config.query.node_secret {
            return Err(Into::into(ErrorCode::AuthenticateFailure(format!(
                "authenticate failure while flight, node: {}",
                config.query.node_id,
            ))));
        }

        let action = request.into_inner();
        match self
            .actions
            .do_action(&action.r#type, &action.body)
            .in_span(root)
            .await
        {
            Err(cause) => Err(cause.into()),
            Ok(body) => Ok(RawResponse::new(
                Box::pin(tokio_stream::once(Ok(FlightResult { body: body.into() })))
                    as FlightStream<FlightResult>,
            )),
        }
    }

    type ListActionsStream = FlightStream<ActionType>;

    #[async_backtrace::framed]
    async fn list_actions(&self, _: Request<Empty>) -> Response<Self::ListActionsStream> {
        Ok(RawResponse::new(Box::pin(stream::empty())))
    }
}
