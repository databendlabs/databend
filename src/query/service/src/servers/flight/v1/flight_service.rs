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

use crate::servers::flight::request_getter::RequestGetter;
use crate::servers::flight::v1::actions::FlightActions;
use crate::servers::flight::v1::actions::flight_actions;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::DoExchangeRequest;
use crate::servers::flight::v1::network::DoExchangeResponse;

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
    async fn do_get(&self, _request: Request<Ticket>) -> Response<Self::DoGetStream> {
        Err(Status::unimplemented(
            "DatabendQuery uses do_exchange for query-node streams",
        ))
    }

    type DoPutStream = FlightStream<PutResult>;

    #[async_backtrace::framed]
    async fn do_put(&self, _req: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        Err(Status::unimplemented("unimplemented do_put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    #[async_backtrace::framed]
    async fn do_exchange(&self, req: StreamReq<FlightData>) -> Response<Self::DoExchangeStream> {
        let params_json = req.get_metadata("x-exchange-params")?;
        let params: crate::servers::flight::DoExchangeParams = serde_json::from_str(&params_json)
            .map_err(|e| {
            Status::invalid_argument(format!("Failed to parse DoExchangeParams: {}", e))
        })?;

        let inbound = DataExchangeManager::instance().handle_do_exchange(
            &params.query_id,
            &params.exchange_session_id,
            &params.source_id,
            &params.stream,
        )?;

        let Some(mut inbound) = inbound else {
            let response = tokio_stream::once(Ok(DoExchangeResponse::receiver_closed().encode()));
            return Ok(RawResponse::new(Box::pin(response)));
        };

        let mut stream = req.into_inner();
        let (tx, rx) = async_channel::bounded(1);

        GlobalIORuntime::instance().spawn(async move {
            loop {
                match stream.next().await {
                    None => {
                        // A physical EOF is neither success nor immediate logical failure: it may
                        // be the connection being replaced. Detach before awaiting the response
                        // write so the reconnect lease starts even if that write is backpressured.
                        inbound.disconnect();
                        let _ = tx
                            .send(Err(Status::unavailable(
                                "do_exchange request stream ended before Finish",
                            )))
                            .await;
                        break;
                    }
                    Some(Err(status)) => {
                        inbound.disconnect();
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                    Some(Ok(flight_data)) => {
                        // Malformed or out-of-order packets are logical failures, not replaceable
                        // transport failures. Retrying them would replay the same invalid packet,
                        // so fail the admitted source immediately instead of starting a lease.
                        let request = match DoExchangeRequest::decode(flight_data) {
                            Ok(request) => request,
                            Err(cause) => {
                                inbound.fail(cause.clone());
                                let _ = tx.send(Err(cause.into())).await;
                                break;
                            }
                        };
                        match inbound.handle_request(request).await {
                            Ok(response) => {
                                let receiver_closed =
                                    matches!(&response, DoExchangeResponse::ReceiverClosed);
                                if tx.send(Ok(response.encode())).await.is_err() || receiver_closed
                                {
                                    break;
                                }
                            }
                            Err(cause) => {
                                inbound.fail(cause.clone());
                                let _ = tx.send(Err(cause.into())).await;
                                break;
                            }
                        }
                    }
                }
            }
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
