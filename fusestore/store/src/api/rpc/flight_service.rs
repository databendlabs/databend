// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow_flight;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::ActionType;
use common_arrow::arrow_flight::BasicAuth;
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
use common_flights::flight_result_to_str;
use common_flights::FlightClaim;
use common_flights::FlightToken;
use common_flights::StoreDoAction;
use common_flights::StoreDoActionResult;
use common_flights::StoreDoGet;
use futures::Stream;
use futures::StreamExt;
use log::info;
use prost::Message;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::configs::Config;
use crate::executor::ActionHandler;
use crate::fs::IFileSystem;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

/// StoreFlightImpl provides data access API-s for FuseQuery, in arrow-flight protocol.
pub struct StoreFlightImpl {
    token: FlightToken,
    action_handler: ActionHandler,
}

impl StoreFlightImpl {
    pub fn create(_conf: Config, fs: Arc<dyn IFileSystem>) -> Self {
        Self {
            token: FlightToken::create(),
            // TODO pass in action handler
            action_handler: ActionHandler::create(fs),
        }
    }

    fn check_token(&self, metadata: &MetadataMap) -> Result<FlightClaim, Status> {
        let token = metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .ok_or_else(|| Status::internal("Error auth-token-bin is empty"))?;

        let claim = self
            .token
            .try_verify_token(token)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(claim)
    }
}

#[async_trait::async_trait]
impl FlightService for StoreFlightImpl {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let req = request
            .into_inner()
            .next()
            .await
            .ok_or_else(|| Status::internal("Error request next is None"))??;

        let HandshakeRequest { payload, .. } = req;
        let auth = BasicAuth::decode(&*payload).map_err(|e| Status::internal(e.to_string()))?;

        // Check auth and create token.
        let user = "root";
        if auth.username == user {
            let claim = FlightClaim {
                username: user.to_string(),
            };
            let token = self
                .token
                .try_create_token(claim)
                .map_err(|e| Status::internal(e.to_string()))?;

            let resp = HandshakeResponse {
                payload: token.into_bytes(),
                ..HandshakeResponse::default()
            };
            let output = futures::stream::once(async { Ok(resp) });
            Ok(Response::new(Box::pin(output)))
        } else {
            Err(Status::unauthenticated(format!(
                "Don't know user {}",
                auth.username
            )))
        }
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

    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, tonic::Status>> + Send + Sync + 'static>>;
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Check token.
        let _claim = self.check_token(&request.metadata())?;

        // Action.
        let action: StoreDoGet = request.try_into()?;
        match action {
            StoreDoGet::Read(act) => {
                let stream =
                    self.action_handler.read_partition(act).await.map_err(|e| {
                        Status::internal(format!("read failure: {}", e.to_string()))
                    })?;
                Ok(Response::new(Box::pin(stream)))
            }
            StoreDoGet::Pull(pull) => {
                let key = pull.key;

                let (tx, rx): (
                    Sender<Result<FlightData, tonic::Status>>,
                    Receiver<Result<FlightData, tonic::Status>>,
                ) = tokio::sync::mpsc::channel(2);

                self.action_handler.do_pull_file(key, tx).await?;

                Ok(Response::new(
                    Box::pin(ReceiverStream::new(rx)) as Self::DoGetStream
                ))
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let _claim = self.check_token(&request.metadata())?;
        let meta = request.metadata();

        let (db_name, tbl_name) =
            common_flights::get_do_put_meta(meta).map_err(|e| Status::internal(e.to_string()))?;

        let append_res = self
            .action_handler
            .do_put(db_name, tbl_name, request.into_inner())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let bytes = serde_json::to_vec(&append_res).map_err(|e| Status::internal(e.to_string()))?;
        let put_res = PutResult {
            app_metadata: bytes,
        };

        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(put_res)
        }))))
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
        // Check token.
        let _claim = self.check_token(&request.metadata())?;

        let action: StoreDoAction = request.try_into()?;
        info!("Receive do_action: {:?}", action);
        let rst = self.action_handler.execute(action).await?;

        self.once_stream_resp(rst)
    }

    type ListActionsStream = FlightStream<ActionType>;
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

impl StoreFlightImpl {
    fn once_stream_resp(
        &self,
        action_rst: StoreDoActionResult,
    ) -> Result<Response<FlightStream<arrow_flight::Result>>, Status> {
        let rst = arrow_flight::Result::from(action_rst);

        info!("oneshot Result stream: {:}", flight_result_to_str(&rst));

        let output = futures::stream::once(async { Ok(rst) });

        Ok(Response::new(Box::pin(output)))
    }
}
