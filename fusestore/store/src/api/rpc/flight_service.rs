// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::pin::Pin;

use common_arrow::arrow_flight::{
    self,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, BasicAuth, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use common_flights::store_do_action::StoreDoAction;
use common_flights::store_do_get::StoreDoGet;
use futures::{SinkExt, Stream, StreamExt};
use log::info;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

use crate::api::rpc::{FlightClaim, FlightToken};
use crate::configs::Config;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct FlightService {
    token: FlightToken,
}

impl FlightService {
    pub fn create(_conf: Config) -> Self {
        Self {
            token: FlightToken::create(),
        }
    }

    pub fn make_server(self) -> FlightServer<impl Flight> {
        FlightServer::new(self)
    }

    fn check_token(&self, metadata: &MetadataMap) -> Result<FlightClaim, Status> {
        let token = metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .unwrap();

        let claim = self
            .token
            .try_verify_token(token)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(claim)
    }
}

#[async_trait::async_trait]
impl Flight for FlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let (tx, rx) = futures::channel::mpsc::channel(10);

        let token = self.token.clone();
        tokio::spawn({
            async move {
                let requests = request.into_inner();
                requests
                    .for_each(move |req| {
                        let mut tx = tx.clone();
                        let req = req.expect("Error reading handshake request");
                        let HandshakeRequest { payload, .. } = req;
                        let auth =
                            BasicAuth::decode(&*payload).expect("Error parsing handshake request");

                        // Token.
                        let user = "root";
                        let resp = if auth.username == user {
                            let token = token
                                .try_create_token(FlightClaim {
                                    user_is_admin: false,
                                    username: user.to_string(),
                                })
                                .expect("Error create token");

                            Ok(HandshakeResponse {
                                payload: token.into_bytes(),
                                ..HandshakeResponse::default()
                            })
                        } else {
                            Err(Status::unauthenticated(format!(
                                "Don't know user {}",
                                auth.username
                            )))
                        };
                        async move {
                            tx.send(resp)
                                .await
                                .expect("Error sending handshake response");
                        }
                    })
                    .await;
            }
        });

        Ok(Response::new(Box::pin(rx)))
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
        // Check token.
        let _claim = self.check_token(&request.metadata())?;

        // Action.
        let action: StoreDoGet = request.try_into()?;
        match action {
            StoreDoGet::Read(_) => {
                todo!()
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
        // Check token.
        let _claim = self.check_token(&request.metadata())?;

        // Action.
        let action: StoreDoAction = request.try_into()?;
        info!("Receive do_action: {:?}", action);

        match action {
            StoreDoAction::CreateDatabase(_) => {
                Err(Status::internal("Store create database unimplemented"))
            }
            StoreDoAction::CreateTable(_) => {
                Err(Status::internal("Store create table unimplemented"))
            }
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
