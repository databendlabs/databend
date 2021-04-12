// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// use std::collections::HashMap;
use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow_flight;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
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
use common_flights::store_do_action::CreateDatabaseActionResult;
use common_flights::store_do_action::StoreDoAction;
use common_flights::store_do_action::StoreDoActionResult;
use common_flights::store_do_get::StoreDoGet;
use common_flights::CreateTableActionResult;
use common_flights::FlightClaim;
use common_flights::FlightToken;
use futures::Stream;
use futures::StreamExt;
#[allow(unused_imports)]
use log::error;
#[allow(unused_imports)]
use log::info;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::configs::Config;
use crate::engine::MemEngine;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Table;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct FlightServiceImpl {
    token: FlightToken,
    meta: Arc<Mutex<MemEngine>>,
}

impl FlightServiceImpl {
    pub fn create(_conf: Config) -> Self {
        Self {
            token: FlightToken::create(),
            meta: MemEngine::create(),
        }
    }

    pub fn make_server(self) -> FlightServiceServer<impl FlightService> {
        FlightServiceServer::new(self)
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
impl FlightService for FlightServiceImpl {
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
            StoreDoGet::Read(_) => Err(Status::internal("Store read unimplemented")),
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
            StoreDoAction::ReadPlan(_) => Err(Status::internal("Store read plan unimplemented")),
            StoreDoAction::CreateDatabase(a) => {
                let plan = a.plan;
                let mut meta = self.meta.lock().unwrap();
                let database_id = meta
                    .create_database(plan)
                    .map_err(|e| Status::internal(e.to_string()))?;

                let action_rst =
                    StoreDoActionResult::CreateDatabase(CreateDatabaseActionResult { database_id });
                let rst = arrow_flight::Result::from(action_rst);
                info!("single Result stream: {:}", flight_result_to_str(&rst));
                let output = futures::stream::once(async { Ok(rst) });

                Ok(Response::new(Box::pin(output)))
            }
            StoreDoAction::CreateTable(a) => {
                let plan = a.plan;
                let db_name = plan.db;
                let table_name = plan.table;

                info!("create table: {:}: {:?}", db_name, table_name);

                let mut meta = self.meta.lock().unwrap();

                let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();
                let flight_data =
                    arrow_flight::utils::flight_data_from_arrow_schema(&plan.schema, &options);

                let table = Table {
                    // the storage engine fills the id.
                    table_id: -1,
                    schema: flight_data.data_header,
                    // TODO
                    placement_policy: vec![],
                };

                let cmd = CmdCreateTable {
                    // TODO not used
                    db_id: -1,
                    db_name,
                    table_name,
                    table: Some(table),
                };

                let table_id = meta.create_table(cmd, plan.if_not_exists)?;

                let action_rst =
                    StoreDoActionResult::CreateTable(CreateTableActionResult { table_id });
                let rst = arrow_flight::Result::from(action_rst);

                info!("single Result stream: {:}", flight_result_to_str(&rst));

                let output = futures::stream::once(async { Ok(rst) });

                Ok(Response::new(Box::pin(output)))
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
