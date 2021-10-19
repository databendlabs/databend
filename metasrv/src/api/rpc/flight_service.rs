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

use common_arrow::arrow_format::flight;
use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::ActionType;
use common_arrow::arrow_format::flight::data::BasicAuth;
use common_arrow::arrow_format::flight::data::Criteria;
use common_arrow::arrow_format::flight::data::Empty;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::data::FlightDescriptor;
use common_arrow::arrow_format::flight::data::FlightInfo;
use common_arrow::arrow_format::flight::data::HandshakeRequest;
use common_arrow::arrow_format::flight::data::HandshakeResponse;
use common_arrow::arrow_format::flight::data::PutResult;
use common_arrow::arrow_format::flight::data::SchemaResult;
use common_arrow::arrow_format::flight::data::Ticket;
use common_arrow::arrow_format::flight::service::flight_service_server::FlightService;
use common_flight_rpc::FlightClaim;
use common_flight_rpc::FlightToken;
use common_meta_flight::MetaFlightAction;
use common_tracing::tracing;
use futures::Stream;
use futures::StreamExt;
use log::info;
use prost::Message;
use serde::Serialize;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::configs::Config;
use crate::executor::ActionHandler;
use crate::executor::ReplySerializer;
use crate::meta_service::MetaNode;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

/// MetaFlightImpl provides data access API-s for DatabendQuery, in arrow-flight protocol.
pub struct MetaFlightImpl {
    token: FlightToken,
    action_handler: ActionHandler,
}

impl MetaFlightImpl {
    pub fn create(_conf: Config, meta_node: Arc<MetaNode>) -> Self {
        Self {
            token: FlightToken::create(),
            // TODO pass in action handler
            action_handler: ActionHandler::create(meta_node),
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
impl FlightService for MetaFlightImpl {
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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        todo!()
    }

    type DoPutStream = FlightStream<PutResult>;
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        todo!()
    }

    type DoExchangeStream = FlightStream<FlightData>;
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }

    type DoActionStream = FlightStream<flight::data::Result>;

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Check token.
        let _claim = self.check_token(request.metadata())?;

        common_tracing::extract_remote_span_as_parent(&request);

        let action: MetaFlightAction = request.try_into()?;
        info!("Receive do_action: {:?}", action);

        let s = JsonSer;
        let body = self.action_handler.execute(action, s).await?;
        let arrow = flight::data::Result { body };
        let output = futures::stream::once(async { Ok(arrow) });
        Ok(Response::new(Box::pin(output)))
    }

    type ListActionsStream = FlightStream<ActionType>;
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

struct JsonSer;
impl ReplySerializer for JsonSer {
    type Output = Vec<u8>;
    fn serialize<T>(&self, v: T) -> common_exception::Result<Self::Output>
    where T: Serialize {
        let v = serde_json::to_vec(&v)?;
        Ok(v)
    }
}
