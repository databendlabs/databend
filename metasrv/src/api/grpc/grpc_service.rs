use std::sync::Arc;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_flight_rpc::FlightClaim;
use common_flight_rpc::FlightToken;
use common_meta_raft_store::MetaGrpcWriteAction;
use common_meta_types::protobuf::meta_server::Meta;
use common_meta_types::protobuf::GetReply;
use common_meta_types::protobuf::GetReq;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::HandshakeResponse;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_tracing::tracing;
use futures::StreamExt;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::executor::ActionHandler;
use crate::meta_service::meta_service_impl::GrpcStream;
use crate::meta_service::MetaNode;

pub struct MetaGrpcImpl {
    token: FlightToken,
    action_handler: ActionHandler,
}

impl MetaGrpcImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            token: FlightToken::create(),
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
impl Meta for MetaGrpcImpl {
    // rpc handshake related type
    type HandshakeStream = GrpcStream<HandshakeResponse>;

    // rpc handshake first
    #[tracing::instrument(level = "info", skip(self))]
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
                "Unknown user: {}",
                auth.username
            )))
        }
    }

    async fn write_msg(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        self.check_token(request.metadata())?;
        common_tracing::extract_remote_span_as_parent(&request);

        let action: MetaGrpcWriteAction = request.try_into()?;
        tracing::info!("Receive write_action: {:?}", action);

        let body = self.action_handler.execute_write(action).await;
        Ok(Response::new(body))
    }

    async fn get_msg(&self, request: Request<GetReq>) -> Result<Response<GetReply>, Status> {
        todo!()
    }
}
