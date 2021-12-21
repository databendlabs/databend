use std::pin::Pin;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_flight_rpc::FlightClaim;
use serde::Serialize;
use tonic::codegen::futures_core::Stream;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::executor::ActionHandler;
use crate::executor::ReplySerializer;
use crate::meta_service::MetaNode;
use crate::protobuf::grpc_meta_service_server::GrpcMetaService;
use crate::protobuf::ActionReply;
use crate::protobuf::ActionReq;
use crate::protobuf::HandshakeRequest;
use crate::protobuf::HandshakeResponse;

pub type GrpcStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct GrpcService {
    action_handler: ActionHandler,
}

impl GrpcService {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            action_handler: ActionHandler::create(meta_node),
        }
    }

    fn do_check_token(&self, metadata: &MetadataMap) -> Result<FlightClaim, Status> {
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

    fn check_token(&self, mut req: Request<ActionReq>) -> Result<Request<ActionReq>, Status> {
        self.do_check_token(req.metadata()); // TODO(ariesdevil)
        Ok(req)
    }
}

#[tonic::async_trait]
impl GrpcMetaService for GrpcService {
    async fn do_action(
        &self,
        request: Request<ActionReq>,
    ) -> Result<Response<ActionReply>, Status> {
        todo!()
    }

    type HandshakeStream = GrpcStream<HandshakeResponse>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        todo!()
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
