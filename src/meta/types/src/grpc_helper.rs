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

//! Helper functions for handling grpc.

use std::error::Error;
use std::str::FromStr;

use log::error;
use tonic::metadata::MetadataValue;

use crate::Endpoint;
use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;
use crate::raft_types::RaftError;

const HEADER_LEADER: &str = "x-databend-meta-leader-grpc-endpoint";
// const HEADER_LEADER_BIN: &str = "x-databend-meta-leader-grpc-endpoint-bin";

pub struct GrpcHelper;

impl GrpcHelper {
    /// Inject span into a tonic request, so that on the remote peer the tracing context can be restored.
    pub fn traced_req<T>(t: T) -> tonic::Request<T> {
        let req = tonic::Request::new(t);
        databend_common_tracing::inject_span_to_tonic_request(req)
    }

    /// Add leader endpoint to the reply to inform the client to contact the leader directly.
    pub fn add_response_meta_leader<T>(
        reply: &mut tonic::Response<T>,
        endpoint: Option<&Endpoint>,
    ) {
        Self::insert_endpoint_to_metadata(reply.metadata_mut(), endpoint, "building a response");
    }

    /// Create a Status error with leader endpoint in metadata.
    /// Similar to `add_response_meta_leader` but for error Status.
    pub fn status_forward_to_leader(endpoint: Option<&Endpoint>) -> tonic::Status {
        let mut status = tonic::Status::unavailable("not leader");
        Self::insert_endpoint_to_metadata(status.metadata_mut(), endpoint, "building a status for leader redirection");
        status
    }

    /// Insert leader endpoint into metadata for client redirection.
    fn insert_endpoint_to_metadata(
        metadata: &mut tonic::metadata::MetadataMap,
        endpoint: Option<&Endpoint>,
        ctx: &str,
    ) {
        let Some(endpoint) = endpoint else {
            log::warn!(
                "no leader endpoint available for client redirection; when:({})",
                ctx
            );
            return;
        };

        match MetadataValue::from_str(&endpoint.to_string()) {
            Ok(v) => {
                metadata.insert(HEADER_LEADER, v);
            }
            Err(err) => {
                error!(
                    "fail to insert leader endpoint({:?}) to metadata: {}; when:({})",
                    endpoint, err, ctx
                );
            }
        }
    }

    /// Parse leader endpoint from metadata map.
    pub fn parse_leader_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<Endpoint> {
        let meta_leader = metadata.get(HEADER_LEADER)?;

        let s = match meta_leader.to_str() {
            Ok(x) => x,
            Err(err) => {
                error!(
                    "invalid leader endpoint in metadata({:?}), error: {}",
                    meta_leader, err
                );
                return None;
            }
        };

        match Endpoint::parse(s) {
            Ok(endpoint) => Some(endpoint),
            Err(e) => {
                error!("invalid leader endpoint: {}, error: {}", s, e);
                None
            }
        }
    }

    pub fn encode_raft_request<T>(v: &T) -> Result<RaftRequest, serde_json::Error>
    where T: serde::Serialize + 'static {
        let data = serde_json::to_string(&v)?;
        Ok(RaftRequest { data })
    }

    pub fn parse_raft_reply<T, E>(
        reply: tonic::Response<RaftReply>,
    ) -> Result<Result<T, RaftError<E>>, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
        E: serde::de::DeserializeOwned,
    {
        let raft_reply = reply.into_inner();

        if !raft_reply.error.is_empty() {
            let e: RaftError<E> = serde_json::from_str(&raft_reply.error)?;
            Ok(Err(e))
        } else {
            let d: T = serde_json::from_str(&raft_reply.data)?;
            Ok(Ok(d))
        }
    }

    /// Parse RaftReply to `Result<T,E>`
    pub fn parse_raft_reply_generic<T, E>(
        reply: RaftReply,
    ) -> Result<Result<T, E>, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
        E: serde::Serialize + serde::de::DeserializeOwned,
    {
        if !reply.error.is_empty() {
            let e: E = serde_json::from_str(&reply.error)?;
            Ok(Err(e))
        } else {
            let d: T = serde_json::from_str(&reply.data)?;
            Ok(Ok(d))
        }
    }

    /// Parse tonic::Request and decode it into required type.
    pub fn parse_req<T>(request: tonic::Request<RaftRequest>) -> Result<T, tonic::Status>
    where T: serde::de::DeserializeOwned {
        let raft_req = request.into_inner();
        Self::parse(&raft_req.data)
    }

    /// Make a gRPC response result with Ok, ApiError or internal error.
    pub fn make_grpc_result<D, E>(
        res: Result<D, RaftError<E>>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where
        D: serde::Serialize + 'static,
        E: serde::Serialize + Error + 'static,
    {
        match res {
            Ok(resp) => GrpcHelper::ok_response(&resp),
            Err(e) => {
                if e.api_error().is_some() {
                    GrpcHelper::err_response(&e)
                } else {
                    Err(GrpcHelper::internal_err(e))
                }
            }
        }
    }

    /// Create an Ok response for raft API.
    pub fn ok_response<D>(d: &D) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where D: serde::Serialize + 'static {
        let data = serde_json::to_string(d).expect("fail to serialize resp");
        let reply = RaftReply {
            data,
            error: "".to_string(),
        };
        Ok(tonic::Response::new(reply))
    }

    /// Create an Ok response contains API error.
    pub fn err_response<E>(e: &E) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where E: serde::Serialize + 'static {
        let error = serde_json::to_string(e).expect("fail to serialize response error");
        let reply = RaftReply {
            data: "".to_string(),
            error,
        };
        Ok(tonic::Response::new(reply))
    }

    /// Parse string and decode it into required type.
    pub fn parse<T>(s: &str) -> Result<T, tonic::Status>
    where T: serde::de::DeserializeOwned {
        let req: T = serde_json::from_str(s).map_err(Self::invalid_arg)?;
        Ok(req)
    }

    /// Create a tonic::Status with invalid argument error.
    pub fn invalid_arg(e: impl ToString) -> tonic::Status {
        tonic::Status::invalid_argument(e.to_string())
    }

    /// Create a tonic::Status with internal error.
    pub fn internal_err(e: impl Error) -> tonic::Status {
        tonic::Status::internal(e.to_string())
    }
}
