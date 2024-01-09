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

use log::error;
use tonic::metadata::MetadataValue;

use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;
use crate::Endpoint;
use crate::RaftError;

const HEADER_LEADER: &str = "x-databend-meta-leader-endpoint";

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
        if let Some(endpoint) = endpoint {
            let metadata = reply.metadata_mut();
            metadata.insert_bin(
                HEADER_LEADER,
                MetadataValue::from_bytes(endpoint.to_string().as_bytes()),
            );
        }
    }

    /// Retrieve leader endpoint from the reply.
    pub fn get_response_meta_leader<T>(reply: &tonic::Response<T>) -> Option<Endpoint> {
        let metadata = reply.metadata();

        let Some(values) = metadata.get(HEADER_LEADER) else {
            return None;
        };

        match values.to_str() {
            Ok(value) => {
                let endpoint = Endpoint::parse(value);

                match endpoint {
                    Ok(endpoint) => Some(endpoint),
                    Err(e) => {
                        error!("invalid response leader endpoint: {}, error: {}", value, e);
                        None
                    }
                }
            }
            Err(e) => {
                error!("invalid response leader endpoint, error: {}", e);
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
        E: serde::Serialize + serde::de::DeserializeOwned,
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

    /// Parse tonic::Request and decode it into required type.
    pub fn parse_req<T>(request: tonic::Request<RaftRequest>) -> Result<T, tonic::Status>
    where T: serde::de::DeserializeOwned {
        let raft_req = request.into_inner();
        Self::parse(&raft_req.data)
    }

    /// Create an Ok response for raft API.
    pub fn ok_response<D>(d: D) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where D: serde::Serialize {
        let data = serde_json::to_string(&d).expect("fail to serialize resp");
        let reply = RaftReply {
            data,
            error: "".to_string(),
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
