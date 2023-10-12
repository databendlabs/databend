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

use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;

pub struct GrpcHelper;

impl GrpcHelper {
    /// Parse tonic::Request and decode it into required type.
    pub fn parse_req<T>(request: tonic::Request<RaftRequest>) -> Result<T, tonic::Status>
    where T: serde::de::DeserializeOwned {
        let raft_req = request.into_inner();
        let req: T = serde_json::from_str(&raft_req.data).map_err(Self::invalid_arg)?;
        Ok(req)
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

    /// Create a tonic::Status with invalid argument error.
    pub fn invalid_arg(e: impl Error) -> tonic::Status {
        tonic::Status::invalid_argument(e.to_string())
    }

    /// Create a tonic::Status with internal error.
    pub fn internal_err(e: impl Error) -> tonic::Status {
        tonic::Status::internal(e.to_string())
    }
}
