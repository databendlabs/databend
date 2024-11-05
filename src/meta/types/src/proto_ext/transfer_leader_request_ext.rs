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

use tonic::Status;

use crate::protobuf as pb;
use crate::raft_types::TransferLeaderRequest;
use crate::Vote;

impl From<TransferLeaderRequest> for pb::TransferLeaderRequest {
    fn from(req: TransferLeaderRequest) -> Self {
        pb::TransferLeaderRequest {
            from: Some(pb::Vote::from(*req.from_leader())),
            to: *req.to_node_id(),
            last_log_id: req.last_log_id().copied().map(pb::LogId::from),
        }
    }
}

impl TryFrom<pb::TransferLeaderRequest> for TransferLeaderRequest {
    /// Converting pb to rust type is done when receiving a request,
    /// thus we just return a tonic status error.
    type Error = Status;

    fn try_from(req: pb::TransferLeaderRequest) -> Result<Self, Status> {
        let from = req
            .from
            .ok_or_else(|| Status::invalid_argument("missing from"))?;

        let r =
            TransferLeaderRequest::new(Vote::from(from), req.to, req.last_log_id.map(|x| x.into()));
        Ok(r)
    }
}
