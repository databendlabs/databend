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

use crate::protobuf as pb;
use crate::raft_types;

impl From<raft_types::VoteResponse> for pb::VoteResponse {
    fn from(resp: raft_types::VoteResponse) -> Self {
        pb::VoteResponse {
            vote: Some(resp.vote.into()),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(|log_id| log_id.into()),
        }
    }
}

impl From<pb::VoteResponse> for raft_types::VoteResponse {
    fn from(resp: pb::VoteResponse) -> Self {
        let vote: raft_types::Vote = resp.vote.unwrap_or_default().into();
        let last_log_id = resp.last_log_id.map(|log_id| log_id.into());
        raft_types::VoteResponse::new(vote, last_log_id, resp.vote_granted)
    }
}
