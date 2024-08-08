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

mod vote_impls {

    use crate::protobuf as pb;
    use crate::raft_types;

    impl From<raft_types::Vote> for pb::Vote {
        fn from(vote: raft_types::Vote) -> Self {
            pb::Vote {
                term: vote.leader_id.term,
                node_id: vote.leader_id.node_id,
                committed: vote.is_committed(),
            }
        }
    }

    impl From<pb::Vote> for raft_types::Vote {
        fn from(vote: pb::Vote) -> Self {
            if vote.committed {
                raft_types::Vote::new_committed(vote.term, vote.node_id)
            } else {
                raft_types::Vote::new(vote.term, vote.node_id)
            }
        }
    }
}

mod log_id_impls {

    use crate::protobuf as pb;
    use crate::raft_types;
    use crate::CommittedLeaderId;

    impl From<raft_types::LogId> for pb::LogId {
        fn from(log_id: raft_types::LogId) -> Self {
            pb::LogId {
                term: log_id.leader_id.term,
                node_id: log_id.leader_id.node_id,
                index: log_id.index,
            }
        }
    }

    impl From<pb::LogId> for raft_types::LogId {
        fn from(log_id: pb::LogId) -> Self {
            raft_types::LogId::new(
                CommittedLeaderId::new(log_id.term, log_id.node_id),
                log_id.index,
            )
        }
    }
}
