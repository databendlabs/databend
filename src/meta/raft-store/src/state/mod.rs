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

//! Raft state includes some essential information about raft, such as term, voted_for
mod raft_state;
pub(crate) mod raft_state_kv;

pub use raft_state::RaftState;
pub use raft_state::TREE_RAFT_STATE;
pub use raft_state_kv::RaftStateKey;
pub use raft_state_kv::RaftStateValue;
