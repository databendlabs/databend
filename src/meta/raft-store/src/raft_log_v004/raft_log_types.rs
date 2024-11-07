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

use std::ops::Deref;

use databend_common_meta_types::raft_types;
use deepsize::DeepSizeOf;

use crate::raft_log_v004::callback::Callback;
use crate::raft_log_v004::codec_wrapper::Cw;
use crate::raft_log_v004::log_store_meta::LogStoreMeta;

/// Defines the types used by RaftLog implementation
#[derive(PartialEq, Eq, Default, Clone, Debug)]
pub struct RaftLogTypes;

impl raft_log::Types for RaftLogTypes {
    type LogId = Cw<raft_types::LogId>;
    type LogPayload = Cw<raft_types::EntryPayload>;
    type Vote = Cw<raft_types::Vote>;
    type Callback = Callback;
    type UserData = LogStoreMeta;

    fn log_index(log_id: &Self::LogId) -> u64 {
        log_id.index
    }

    fn payload_size(payload: &Self::LogPayload) -> u64 {
        let size = match payload.deref() {
            raft_types::EntryPayload::Blank => 0,
            raft_types::EntryPayload::Normal(log_entry) => log_entry.deep_size_of(),
            raft_types::EntryPayload::Membership(_) => size_of::<raft_types::Membership>(),
        };

        size as u64
    }
}
