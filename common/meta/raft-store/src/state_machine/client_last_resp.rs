// Copyright 2021 Datafuse Labs.
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

use serde::Deserialize;
use serde::Serialize;

use crate::state_machine::AppliedState;

/// Client last response that is stored in SledTree
/// raft state: A mapping of client serial IDs to their state info:
/// (serial, RaftResponse)
/// This is used to de-dup client request, to impl idempotent operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientLastRespValue {
    pub req_serial_num: u64,
    pub res: AppliedState,
}
