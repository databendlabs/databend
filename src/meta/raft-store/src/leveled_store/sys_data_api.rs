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

use std::collections::BTreeMap;

use databend_common_meta_types::LogId;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::StoredMembership;

/// APIs to access the non-user-data of the state machine(leveled map).
pub trait SysDataApiRO {
    fn curr_seq(&self) -> u64;

    fn last_applied_ref(&self) -> &Option<LogId>;

    fn last_membership_ref(&self) -> &StoredMembership;

    fn nodes_ref(&self) -> &BTreeMap<NodeId, Node>;
}
