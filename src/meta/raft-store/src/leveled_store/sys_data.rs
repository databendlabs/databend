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

use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::StoredMembership;

use crate::leveled_store::sys_data_api::SysDataApiRO;

impl<T> SysDataApiRO for T
where T: AsRef<SysData>
{
    fn curr_seq(&self) -> u64 {
        self.as_ref().curr_seq()
    }

    fn last_applied_ref(&self) -> &Option<LogId> {
        self.as_ref().last_applied_ref()
    }

    fn last_membership_ref(&self) -> &StoredMembership {
        self.as_ref().last_membership_ref()
    }

    fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        self.as_ref().nodes_ref()
    }
}
