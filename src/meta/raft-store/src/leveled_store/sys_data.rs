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
use log::debug;

use crate::leveled_store::sys_data_api::SysDataApiRO;

/// System data(non-user data).
///
/// System data is **NOT** leveled. At each level, there is a complete copy of the system data.
#[derive(Debug, Default, Clone)]
pub struct SysData {
    /// The last applied log id.
    last_applied: Option<LogId>,

    /// The last applied membership log.
    last_membership: StoredMembership,

    /// All of the nodes in this cluster, including the voters and learners.
    nodes: BTreeMap<NodeId, Node>,

    /// The sequence number for every [`SeqV`] value that is stored in this state machine.
    ///
    /// A seq is globally unique and monotonically increasing.
    sequence: u64,
}

impl SysDataApiRO for SysData {
    fn curr_seq(&self) -> u64 {
        self.sequence
    }

    fn last_applied_ref(&self) -> &Option<LogId> {
        &self.last_applied
    }

    fn last_membership_ref(&self) -> &StoredMembership {
        &self.last_membership
    }

    fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }
}

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

impl SysData {
    pub(crate) fn update_seq(&mut self, seq: u64) {
        self.sequence = seq;
    }

    /// Increase the sequence number by 1 and return the updated value
    pub(crate) fn next_seq(&mut self) -> u64 {
        self.sequence += 1;
        debug!("next_seq: {}", self.sequence);
        // dbg!("next_seq", self.sequence);

        self.sequence
    }

    pub fn last_applied_mut(&mut self) -> &mut Option<LogId> {
        &mut self.last_applied
    }

    pub fn last_membership_mut(&mut self) -> &mut StoredMembership {
        &mut self.last_membership
    }

    pub fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        &mut self.nodes
    }
}
