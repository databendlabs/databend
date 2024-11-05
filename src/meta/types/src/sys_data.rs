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

use log::debug;

use crate::LogId;
use crate::Node;
use crate::NodeId;
use crate::StoredMembership;

/// Snapshot System data(non-user data).
///
/// System data is **NOT** leveled. At each level, there is a complete copy of the system data.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
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

impl SysData {
    pub fn curr_seq(&self) -> u64 {
        self.sequence
    }

    pub fn update_seq(&mut self, seq: u64) {
        self.sequence = seq;
    }

    /// Increase the sequence number by 1 and return the updated value
    pub fn next_seq(&mut self) -> u64 {
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

    pub fn last_applied_ref(&self) -> &Option<LogId> {
        &self.last_applied
    }

    pub fn last_membership_ref(&self) -> &StoredMembership {
        &self.last_membership
    }

    pub fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }
}
