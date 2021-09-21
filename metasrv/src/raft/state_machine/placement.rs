// Copyright 2020 Datafuse Labs.
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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use crate::meta_service::NodeId;
use crate::raft::state_machine::Node;
use crate::raft::state_machine::Slot;

/// IPlacement defines the behavior of an algo to assign file to nodes.
/// An placement algo considers the replication config, such as number of copies,
/// and workload balancing etc.
///
/// A placement algo is a two-level mapping:
/// The first is about how to assign files(identified by keys) to a virtual bucket(AKA slot),
/// and the second is about how to assign a slot to nodes.
///
/// Data migration should be impl on only the second level, in which way only a small piece of metadata
/// will be modified when repairing a damaged server or when extending the cluster.
///
/// A default consistent-hash like impl is provided for most cases.
/// With this algo user only need to impl two methods: get_slots() and get_node().
pub trait Placement {
    /// Returns the Node-s that are responsible to store a copy of a file.
    fn nodes_to_store_key(&self, key: &str) -> Vec<Node> {
        // TODO(xp): handle error instead of using unwrap
        let slot_idx = self.slot_index_for_key(key);
        let slot = self.get_slot(slot_idx);

        slot.node_ids
            .iter()
            .map(|nid| self.get_placement_node(nid).unwrap().unwrap())
            .collect()
    }

    /// Returns the slot index to store a file.
    fn slot_index_for_key(&self, key: &str) -> u64 {
        // TODO use consistent hash if need to extend cluster.
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hsh = hasher.finish();
        hsh % self.get_slots().len() as u64
    }

    fn get_slot(&self, slot_idx: u64) -> &Slot {
        &self.get_slots()[slot_idx as usize]
    }

    fn get_slots(&self) -> &[Slot];

    /// Returns a node.
    fn get_placement_node(&self, node_id: &NodeId) -> common_exception::Result<Option<Node>>;
}

/// Evenly chooses `n` elements from `m` elements
pub fn rand_n_from_m(m: usize, n: usize) -> anyhow::Result<Vec<usize>> {
    if m < n {
        return Err(anyhow::anyhow!("m={} must >= n={}", m, n));
    }

    let mut chosen = Vec::with_capacity(n);

    let mut need = n;
    for i in 0..m {
        if rand::random::<usize>() % (m - i) < need {
            chosen.push(i);
            need -= 1;
        }
    }

    Ok(chosen)
}
