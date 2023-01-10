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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::Result;
use rand::prelude::SliceRandom;
use rand::thread_rng;

#[typetag::serde(tag = "type")]
pub trait PartInfo: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    #[allow(clippy::borrowed_box)]
    fn equals(&self, info: &Box<dyn PartInfo>) -> bool;

    /// Used for partition distributed.
    fn hash(&self) -> u64;
}

impl Debug for Box<dyn PartInfo> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(str) => write!(f, "{}", str),
            Err(_cause) => Err(std::fmt::Error {}),
        }
    }
}

impl PartialEq for Box<dyn PartInfo> {
    fn eq(&self, other: &Self) -> bool {
        let this_type_id = self.as_any().type_id();
        let other_type_id = other.as_any().type_id();

        match this_type_id == other_type_id {
            true => self.equals(other),
            false => false,
        }
    }
}

#[allow(dead_code)]
pub type PartInfoPtr = Arc<Box<dyn PartInfo>>;

/// For cache affinity, we consider some strategies when reshuffle partitions.
/// For example:
/// Under PartitionsShuffleKind::Mod, the same partition is always routed to the same executor.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub enum PartitionsShuffleKind {
    // Bind the Partition to executor one by one with order.
    Seq,
    // Bind the Partition to executor by partition.hash()%executor_nums order.
    Mod,
    // Bind the Partition to executor by partition.rand() order.
    Rand,
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Partitions {
    pub kind: PartitionsShuffleKind,
    pub partitions: Vec<PartInfoPtr>,
}

impl Partitions {
    pub fn create(kind: PartitionsShuffleKind, partitions: Vec<PartInfoPtr>) -> Self {
        Partitions { kind, partitions }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn reshuffle(&self, executors: Vec<String>) -> Result<HashMap<String, Partitions>> {
        let mut executors_sort = executors;
        executors_sort.sort();

        let executor_nums = executors_sort.len();
        let partitions = match self.kind {
            PartitionsShuffleKind::Seq => self.partitions.clone(),
            PartitionsShuffleKind::Mod => {
                // Sort by hash%executor_nums.
                let mut parts = self.partitions.clone();
                parts.sort_by(|a, b| {
                    let hl = a.hash() % executor_nums as u64;
                    let hr = b.hash() % executor_nums as u64;
                    hl.cmp(&hr)
                });
                parts
            }
            PartitionsShuffleKind::Rand => {
                let mut rng = thread_rng();
                let mut parts = self.partitions.clone();
                parts.shuffle(&mut rng);
                parts
            }
        };

        let mut executor_part = HashMap::default();
        let parts_per_node = partitions.len() / executor_nums;
        for (idx, executor) in executors_sort.iter().enumerate() {
            let begin = parts_per_node * idx;
            let end = parts_per_node * (idx + 1);
            let mut parts = partitions[begin..end].to_vec();

            if idx == executor_nums - 1 {
                // For some irregular partitions, we assign them to the last node
                let begin = parts_per_node * executor_nums;
                parts.extend_from_slice(&partitions[begin..]);
            }
            executor_part.insert(
                executor.clone(),
                Partitions::create(PartitionsShuffleKind::Seq, parts.to_vec()),
            );
        }
        Ok(executor_part)
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            kind: PartitionsShuffleKind::Seq,
            partitions: vec![],
        }
    }
}
