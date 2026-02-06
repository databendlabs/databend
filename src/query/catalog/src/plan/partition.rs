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

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_meta_types::NodeInfo;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use parking_lot::RwLock;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use sha2::Digest;

use crate::plan::PartStatistics;
use crate::table_context::TableContext;

/// Partition information.
#[derive(PartialEq)]
pub enum PartInfoType {
    // Block level partition information.
    // Read the data from the block level.
    BlockLevel,
    // In lazy level, we need:
    // 1. read the block location information from the segment.
    // 2. read the block data from the block level.
    LazyLevel,
}

#[typetag::serde(tag = "type")]
pub trait PartInfo: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    #[allow(clippy::borrowed_box)]
    fn equals(&self, info: &Box<dyn PartInfo>) -> bool;

    /// Used for partition distributed.
    fn hash(&self) -> u64;

    /// Get the partition type.
    /// Default is block level.
    /// If the partition is lazy level, it should be override.
    fn part_type(&self) -> PartInfoType {
        PartInfoType::BlockLevel
    }
}

impl Debug for Box<dyn PartInfo> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    // Bind the Partition to executor by ConsistentHash(partition.hash()) order.
    ConsistentHash,
    // Bind the Partition to executor by partition.rand() order.
    Rand,
    // Bind the Partition to executor by broadcast
    BroadcastCluster,
    // Bind the Partition to warehouse executor by broadcast
    BroadcastWarehouse,
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

    pub fn reshuffle(&self, executors: Vec<Arc<NodeInfo>>) -> Result<HashMap<String, Partitions>> {
        let mut executors_sorted = executors;
        executors_sorted.sort_by(|left, right| left.cache_id.cmp(&right.cache_id));

        let num_executors = executors_sorted.len();
        let partitions = match self.kind {
            PartitionsShuffleKind::Seq => self.partitions.clone(),
            PartitionsShuffleKind::Mod => {
                // Sort by hash%executor_nums.
                let mut parts = self
                    .partitions
                    .iter()
                    .map(|p| (p.hash() % num_executors as u64, p.clone()))
                    .collect::<Vec<_>>();
                parts.sort_by(|a, b| a.0.cmp(&b.0));
                parts.into_iter().map(|x| x.1).collect()
            }
            PartitionsShuffleKind::ConsistentHash => {
                let mut scale = 0;
                let num_executors = executors_sorted.len();
                const EXPECT_NODES: usize = 100;
                while num_executors << scale < EXPECT_NODES {
                    scale += 1;
                }

                let mut executor_part = executors_sorted
                    .iter()
                    .map(|e| (e.id.clone(), Partitions::default()))
                    .collect::<HashMap<_, _>>();

                let mut ring = executors_sorted
                    .iter()
                    .flat_map(|e| {
                        let mut s = DefaultHasher::new();
                        e.cache_id.hash(&mut s);
                        (0..1 << scale).map(move |i| {
                            i.hash(&mut s);
                            (e.id.clone(), s.finish())
                        })
                    })
                    .collect::<Vec<_>>();

                ring.sort_by(|&(_, a), &(_, b)| a.cmp(&b));

                for p in self.partitions.iter() {
                    let k = p.hash();
                    let idx = ring
                        .binary_search_by(|&(_, h)| h.cmp(&k))
                        .unwrap_or_else(|i| i);
                    let executor = if idx == ring.len() {
                        ring[0].0.clone()
                    } else {
                        ring[idx].0.clone()
                    };
                    let part = executor_part.get_mut(&executor).unwrap();
                    part.partitions.push(p.clone());
                }
                return Ok(executor_part);
            }
            PartitionsShuffleKind::Rand => {
                let mut rng = thread_rng();
                let mut parts = self.partitions.clone();
                parts.shuffle(&mut rng);
                parts
            }
            // the executors will be all nodes in the warehouse if a query is BroadcastWarehouse.
            PartitionsShuffleKind::BroadcastCluster | PartitionsShuffleKind::BroadcastWarehouse => {
                return Ok(executors_sorted
                    .into_iter()
                    .map(|executor| {
                        (
                            executor.id.clone(),
                            Partitions::create(PartitionsShuffleKind::Seq, self.partitions.clone()),
                        )
                    })
                    .collect());
            }
        };

        // If there is only one partition, we prioritize executing the query on the local node.
        if partitions.len() == 1 {
            let mut executor_part = HashMap::default();

            let local_id = &GlobalConfig::instance().query.node_id;
            for executor in executors_sorted.into_iter() {
                let parts = match &executor.id == local_id {
                    true => partitions.clone(),
                    false => vec![],
                };

                executor_part.insert(
                    executor.id.clone(),
                    Partitions::create(PartitionsShuffleKind::Seq, parts),
                );
            }

            return Ok(executor_part);
        }

        // parts_per_executor = num_parts / num_executors
        // remain = num_parts % num_executors
        // part distribution:
        //   executor number      | Part number of each executor
        // ------------------------------------------------------
        // num_executors - remain |   parts_per_executor
        //     remain             |   parts_per_executor + 1
        let num_parts = partitions.len();
        let mut executor_part = HashMap::default();
        for (idx, executor) in executors_sorted.into_iter().enumerate() {
            let begin = num_parts * idx / num_executors;
            let end = num_parts * (idx + 1) / num_executors;
            let parts = if begin == end {
                // reach here only when num_executors > num_parts
                vec![]
            } else {
                partitions[begin..end].to_vec()
            };

            executor_part.insert(
                executor.id.clone(),
                Partitions::create(PartitionsShuffleKind::Seq, parts),
            );
        }

        Ok(executor_part)
    }

    pub fn compute_sha256(&self) -> Result<String> {
        // Convert to serde_json::Value first, then sort all object keys recursively
        // to ensure deterministic serialization (HashMap key order is not guaranteed).
        let value = serde_json::to_value(&self.partitions)?;
        let sorted_value = Self::sort_json_keys(value);
        let buf = serde_json::to_vec(&sorted_value)?;
        let sha = sha2::Sha256::digest(buf);
        Ok(format!("{:x}", sha))
    }

    /// Recursively sort all object keys in a JSON value to ensure deterministic serialization.
    fn sort_json_keys(value: serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match value {
            Value::Object(map) => {
                let sorted: serde_json::Map<String, Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, Self::sort_json_keys(v)))
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect();
                Value::Object(sorted)
            }
            Value::Array(arr) => Value::Array(arr.into_iter().map(Self::sort_json_keys).collect()),
            other => other,
        }
    }

    /// Get the partition type.
    pub fn partitions_type(&self) -> PartInfoType {
        // If the self.partitions is empty, it means that the partition is block level.
        if self.partitions.is_empty() {
            return PartInfoType::BlockLevel;
        }

        self.partitions[0].part_type()
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

/// StealablePartitions is used for cache affinity
/// that is, the same partition is always routed to the same executor as possible.
#[derive(Clone)]
pub struct StealablePartitions {
    pub partitions: Arc<RwLock<Vec<VecDeque<PartInfoPtr>>>>,
    pub ctx: Arc<dyn TableContext>,
    // In some cases, we need to disable steal.
    // Such as topk queries, this is suitable that topk will respect all the pagecache and reduce false sharing between threads.
    pub disable_steal: bool,
}

impl StealablePartitions {
    pub fn new(partitions: Vec<VecDeque<PartInfoPtr>>, ctx: Arc<dyn TableContext>) -> Self {
        StealablePartitions {
            partitions: Arc::new(RwLock::new(partitions)),
            ctx,
            disable_steal: false,
        }
    }

    pub fn disable_steal(&mut self) {
        self.disable_steal = true;
    }

    pub fn steal(&self, idx: usize, max_size: usize) -> Option<Vec<PartInfoPtr>> {
        let mut partitions = self.partitions.write();
        if partitions.is_empty() {
            return None;
        }

        let idx = if idx >= partitions.len() {
            idx % partitions.len()
        } else {
            idx
        };

        for step in 0..partitions.len() {
            let index = (idx + step) % partitions.len();

            if !partitions[index].is_empty() {
                let ps = &mut partitions[index];
                let size = ps.len().min(max_size);
                return Some(ps.drain(..size).collect());
            }

            if self.disable_steal {
                break;
            }
        }

        drop(partitions);

        None
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReclusterTask {
    pub parts: Partitions,
    pub stats: PartStatistics,
    pub total_rows: usize,
    pub total_bytes: usize,
    pub total_compressed: usize,
    pub level: i32,
}

pub type BlockMetaWithHLL = (Arc<BlockMeta>, Option<RawBlockHLL>);

#[derive(Clone)]
pub enum ReclusterParts {
    Recluster {
        tasks: Vec<ReclusterTask>,
        remained_blocks: Vec<BlockMetaWithHLL>,
        removed_segment_indexes: Vec<usize>,
        removed_segment_summary: Statistics,
    },
    Compact(Partitions),
}

impl ReclusterParts {
    pub fn is_empty(&self) -> bool {
        match self {
            ReclusterParts::Recluster {
                tasks,
                remained_blocks,
                ..
            } => tasks.is_empty() && remained_blocks.is_empty(),
            ReclusterParts::Compact(parts) => parts.is_empty(),
        }
    }

    pub fn new_recluster_parts() -> Self {
        Self::Recluster {
            tasks: vec![],
            remained_blocks: vec![],
            removed_segment_indexes: vec![],
            removed_segment_summary: Statistics::default(),
        }
    }

    pub fn new_compact_parts() -> Self {
        Self::Compact(Partitions::default())
    }

    pub fn is_distributed(&self, ctx: Arc<dyn TableContext>) -> bool {
        match self {
            ReclusterParts::Recluster { tasks, .. } => tasks.len() > 1,
            ReclusterParts::Compact(_) => {
                (!ctx.get_cluster().is_empty())
                    && ctx
                        .get_settings()
                        .get_enable_distributed_compact()
                        .unwrap_or(false)
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ReclusterInfoSideCar {
    pub merged_blocks: Vec<BlockMetaWithHLL>,
    pub removed_segment_indexes: Vec<usize>,
    pub removed_statistics: Statistics,
}
