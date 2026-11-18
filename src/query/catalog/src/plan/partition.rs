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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_meta_client::types::NodeInfo;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use parking_lot::RwLock;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use sha2::Digest;

use crate::plan::PartStatistics;
use crate::plan::VirtualColumnLayout;
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

    /// Whether this part carries metadata that must stay with any reshuffled
    /// slice of the real partitions.
    fn is_reshuffle_header(&self) -> bool {
        false
    }

    /// Bytes the part holds, itself and on the heap, for caches of parts. By default the part
    /// itself only: parts owning heap data count it.
    fn mem_bytes(&self) -> usize {
        size_of_val(self)
    }
}

impl Debug for Box<dyn PartInfo> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(json) => f.write_str(&json),
            Err(cause) => f
                .debug_struct("PartInfo")
                .field("serialization_error", &cause)
                .finish(),
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
        // Some partition lists include synthetic header parts that carry
        // metadata needed to interpret the real partitions. Reshuffle must not
        // assign such a header to only the first executor slice:
        //
        //   executor-1: [header, part1, part2]
        //   executor-2: [part3, part4]         // missing metadata
        //
        // Instead, every non-empty executor slice receives the headers:
        //
        //   executor-1: [header, part1, part2]
        //   executor-2: [header, part3, part4]
        let headers = self
            .partitions
            .iter()
            .filter(|part| part.is_reshuffle_header())
            .cloned()
            .collect::<Vec<_>>();
        let regular_partitions = self
            .partitions
            .iter()
            .filter(|part| !part.is_reshuffle_header())
            .cloned()
            .collect::<Vec<_>>();

        let partitions = match self.kind {
            PartitionsShuffleKind::Seq => regular_partitions,
            PartitionsShuffleKind::Mod => {
                // Sort by hash%executor_nums.
                let mut parts = regular_partitions
                    .into_iter()
                    .map(|p| (p.hash() % num_executors as u64, p.clone()))
                    .collect::<Vec<_>>();
                parts.sort_by_key(|a| a.0);
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

                if !headers.is_empty() && regular_partitions.is_empty() {
                    let local_id = &GlobalConfig::instance().query.node_id;
                    if let Some(part) = executor_part.get_mut(local_id) {
                        part.partitions = headers;
                    }
                    return Ok(executor_part);
                }

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

                ring.sort_by_key(|&(_, a)| a);

                for p in &regular_partitions {
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
                if !headers.is_empty() {
                    for part in executor_part.values_mut() {
                        if !part.partitions.is_empty() {
                            part.partitions.splice(0..0, headers.clone());
                        }
                    }
                }
                return Ok(executor_part);
            }
            PartitionsShuffleKind::Rand => {
                let mut rng = thread_rng();
                let mut parts = regular_partitions;
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
        if partitions.is_empty() && !headers.is_empty() {
            let mut executor_part = HashMap::default();

            let local_id = &GlobalConfig::instance().query.node_id;
            for executor in executors_sorted.into_iter() {
                let parts = match &executor.id == local_id {
                    true => headers.clone(),
                    false => vec![],
                };

                executor_part.insert(
                    executor.id.clone(),
                    Partitions::create(PartitionsShuffleKind::Seq, parts),
                );
            }

            return Ok(executor_part);
        }

        if partitions.len() == 1 {
            let mut executor_part = HashMap::default();

            let local_id = &GlobalConfig::instance().query.node_id;
            for executor in executors_sorted.into_iter() {
                let parts = match &executor.id == local_id {
                    true => headers
                        .iter()
                        .cloned()
                        .chain(partitions.iter().cloned())
                        .collect(),
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
                headers
                    .iter()
                    .cloned()
                    .chain(partitions[begin..end].iter().cloned())
                    .collect()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum VerticalReclusterKind {
    SortBlocks,
    MergeBlocks,
}

/// Per-level block counts, rows and sizes for insert/recluster diagnostic logs, not table statistics.
/// `None` means no cluster statistics; -1 denotes a perfect block.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterLevelLogStats {
    pub level: Option<i32>,
    pub block_count: u64,
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
}

impl ClusterLevelLogStats {
    pub fn accumulate(levels: &mut BTreeMap<Option<i32>, Self>, block: &BlockMeta) {
        let level = block.cluster_stats.as_ref().map(|stats| stats.level);
        let stats = levels.entry(level).or_default();
        stats.level = level;
        stats.block_count += 1;
        stats.row_count += block.row_count;
        stats.block_size += block.block_size;
        stats.file_size += block.file_size;
    }
}

/// Work performed by the row-sort stage of a recluster task.
///
/// Task plans are exchanged only between query processes running the same version;
/// this enum is not part of persisted FUSE metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReclusterTaskKind {
    /// Establish row order. For ordinary linear FUSE tasks, the inputs are one
    /// unordered block or a size-bounded group of small blocks. Other layouts
    /// retain their layout-specific sorting and re-aggregation semantics.
    SortBlocks,
    /// Merge blocks already ordered by the current linear cluster key, without
    /// sorting their rows again.
    MergeBlocks,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReclusterTask {
    pub parts: Partitions,
    pub stats: PartStatistics,
    pub total_rows: usize,
    pub total_bytes: usize,
    pub total_compressed: usize,
    /// Base level; the serializer requests `level + 1` (perfect blocks may become -1).
    pub level: i32,
    /// Effective input levels under the current cluster key, not historical stored levels.
    #[serde(default)]
    pub input_level_stats: Vec<ClusterLevelLogStats>,
    pub kind: ReclusterTaskKind,
    /// Absent preserves the existing horizontal pipeline and wire compatibility.
    #[serde(default)]
    pub vertical_kind: Option<VerticalReclusterKind>,
    /// Hard admission budget for the independent vertical executor.
    #[serde(default)]
    pub memory_budget: usize,
    pub virtual_column_layout: Option<VirtualColumnLayout>,
    /// Index metadata in original input-block order; absent when reuse is disabled.
    #[serde(default)]
    pub inverted_index_sources: Vec<Vec<BlockIndexMeta>>,
    /// Merge groups as consecutive runs of `parts` and `inverted_index_sources`, merged one
    /// group after another. Empty means one group; a node that predates the field merges the
    /// task as one group, which is still correct.
    #[serde(default)]
    pub merge_groups: Vec<ReclusterMergeGroup>,
}

/// One merge group of a recluster task. The totals size the group's output blocks, which
/// the parts alone cannot provide: they do not carry the uncompressed block size.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReclusterMergeGroup {
    pub blocks: usize,
    pub total_rows: usize,
    pub total_bytes: usize,
    pub total_compressed: usize,
}

impl ReclusterTask {
    /// Splits the task into one task per merge group, each with its own slice of `parts`
    /// and `inverted_index_sources`.
    pub fn split_merge_groups(&self) -> Result<Vec<ReclusterTask>> {
        if self.merge_groups.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let groups = &self.merge_groups;
        let blocks = groups.iter().map(|group| group.blocks).sum::<usize>();
        if blocks != self.parts.len() || groups.iter().any(|group| group.blocks == 0) {
            return Err(ErrorCode::Internal(format!(
                "recluster merge groups cover {blocks} blocks, the task has {}",
                self.parts.len()
            )));
        }
        let totals = groups.iter().fold((0, 0, 0), |acc, group| {
            (
                acc.0 + group.total_rows,
                acc.1 + group.total_bytes,
                acc.2 + group.total_compressed,
            )
        });
        if totals != (self.total_rows, self.total_bytes, self.total_compressed) {
            return Err(ErrorCode::Internal(
                "recluster merge group totals do not add up to the task totals",
            ));
        }
        let index_sources = !self.inverted_index_sources.is_empty();
        if index_sources && self.inverted_index_sources.len() != self.parts.len() {
            return Err(ErrorCode::Internal(
                "recluster inverted index sources are not aligned with the parts",
            ));
        }
        let mut start = 0;
        Ok(groups
            .iter()
            .map(|group| {
                let range = start..start + group.blocks;
                start = range.end;
                ReclusterTask {
                    parts: Partitions::create(
                        self.parts.kind.clone(),
                        self.parts.partitions[range.clone()].to_vec(),
                    ),
                    stats: PartStatistics {
                        read_rows: group.total_rows,
                        read_bytes: group.total_bytes,
                        partitions_scanned: group.blocks,
                        partitions_total: group.blocks,
                        ..self.stats.clone()
                    },
                    total_rows: group.total_rows,
                    total_bytes: group.total_bytes,
                    total_compressed: group.total_compressed,
                    level: self.level,
                    // The input levels describe the whole task, which logs them once.
                    input_level_stats: vec![],
                    kind: self.kind,
                    vertical_kind: self.vertical_kind,
                    memory_budget: self.memory_budget,
                    virtual_column_layout: self.virtual_column_layout.clone(),
                    inverted_index_sources: match index_sources {
                        true => self.inverted_index_sources[range].to_vec(),
                        false => vec![],
                    },
                    merge_groups: vec![],
                }
            })
            .collect())
    }
}

/// A surviving block keeps its segment-local virtual-column interpretation
/// and per-block statistics when maintenance replaces the containing segment.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RetainedBlockMeta {
    pub block_meta: Arc<BlockMeta>,
    pub hll: Option<RawBlockHLL>,
    pub top_n: Option<BlockTopN>,
    /// Local index into the enclosing container's `virtual_schemas`.
    pub virtual_schema_index: usize,
}

#[derive(Clone, Default)]
pub struct ReclusterParts {
    pub tasks: Vec<ReclusterTask>,
    pub remained_blocks: Vec<RetainedBlockMeta>,
    pub virtual_schemas: Vec<Option<Arc<VirtualSegmentSchema>>>,
    pub removed_segment_indexes: Vec<usize>,
    pub removed_segment_summary: Statistics,
}

impl ReclusterParts {
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.remained_blocks.is_empty()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ReclusterInfoSideCar {
    pub merged_blocks: Vec<RetainedBlockMeta>,
    pub virtual_schemas: Vec<Option<Arc<VirtualSegmentSchema>>>,
    pub removed_segment_indexes: Vec<usize>,
    pub removed_statistics: Statistics,
    /// Acquire the table lock only around refresh, sequence validation, and CAS publish.
    #[serde(default)]
    pub acquire_commit_lock: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertical_recluster_task_serde_roundtrip() {
        let task = ReclusterTask {
            parts: Partitions::default(),
            stats: PartStatistics::default(),
            total_rows: 42,
            total_bytes: 1024,
            total_compressed: 512,
            level: 3,
            input_level_stats: vec![],
            virtual_column_layout: None,
            kind: ReclusterTaskKind::MergeBlocks,
            vertical_kind: Some(VerticalReclusterKind::MergeBlocks),
            memory_budget: 64 * 1024 * 1024,
            inverted_index_sources: vec![vec![BlockIndexMeta {
                index_name: "text_idx".into(),
                location: ("source.index".into(), 2),
                size: 123,
                index_version: "definition-v1".into(),
            }]],
            merge_groups: vec![],
        };

        let encoded = serde_json::to_vec(&task).unwrap();
        let decoded: ReclusterTask = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded.kind, task.kind);
        assert_eq!(decoded.vertical_kind, task.vertical_kind);
        assert_eq!(decoded.memory_budget, task.memory_budget);
        assert_eq!(decoded.inverted_index_sources, task.inverted_index_sources);
    }

    #[test]
    fn test_old_recluster_task_defaults_vertical_fields() {
        let encoded = serde_json::json!({
            "parts": { "kind": "Seq", "partitions": [] },
            "stats": PartStatistics::default(),
            "total_rows": 0,
            "total_bytes": 0,
            "total_compressed": 0,
            "level": 0,
            "kind": "SortBlocks"
        });
        let decoded: ReclusterTask = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded.vertical_kind, None);
        assert_eq!(decoded.memory_budget, 0);
        assert!(decoded.input_level_stats.is_empty());
        assert!(decoded.virtual_column_layout.is_none());
        assert!(decoded.inverted_index_sources.is_empty());
        assert!(decoded.merge_groups.is_empty());
    }

    fn group(blocks: usize, rows: usize) -> ReclusterMergeGroup {
        ReclusterMergeGroup {
            blocks,
            total_rows: rows,
            total_bytes: rows * 10,
            total_compressed: rows * 2,
        }
    }

    fn grouped_task(blocks: usize, groups: Vec<ReclusterMergeGroup>) -> ReclusterTask {
        use databend_common_expression::Scalar;

        use crate::plan::StreamTablePart;
        let partitions = (0..blocks)
            .map(|block| {
                StreamTablePart::create(
                    Partitions::default(),
                    Scalar::Number((block as u64).into()),
                )
            })
            .collect();
        let rows = groups.iter().map(|group| group.total_rows).sum::<usize>();
        ReclusterTask {
            parts: Partitions::create(PartitionsShuffleKind::Seq, partitions),
            stats: PartStatistics::new_exact(rows, rows * 10, blocks, blocks),
            total_rows: rows,
            total_bytes: rows * 10,
            total_compressed: rows * 2,
            level: 1,
            input_level_stats: vec![],
            kind: ReclusterTaskKind::MergeBlocks,
            vertical_kind: None,
            memory_budget: 0,
            virtual_column_layout: None,
            inverted_index_sources: (0..blocks)
                .map(|block| {
                    vec![BlockIndexMeta {
                        index_name: format!("source-{block}"),
                        location: (format!("source-{block}.index"), 2),
                        size: 1,
                        index_version: "v1".into(),
                    }]
                })
                .collect(),
            merge_groups: groups,
        }
    }

    #[test]
    fn test_split_merge_groups_keeps_parts_and_index_sources_aligned() {
        let task = grouped_task(5, vec![group(2, 30), group(3, 70)]);
        let decoded: ReclusterTask =
            serde_json::from_slice(&serde_json::to_vec(&task).unwrap()).unwrap();
        assert_eq!(decoded.merge_groups, task.merge_groups);

        let groups = task.split_merge_groups().unwrap();
        assert_eq!(groups.len(), 2);
        for (group, range, rows) in [(&groups[0], 0..2, 30), (&groups[1], 2..5, 70)] {
            assert_eq!(group.parts.partitions, task.parts.partitions[range.clone()]);
            assert_eq!(
                group.inverted_index_sources,
                task.inverted_index_sources[range.clone()]
            );
            assert_eq!(group.parts.kind, PartitionsShuffleKind::Seq);
            assert_eq!(
                (group.total_rows, group.total_bytes, group.total_compressed),
                (rows, rows * 10, rows * 2)
            );
            assert_eq!(
                (group.stats.read_rows, group.stats.partitions_total),
                (rows, range.len())
            );
            assert_eq!(group.level, task.level);
            assert!(group.merge_groups.is_empty());
        }

        // Without index reuse the groups carry no index sources either.
        let mut task = task;
        task.inverted_index_sources.clear();
        assert!(
            task.split_merge_groups()
                .unwrap()
                .iter()
                .all(|group| group.inverted_index_sources.is_empty())
        );

        // A task without groups is one group.
        task.merge_groups.clear();
        let groups = task.split_merge_groups().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].parts, task.parts);
    }

    #[test]
    fn test_split_merge_groups_rejects_inconsistent_groups() {
        // The groups must cover every part, with no empty group.
        assert!(
            grouped_task(5, vec![group(2, 30), group(2, 70)])
                .split_merge_groups()
                .is_err()
        );
        assert!(
            grouped_task(5, vec![group(5, 30), group(0, 70)])
                .split_merge_groups()
                .is_err()
        );
        // The group totals must add up to the task totals.
        let mut task = grouped_task(5, vec![group(2, 30), group(3, 70)]);
        task.total_bytes += 1;
        assert!(task.split_merge_groups().is_err());
        // Index sources, when present, must be aligned with the parts.
        let mut task = grouped_task(5, vec![group(2, 30), group(3, 70)]);
        task.inverted_index_sources.pop();
        assert!(task.split_merge_groups().is_err());
    }
}
