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
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::ColumnId;
use databend_common_expression::types::DataType;
use databend_common_expression::types::number::F32;
use databend_storages_common_table_meta::meta::ColumnStatistics;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockMetaIndex {
    /// {segment|block}_id is used in `InternalColumnMeta` to generate internal column data,
    /// where older data has smaller id, but {segment|block}_idx is opposite,
    /// so {segment|block}_id = {segment|block}_count - {segment|block}_idx - 1
    pub segment_idx: usize,
    pub block_idx: usize,
    pub range: Option<Range<usize>>,
    #[serde(default)]
    pub granule_ranges: Option<Vec<Range<usize>>>,
    /// The page size of the block.
    /// If the block format is parquet, its page size is the rows count of the block.
    /// If the block format is native, its page size is the rows count of each page. (The rows count of the last page may be smaller than the page size.)
    pub page_size: usize,
    pub block_id: usize,
    pub block_location: String,
    pub segment_location: String,
    pub snapshot_location: Option<String>,
    // The search matched rows in the block (aligned with `matched_scores` when present).
    pub matched_rows: Option<Vec<usize>>,
    // Optional scores for the matched rows.
    pub matched_scores: Option<Vec<F32>>,
    // The vector topn rows and scores in the block.
    pub vector_scores: Option<Vec<(usize, F32)>>,
    // The optional meta of virtual columns.
    pub virtual_block_meta: Option<VirtualBlockMetaIndex>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct VirtualBlockMetaIndex {
    pub virtual_block_location: String,
    /// Key is query column ID; value is the complete materialization plan.
    pub fields: BTreeMap<ColumnId, VirtualFieldReadPlan>,
    /// Query-local physical slots required by `fields`.
    pub read_slots: Vec<VirtualReadSlot>,
    /// Source table column IDs that do not require fallback reads.
    pub ignored_source_column_ids: HashSet<ColumnId>,
    /// TopN-only statistics keyed by query column ID. Statistics are already
    /// converted to the query-visible logical type. Range pruning derives its
    /// virtual statistics independently from BlockMeta before this stage.
    pub virtual_column_stats: HashMap<ColumnId, ColumnStatistics>,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct VirtualReadSlotId(pub u32);

impl VirtualReadSlotId {
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VirtualReadSlot {
    pub offset: u64,
    pub len: u64,
    pub num_values: u64,
    /// Physical type stored in the sidecar.
    pub data_type: DataType,
}

/// Read plan for materializing one query-visible virtual column.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum VirtualFieldReadPlan {
    /// The requested path is known to be absent from this virtual column file.
    Missing,
    /// Directly read one query-local sidecar slot.
    Direct { slot: VirtualReadSlotId },
    /// Read from a parent plan and extract a keypath suffix.
    FromParent {
        parent: Box<VirtualFieldReadPlan>,
        suffix_path: String,
    },
    /// Read one entry from a shared map represented by adjacent key/value slots.
    Shared {
        key_slot: VirtualReadSlotId,
        value_slot: VirtualReadSlotId,
        index: u32,
    },
    /// Merge candidate representations by taking the first non-NULL value.
    Coalesce { plans: Vec<VirtualFieldReadPlan> },
    /// Reconstruct an object from child plans keyed by field name.
    Object {
        entries: Vec<(String, VirtualFieldReadPlan)>,
    },
}

#[typetag::serde(name = "block_meta_index")]
impl BlockMetaInfo for BlockMetaIndex {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        BlockMetaIndex::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl BlockMetaIndex {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&BlockMetaIndex> {
        BlockMetaIndex::downcast_ref_from(info).ok_or_else(|| {
            ErrorCode::Internal("Cannot downcast from BlockMetaInfo to BlockMetaIndex.")
        })
    }
}
