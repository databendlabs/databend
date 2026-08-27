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
use std::collections::HashSet;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::ColumnId;
use databend_common_expression::types::number::F32;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockMetaIndex {
    /// {segment|block}_id is used in `InternalColumnMeta` to generate internal column data,
    /// where older data has smaller id, but {segment|block}_idx is opposite,
    /// so {segment|block}_id = {segment|block}_count - {segment|block}_idx - 1
    pub segment_idx: usize,
    pub block_idx: usize,
    pub range: Option<Range<usize>>,
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
    // Key is parquet column id used for reading, value is the column meta.
    pub virtual_column_metas: BTreeMap<ColumnId, VirtualColumnMeta>,
    // Key is (source column id, shared data type), value is the base column id for shared map data.
    #[serde(with = "shared_virtual_column_ids_serde")]
    pub shared_virtual_column_ids: BTreeMap<(ColumnId, VirtualColumnSharedDataType), ColumnId>,
    // If all the virtual columns are generated,
    // we can reduce IO by ignoring the source column.
    pub ignored_source_column_ids: HashSet<ColumnId>,
    // Key is virtual column id, value is the read plan.
    pub virtual_column_read_plan: BTreeMap<ColumnId, Vec<VirtualColumnReadPlan>>,
}

/// Read plan for materializing a virtual column from parquet virtual data.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum VirtualColumnReadPlan {
    /// The requested path is known to be absent from this virtual column file.
    /// Materialize it as NULL without reading the source column.
    Missing,
    /// Directly read the materialized virtual column by name.
    Direct { name: String },
    /// Read from a parent plan (usually a variant column) and extract by keypath suffix.
    FromParent {
        parent: Box<VirtualColumnReadPlan>,
        suffix_path: String,
    },
    /// Read from the shared map column using the key index for this source column.
    Shared {
        source_column_id: ColumnId,
        data_type: VirtualColumnSharedDataType,
        index: u32,
    },
    /// Merge multiple candidate plans by taking the first non-NULL value per row.
    Coalesce { plans: Vec<VirtualColumnReadPlan> },
    /// Reconstruct an object from child plans keyed by field name.
    Object {
        entries: Vec<(String, VirtualColumnReadPlan)>,
    },
}

mod shared_virtual_column_ids_serde {
    use std::collections::BTreeMap;

    use databend_common_expression::ColumnId;
    use databend_storages_common_index::VirtualColumnSharedDataType;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    type SharedVirtualColumnIds = BTreeMap<(ColumnId, VirtualColumnSharedDataType), ColumnId>;
    type SharedVirtualColumnIdEntry = ((ColumnId, VirtualColumnSharedDataType), ColumnId);

    pub fn serialize<S>(ids: &SharedVirtualColumnIds, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        ids.iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<SharedVirtualColumnIdEntry>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SharedVirtualColumnIds, D::Error>
    where D: Deserializer<'de> {
        Ok(
            Vec::<SharedVirtualColumnIdEntry>::deserialize(deserializer)?
                .into_iter()
                .collect(),
        )
    }
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
