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
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::number::F32;
use databend_storages_common_table_meta::meta::ColumnStatistics;

/// The rows of a block found by an index, attached by the pruner of that index:
/// the rows the inverted index matched, or the rows the vector index scored.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMatches {
    /// The rows found, as distinct row numbers of the block, in the order the
    /// index returned them.
    pub rows: Vec<u32>,
    /// The score of each row of `rows`. `None` when the query does not read the
    /// score of the index.
    pub scores: Option<Vec<F32>>,
    /// The scan outputs only `rows`, because the query drops the other rows
    /// anyway. Otherwise it outputs every row: the query still has to see them.
    pub rows_only: bool,
}

impl IndexMatches {
    fn row_bitmap(&self, num_rows: usize) -> Bitmap {
        let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
        for row in self.rows.iter().map(|row| *row as usize) {
            if row < num_rows {
                bitmap.set(row, true);
            }
        }
        bitmap.into()
    }
}

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
    /// The rows the inverted index matched, with their scores when the query
    /// reads `score()`.
    pub inverted_index_matches: Option<IndexMatches>,
    /// The rows the vector index scored, with their scores.
    pub vector_index_matches: Option<IndexMatches>,
    // The optional meta of virtual columns.
    pub virtual_block_meta: Option<VirtualBlockMetaIndex>,
}

impl BlockMetaIndex {
    /// The rows of the block the scan outputs, out of `num_rows`; `None` when it
    /// outputs every row.
    ///
    /// An index narrows the output to the rows it found with
    /// `IndexMatches::rows_only`. A row is output only if every index selects it.
    pub fn selected_rows(&self, num_rows: usize) -> Option<Bitmap> {
        let mut selected: Option<Bitmap> = None;
        let indexes = [&self.inverted_index_matches, &self.vector_index_matches];
        for matches in indexes.into_iter().flatten() {
            if !matches.rows_only {
                continue;
            }
            let rows = matches.row_bitmap(num_rows);
            selected = Some(match selected {
                Some(selected) => &selected & &rows,
                None => rows,
            });
        }
        selected.filter(|rows| rows.null_count() > 0)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn selected(index: &BlockMetaIndex, num_rows: usize) -> Option<Vec<bool>> {
        index
            .selected_rows(num_rows)
            .map(|rows| rows.iter().collect())
    }

    fn matches(rows: &[u32], rows_only: bool) -> Option<IndexMatches> {
        Some(IndexMatches {
            rows: rows.to_vec(),
            scores: None,
            rows_only,
        })
    }

    #[test]
    fn test_selected_rows() {
        // Without index pruning the scan outputs every row.
        assert_eq!(selected(&BlockMetaIndex::default(), 4), None);

        // The rows arrive in any order, as a score TopK returns them.
        let mut index = BlockMetaIndex {
            inverted_index_matches: matches(&[3, 1], true),
            ..Default::default()
        };
        assert_eq!(selected(&index, 4), Some(vec![false, true, false, true]));
        // Every row is found: the scan outputs every row.
        index.inverted_index_matches = matches(&[1, 0], true);
        assert_eq!(selected(&index, 2), None);

        // The query still has to see the other rows.
        index.inverted_index_matches = matches(&[3, 1], false);
        assert_eq!(selected(&index, 4), None);

        // The vector index selects rows the same way.
        index.vector_index_matches = matches(&[2, 3], true);
        assert_eq!(selected(&index, 4), Some(vec![false, false, true, true]));
        index.vector_index_matches = matches(&[2, 3], false);
        assert_eq!(selected(&index, 4), None);

        // Both indexes select rows: a row is output only if both select it.
        index.inverted_index_matches = matches(&[3, 1], true);
        index.vector_index_matches = matches(&[2, 3], true);
        assert_eq!(selected(&index, 4), Some(vec![false, false, false, true]));
        // No common row: the scan outputs none, not every row.
        index.vector_index_matches = matches(&[0], true);
        assert_eq!(selected(&index, 4), Some(vec![false; 4]));
    }
}
